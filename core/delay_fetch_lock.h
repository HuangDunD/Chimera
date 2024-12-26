#pragma once
#include "common.h"
#include "config.h"

#include <scheduler/corotine_scheduler.h>
#include <butil/logging.h>
#include <mutex>
#include <cassert>
#include <iostream>
#include <list>
#include <condition_variable>
#include <thread>
#include <queue>
#include <atomic>

class DYFetchLocalPageLockTable;

// 这里是想要使用DYLocalPageLock来实现Delay Release的功能
class DYFetchLocalPageLock{ 
friend class DYFetchLocalPageLockTable;
friend class ComputeServer;
private:
    page_id_t page_id;          // 数据页id
    lock_t lock;                // 读写锁, 记录当前数据页的ref
    LockMode remote_mode;       // 这个计算节点申请的远程节点的锁模式
    bool is_pending = false;    // 是否正在pending
    bool xpending_ = false;      // 配合is_pending使用
    bool is_granting = false;   // 是否正在授权
    bool success_return = false; // 加锁是否成功
    node_id_t update_node = -1;       // 最新的节点
    bool is_in_delay_fetch_list = false; // 是否在delay_release_lock_list中
    int s_ref_delay_fetch = 0;          // 特殊, 仅用于标记锁升级竞争失败要求释放S锁
    int x_ref_delay_fetch = 0;    // 延迟获取的引用计数
    LockMode delay_fetch_mode;  // 延迟获取的锁模式
    std::chrono::time_point<std::chrono::steady_clock> first_fetch_time;   // 获取时间
    std::chrono::time_point<std::chrono::steady_clock> ddl_time;   // ddl时间
    // std::list<DYFetchLocalPageLock*>::iterator delay_fetch_list_it; // 在delay_fetch_lock_list中的迭代器

private:
    std::mutex mutex;           // 用于保护读写锁的互斥锁
    DYFetchLocalPageLockTable* page_table;

public:
    DYFetchLocalPageLock(page_id_t pid, DYFetchLocalPageLockTable* table) {
        page_id = pid;
        lock = 0;
        remote_mode = LockMode::NONE;
        page_table = table;
        delay_fetch_mode = LockMode::NONE;
    }

    LockMode LockShared(){
        // LOG(INFO) << "LockShared: " << page_id;
        bool try_latch = true;
        bool enter = false;
        LockMode fetch_mode = LockMode::NONE; //如果返回LockMode::NONE, 则不需要去远程获取控制权
        while(try_latch){
            mutex.lock(); 
            if(!enter && !is_pending) {
                // 允许进入
                enter = true;
                s_ref_delay_fetch++;
            }else if(!enter && is_pending ){
                mutex.unlock();
                continue; // !这里考虑协程切换
            }
            // enter is true
            if(remote_mode == LockMode::EXCLUSIVE){
                if(lock == EXCLUSIVE_LOCKED) {
                    mutex.unlock();
                }
                else {
                    lock++;
                    // 由于远程已经持有排他锁, 因此无需再去远程获取锁
                    // 加上锁了
                    s_ref_delay_fetch--;
                    mutex.unlock();
                    try_latch = false;
                }
            }
            else if(remote_mode == LockMode::SHARED){
                if(lock == EXCLUSIVE_LOCKED) {
                    mutex.unlock();
                    LOG(ERROR) << "Locol Grant Exclusive Lock, however remote only grant shared lock";
                }
                else {
                    lock++;
                    // 由于远程已经持有共享锁, 因此无需再去远程获取锁
                    // 加上锁了
                    s_ref_delay_fetch--;
                    mutex.unlock();
                    try_latch = false;
                }
            }
            else if(remote_mode == LockMode::NONE){
                if(is_granting) {
                    // 已经有线程在远程获取锁
                    // do nothing
                    mutex.unlock();
                    std::this_thread::sleep_for(std::chrono::microseconds(1)); //sleep 1us
                    continue;
                }
                else if(!is_in_delay_fetch_list) {
                    is_in_delay_fetch_list = true;
                    first_fetch_time = std::chrono::steady_clock::now();
                    assert(delay_fetch_mode == LockMode::NONE);
                    delay_fetch_mode = LockMode::SHARED;
                }
                else{
                    assert(delay_fetch_mode != LockMode::NONE);
                    assert(s_ref_delay_fetch > 0);
                    if(first_fetch_time + std::chrono::microseconds(DelayFetchTime) < std::chrono::steady_clock::now() && is_granting == false){
                        // 超时， 去远程获取数据页
                        is_granting = true;
                        try_latch = false;
                        fetch_mode = delay_fetch_mode;
                        is_in_delay_fetch_list = false;
                    }
                }
                mutex.unlock();
            }
            else{
                assert(false);
            }
        }
        return fetch_mode;
    };

    LockMode LockShared(CoroutineScheduler* coro_sched, coro_yield_t&yield, coro_id_t coro_id, bool delay = true);

    // 调用LockExclusive()或者LockShared()之后, 返回了需要对全局进行上锁，需要调用这个函数继续将这个函数上锁
    void ReLockShared(){
        while (true) {
            mutex.lock();
            assert(remote_mode == LockMode::SHARED || remote_mode == LockMode::EXCLUSIVE);
            if(remote_mode == LockMode::SHARED) assert(lock != EXCLUSIVE_LOCKED);
            if(lock == EXCLUSIVE_LOCKED){
                mutex.unlock();
            }
            else{
                lock++;
                // 加上锁了
                assert(s_ref_delay_fetch > 0);
                s_ref_delay_fetch--;
                mutex.unlock();
                break;
            }
        }
    }

    LockMode LockExclusive() {
        // LOG(INFO) << "LockShared: " << page_id;
        bool try_latch = true;
        bool enter = false;
        LockMode fetch_mode = LockMode::NONE; //如果返回LockMode::NONE, 则不需要去远程获取控制权
        while(try_latch){
            mutex.lock(); 
            if(!enter && !is_pending) {
                // 允许进入
                enter = true;
                x_ref_delay_fetch++;
            }else if(!enter && is_pending){
                mutex.unlock();
                continue; // !这里考虑协程切换
            }
            // enter is true
            if(remote_mode == LockMode::EXCLUSIVE){
                if(lock != 0) {
                    mutex.unlock();
                }
                else {
                    lock = EXCLUSIVE_LOCKED;
                    // 由于远程已经持有排他锁, 因此无需再去远程获取锁
                    // 加上锁了
                    x_ref_delay_fetch--;
                    mutex.unlock();
                    try_latch = false;
                }
            }
            else if(remote_mode == LockMode::SHARED || remote_mode == LockMode::NONE){
                if(is_granting) {
                    // 已经有线程在远程获取锁
                    // do nothing
                    mutex.unlock();
                    std::this_thread::sleep_for(std::chrono::microseconds(1)); //sleep 1us
                    continue;
                }
                else if(!is_in_delay_fetch_list) {
                    is_in_delay_fetch_list = true;
                    first_fetch_time = std::chrono::steady_clock::now();
                    assert(delay_fetch_mode == LockMode::NONE);
                    delay_fetch_mode = LockMode::EXCLUSIVE;
                }
                else{
                    assert(delay_fetch_mode != LockMode::NONE);
                    assert(x_ref_delay_fetch > 0);
                    delay_fetch_mode = LockMode::EXCLUSIVE;
                    if(first_fetch_time + std::chrono::microseconds(DelayFetchTime) < std::chrono::steady_clock::now() && is_granting == false){
                        // 超时， 去远程获取数据页
                        is_granting = true;
                        try_latch = false;
                        fetch_mode = delay_fetch_mode;
                        is_in_delay_fetch_list = false;
                    }
                }
                mutex.unlock();
            }
            else{
                assert(false);
            }
        }
        return fetch_mode;
    }

    LockMode LockExclusive(CoroutineScheduler* coro_sched, coro_yield_t&yield, coro_id_t coro_id, bool delay = true);

    void ReLockExclusive(){
        while (true) {
            mutex.lock();
            assert(remote_mode == LockMode::EXCLUSIVE);
            if(lock != 0){
                mutex.unlock();
            }
            else{
                lock = EXCLUSIVE_LOCKED;
                // 加上锁了
                assert(x_ref_delay_fetch > 0);
                x_ref_delay_fetch--;
                mutex.unlock();
                break;
            }
        }
    }

    void RemoteNotifyLockSuccess(bool xlock, node_id_t valid_node){
        mutex.lock();
        assert(is_granting == true);
        if(xlock) assert(delay_fetch_mode == LockMode::EXCLUSIVE);
        else assert(delay_fetch_mode == LockMode::SHARED); 
        success_return = true;
        update_node = valid_node;
        mutex.unlock();
    }

    bool TryRemoteLockSuccess(node_id_t* ret_node){
        bool suc_lock = false;
        mutex.lock();
        assert(is_granting == true);
        if(success_return == true){
            *ret_node = update_node;
            // 重置远程加锁成功标志位
            success_return = false;
            suc_lock = true;
            update_node = -1;
        }
        mutex.unlock();
        return suc_lock;
    }

    // 调用LockExclusive()或者LockShared()之后, 如果返回true, 则需要调用这个函数将granting状态转换为shared或者exclusive
    void LockRemoteOK(node_id_t node_id){
        // LOG(INFO) << "LockRemoteOK: " << page_id ;
        mutex.lock();
        assert(is_granting == true);
        is_granting = false;
        // 可以通过lock的值来判断远程的锁模式，因为LockMode::GRANTING和LockMode::UPGRADING的时候其他线程不能加锁
        assert(delay_fetch_mode == LockMode::SHARED || delay_fetch_mode == LockMode::EXCLUSIVE);
        if(delay_fetch_mode == LockMode::SHARED){
            assert(s_ref_delay_fetch > 0);
            // LOG(INFO) << "LockRemoteOK: " << page_id << " SHARED in node " << node_id;
            remote_mode = LockMode::SHARED;
        }
        else{
            assert(x_ref_delay_fetch > 0);
            // LOG(INFO) << "LockRemoteOK: " << page_id << " EXCLUSIVE in node " << node_id;
            remote_mode = LockMode::EXCLUSIVE;
        }
        delay_fetch_mode = LockMode::NONE;
        mutex.unlock();
    }

    int UnlockShared(){
        // LOG(INFO) << "UnlockShared: " << page_id;
        int unlock_remote = 0; // 0表示不需要释放远程锁, 1表示需要释放S锁, 2表示需要释放X锁
        mutex.lock();
        assert(lock > 0);
        assert(lock != EXCLUSIVE_LOCKED);
        // assert(!is_granting); //当持有S锁时，其他线程一定没获取X锁，所以不会有is_granting
        --lock; 
        // LOG(INFO) << "UnlockShared: table:" << table_id << " page_id: " << page_id << " is_pending: " << is_pending << " lock: " << lock << " s_ref_delay_fetch: " << s_ref_delay_fetch << " x_ref_delay_fetch: " << x_ref_delay_fetch;
        // !!! 注意这里的is_pending有一种情况是true, 但是是释放x锁的pending先到的, 也就是并不是所持有的S锁的pending
        if(lock == 0 && is_pending && remote_mode == LockMode::SHARED && s_ref_delay_fetch == 0 && !xpending_){
            unlock_remote = 1;
            remote_mode = LockMode::NONE;
            is_pending = false; // 释放远程锁后，将is_pending置为false
            // 此处释放远程锁应该阻塞其他线程再去获取锁，否则在远程可能该节点锁没释放又获取的情况
        }
        else if(lock == 0 && is_pending && remote_mode == LockMode::EXCLUSIVE && x_ref_delay_fetch == 0 && s_ref_delay_fetch == 0){
            unlock_remote = (remote_mode == LockMode::SHARED) ? 1 : 2;
            is_pending = false; // 释放远程锁后，将is_pending置为false
            remote_mode = LockMode::NONE;
            xpending_ = false;
            // 此处释放远程锁应该阻塞其他线程再去获取锁，否则在远程可能该节点锁没释放又获取的情况
        }
        else{
            mutex.unlock();
        }
        return unlock_remote;
    }

    int UnlockExclusive(){
        // LOG(INFO) << "UnlockExclusive: " << page_id;
        int unlock_remote = 0;
        mutex.lock();
        assert(remote_mode == LockMode::EXCLUSIVE);
        assert(lock == EXCLUSIVE_LOCKED);
        assert(!is_granting); 
        lock = 0;
        // LOG(INFO) << "UnlockExclusive: table:" << table_id << " page_id: " << page_id << " is_pending: " << is_pending << " lock: " << lock << " s_ref_delay_fetch: " << s_ref_delay_fetch << " x_ref_delay_fetch: " << x_ref_delay_fetch;
        if(is_pending && x_ref_delay_fetch == 0 && s_ref_delay_fetch == 0){
            unlock_remote = 2;
            is_pending = false; // 释放远程锁后，将is_pending置为false
            remote_mode = LockMode::NONE;
            xpending_ = false;
            // 此处释放远程锁应该阻塞其他线程再去获取锁，否则在远程可能该节点锁没释放又获取的情况
        }
        else{
            mutex.unlock();
        }
        return unlock_remote;
    }

    int UnlockAny(){
        // 这个函数在一个线程结束的时候调用，此时本地的锁已经释放，远程的锁也应该释放
        int unlock_remote = 0; // 0表示不需要释放远程锁, 1表示需要释放S锁, 2表示需要释放X锁
        mutex.lock();
        // assert(lock == 0);
        // assert(!is_granting);
        // assert(!is_pending);
        // assert(!is_in_delay_fetch_list);
        // assert(s_ref_delay_fetch == 0);
        // assert(x_ref_delay_fetch == 0);
        // LOG(INFO) << "UnlockAny: table_id: " << table_id << " page_id: " << page_id << " remote_mode: " << (int)remote_mode << " is_granting: " << is_granting << " is_pending: " << is_pending << " lock: " << lock << " s_ref_delay_fetch: " << s_ref_delay_fetch << " x_ref_delay_fetch: " << x_ref_delay_fetch;
        if(remote_mode == LockMode::NONE){
            // 远程没有持有锁
            mutex.unlock();
        }
        else if(remote_mode == LockMode::SHARED){
            unlock_remote = 1;
            remote_mode = LockMode::NONE;
        }
        else if(remote_mode == LockMode::EXCLUSIVE){
            unlock_remote = 2;
            remote_mode = LockMode::NONE;
        }
        else{
            assert(false);
        }
        return unlock_remote;
    }

    // 调用UnlockExclusive()或者UnlockShared()之后, 如果返回true, 则需要调用这个函数释放本地的mutex
    void UnlockRemoteOK(){
        mutex.unlock();
    }

    int Pending(node_id_t n, bool xpending){
        int unlock_remote = 0;
        mutex.lock();
        // LOG(INFO) << "Pending: " << page_id ;
        assert(!is_pending);
        // is_granting == true 是极少出现的情况，本地0->X/0->S/S->X, 获取锁的节点还未来得及将remote_mode设置为SHARED或者EXCLUSIVE
        // 就进入了Pending状态

        // 存疑的情况
        // !主动释放后接受到pending，如果本地又加上了锁，远程必定为None? 因为必须要该锁在远程获取之后, 才能本地加锁成功

        // 相对正常的情况
        // 1. 本地没有正在远程获取锁，远程持有S锁或X锁，如果本地锁是0，则立即释放远程锁，否则置is_pending为true
        // 2. 本地没有正在远程获取锁，远程没有持有锁，这种情况是由于本地主动释放锁，远程还未来得及获取锁，此时无需做任何操作
        
        // 这里需要考虑这两种特殊情况
        // 第一种情况是远程已经有S锁，正在申请远程X锁，这个时候发生pending，
        // 一种可能是因为与其他申请X锁的计算节点竞争失败，导致需要将自己已经获取的S锁释放
        // 第二种可能是已经远程获取了X锁，但是本地还没有更新remote_mode, 而其他节点也想获取X锁，因此向本节点发送pending请求

        // 第二种情况是远程无锁，正在申请远程S/X锁，这个时候发生pending，原因是远程已经获取了锁，但是还没有将remote_mode设置为SHARED或者EXCLUSIVE
        // 这种情况下，直接置为Pending状态即可

        // LOG(INFO) << "Pending: table_id: " << table_id << " page_id: " << page_id << " is_granting: " << is_granting << " s_ref_delay_fetch: " << s_ref_delay_fetch << 
            // " x_ref_delay_fetch: " << x_ref_delay_fetch << " remote_mode: " << (int)remote_mode << " lock: " << lock << " xpending: " << xpending << "remote_node: " << n;
        if(!is_granting && remote_mode != LockMode::NONE){
            if(lock == 0 && s_ref_delay_fetch == 0 && remote_mode == LockMode::SHARED){
                // 立刻在远程释放锁
                unlock_remote = 1;
                remote_mode = LockMode::NONE;
                // 在函数外部unlock
            }
            else if(lock == 0 && s_ref_delay_fetch == 0 && x_ref_delay_fetch == 0 && remote_mode == LockMode::EXCLUSIVE){
                // 立刻在远程释放锁
                unlock_remote = 2;
                remote_mode = LockMode::NONE;
                // 在函数外部unlock
            }
            else{
                is_pending = true;
                mutex.unlock();
            }
        }
        else if(!is_granting && remote_mode == LockMode::NONE){
            // 计算节点主动释放锁
            mutex.unlock();
        }
        else if(is_granting && remote_mode == LockMode::SHARED){
            // 远程已经获取了S锁，正在申请X锁
            assert(x_ref_delay_fetch != 0); // 由于是is_granting, 说明必定有操作还没get到锁
            if(xpending){ 
                // 要求释放X锁
                is_pending = true;
                xpending_ = true;
                mutex.unlock();
            }
            else{
                assert(remote_mode == LockMode::SHARED);
                // 远程持有S锁，正在申请X锁，这个时候发生pending，一种可能是因为与其他申请X锁的计算节点竞争失败，导致需要将自己已经获取的S锁释放
                // 这个时候lock为多少是不好说的, 因为可能本地仍有S锁，也可能已经释放了S锁
                if(lock == 0 && s_ref_delay_fetch == 0){
                    // 立刻在远程释放锁
                    unlock_remote = 1;
                    remote_mode = LockMode::NONE;
                    // 在函数外部unlock
                }
                else{
                    is_pending = true;
                    mutex.unlock();
                }
            }
        }
        else if(is_granting && remote_mode == LockMode::NONE){
            // 这里考虑两种情况，第一种是没有主动释放锁，0->X / 0->S, 本地还未来得及将remote_mode设置为SHARED或者EXCLUSIVE
            // 第二种是主动释放锁，接受了过时的pending，而又来了新的加锁请求
            // 无论xpengding是true还是false, 都一样
            assert(x_ref_delay_fetch != 0 || s_ref_delay_fetch != 0);
            assert(lock == 0);
            is_pending = true;
            mutex.unlock();
        }
        else{
            // is_granting == true, remote_mode == EXCLUSIVE
            assert(false);
        }
        return unlock_remote;
    }
    
};

// Delay Release的锁表
class DYFetchLocalPageLockTable{ 
friend class ComputeServer;
friend class DYFetchLocalPageLock;
public:  
    DYFetchLocalPageLockTable(Phase& p): phase(p) {
        for(int i=0; i<ComputeNodeBufferPageSize; i++){
            DYFetchLocalPageLock* lock = new DYFetchLocalPageLock(i, this);
            page_table[i] = lock;
        }
    }

    DYFetchLocalPageLock* GetLock(page_id_t page_id) {
        return page_table[page_id];
    }

private:
    DYFetchLocalPageLock* page_table[ComputeNodeBufferPageSize];

    // std::list<DYFetchLocalPageLock*> delay_fetch_lock_list;    // 延迟释放锁的链表，释放锁时，先加入这个链表，然后等待一段时间，如果没有请求这个锁，就释放
    // std::mutex delay_fetch_list_mutex;
    // std::condition_variable delay_fetch_list_cv;

    // std::queue<page_id_t> decided_fetch_queue; 
    // std::mutex decided_fetch_queue_mutex;
    // std::condition_variable decided_fetch_queue_cv;
    // 统计信息
    std::atomic<int> hit_delayed_lock_cnt = 0;
public:
    std::atomic<int> delay_fetch_lock_ref = 0;
    std::atomic<int> delay_fetch_lock_remote = 0;

    std::atomic<int> delay_hit_ref = 0;
    std::atomic<int> rpc_hit_ref = 0;
    std::atomic<int> hold_period_hit_ref = 0;
    Phase& phase;
};
