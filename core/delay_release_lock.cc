#include "delay_release_lock.h"

bool DYLocalPageLock::LockShared() {
    // LOG(INFO) << "LockShared: " << page_id;
    bool lock_remote = false;
    bool try_latch = true;
    while(try_latch){
        mutex.lock();
        assert(!(is_decided_release && is_in_delay_release_list));
        if(is_granting || is_decided_release){
            // 当前节点已经有线程正在远程获取这个数据页的锁，其他线程无需再去远程获取锁
            // 当前节点已经决定释放这个数据页的锁, 无需再去获取锁
            mutex.unlock();
            continue;
        }
        else if(remote_mode == LockMode::EXCLUSIVE){
            if(lock == EXCLUSIVE_LOCKED) {
                mutex.unlock();
            }
            else if(lock == 0){
                assert(is_in_delay_release_list == true); // 不在decided_release_queue中, lock为0, 说明一定在delay_release_lock_list中
                // 在delay_release_lock_list中, 但是没有决定释放
                // 说明有线程在期限内请求了这个锁, 就会从这个列表中移除
                // 无需再去远程获取锁
                lock++;
                is_in_delay_release_list = false;
                page_table->delay_release_list_mutex.lock();
                assert(this == *delay_release_list_it);
                page_table->delay_release_lock_list.erase(delay_release_list_it);
                page_table->delay_release_list_mutex.unlock();
                mutex.unlock();
                page_table->hit_delayed_lock_cnt++; // 统计命中延迟释放的锁
                lock_remote = false;
                try_latch = false;
            }
            else{
                assert(is_in_delay_release_list == false);
                lock++;
                mutex.unlock();
                // 由于远程已经持有排他锁, 因此无需再去远程获取锁
                lock_remote = false;
                try_latch = false;
            }
        }
        else if(remote_mode == LockMode::SHARED){
            assert(lock!=EXCLUSIVE_LOCKED);
            if(lock == 0){
                assert(is_in_delay_release_list == true); // 不在decided_release_queue中, lock为0, 说明一定在delay_release_lock_list中
                // 在delay_release_lock_list中, 但是没有决定释放
                // 说明有线程在期限内请求了这个锁, 就会从这个列表中移除
                // 无需再去远程获取锁
                lock++;
                is_in_delay_release_list = false;
                page_table->delay_release_list_mutex.lock();
                assert(this == *delay_release_list_it);
                page_table->delay_release_lock_list.erase(delay_release_list_it);
                page_table->delay_release_list_mutex.unlock();
                mutex.unlock();
                page_table->hit_delayed_lock_cnt++; // 统计命中延迟释放的锁
                lock_remote = false;
                try_latch = false;
            }
            else{
                assert(is_in_delay_release_list == false);
                lock++;
                mutex.unlock();
                // 由于远程已经持有共享锁
                lock_remote = false;
                try_latch = false;
            }
        }
        else if(remote_mode == LockMode::NONE){
            lock++;
            is_granting = true;
            lock_remote = true;
            try_latch = false;
            mutex.unlock();
        }
    }
    return lock_remote;
}

bool DYLocalPageLock::LockExclusive() {
    // LOG(INFO) << "LockShared: " << page_id;
    bool lock_remote = false;
    bool try_latch = true;
    while(try_latch){
        mutex.lock();
        assert(!(is_decided_release && is_in_delay_release_list));
        if(is_granting || is_decided_release){
            // 当前节点已经有线程正在远程获取这个数据页的锁，其他线程无需再去远程获取锁
            // 当前节点已经决定释放这个数据页的锁, 无需再去获取锁
            mutex.unlock();
        }
        else if(remote_mode == LockMode::EXCLUSIVE){
            if(lock != 0) {
                mutex.unlock();
            }
            else{
                assert(is_in_delay_release_list == true); // 不在decided_release_queue中, lock为0, 说明一定在delay_release_lock_list中
                // 在delay_release_lock_list中, 但是没有决定释放
                // 说明有线程在期限内请求了这个锁, 就会从这个列表中移除
                // 无需再去远程获取锁
                lock = EXCLUSIVE_LOCKED;
                is_in_delay_release_list = false;
                page_table->delay_release_list_mutex.lock();
                assert(this == *delay_release_list_it);
                page_table->delay_release_lock_list.erase(delay_release_list_it);
                page_table->delay_release_list_mutex.unlock();
                mutex.unlock();
                page_table->hit_delayed_lock_cnt++; // 统计命中延迟释放的锁
                lock_remote = false;
                try_latch = false;
            }
        }
        else if(remote_mode == LockMode::SHARED){
            assert(lock!=EXCLUSIVE_LOCKED);
            if(lock != 0){
                mutex.unlock();
            }
            else{
                // 这里立刻释放共享锁，然后去获取排他锁
                assert(is_in_delay_release_list == true); // 不在decided_release_queue中, lock为0, 说明一定在delay_release_lock_list中
                // 不等待计时, 移出delay_release_lock_list
                is_decided_release = true;
                is_in_delay_release_list = false;

                std::unique_lock<std::mutex> l(page_table->delay_release_list_mutex);
                assert(this == *delay_release_list_it);
                page_table->delay_release_lock_list.erase(delay_release_list_it);
                page_table->delay_release_list_cv.notify_one();

                std::unique_lock<std::mutex> l2(page_table->decided_release_queue_mutex);
                page_table->decided_release_queue.push(page_id);
                page_table->decided_release_queue_cv.notify_one();
                mutex.unlock(); // 等待释放共享锁
            }
        }
        else if(remote_mode == LockMode::NONE){
            lock = EXCLUSIVE_LOCKED;
            is_granting = true;
            lock_remote = true;
            try_latch = false;
            mutex.unlock();
        }
    }
    return lock_remote;
}

// 调用LockExclusive()或者LockShared()之后, 如果返回true, 则需要调用这个函数将granting状态转换为shared或者exclusive
void DYLocalPageLock::LockRemoteOK(){
    // LOG(INFO) << "LockRemoteOK: " << page_id << std::endl;
    mutex.lock();
    assert(is_granting == true);
    is_granting = false;
    // 可以通过lock的值来判断远程的锁模式，因为LockMode::GRANTING和LockMode::UPGRADING的时候其他线程不能加锁
    if(lock == EXCLUSIVE_LOCKED){
        // LOG(INFO) << "LockRemoteOK: " << page_id << " EXCLUSIVE_LOCKED in node " << node_id;
        remote_mode = LockMode::EXCLUSIVE;
    }
    else{
        // LOG(INFO) << "LockRemoteOK: " << page_id << " SHARED in node " << node_id;
        remote_mode = LockMode::SHARED;
    }
    mutex.unlock();
}

void DYLocalPageLock::UnlockShared() {
    // LOG(INFO) << "UnlockShared: " << page_id << std::endl;
    mutex.lock();
    assert(lock > 0);
    assert(lock != EXCLUSIVE_LOCKED);
    assert(!is_granting); //当持有S锁时，其他线程一定没获取X锁，所以不会有is_granting
    assert(!is_decided_release && !is_in_delay_release_list);
    --lock;
    if(lock == 0){
        // 释放锁
        std::unique_lock<std::mutex> l(page_table->delay_release_list_mutex);
        release_time = std::chrono::steady_clock::now() + std::chrono::microseconds(DelayReleaseTime);
        page_table->delay_release_lock_list.emplace_back(this);
        delay_release_list_it = --page_table->delay_release_lock_list.end();
        page_table->delay_release_list_cv.notify_one();
        is_in_delay_release_list = true;
        mutex.unlock();
    }
    else{
        mutex.unlock();
    }
}

void DYLocalPageLock::UnlockExclusive(){
    // LOG(INFO) << "UnlockExclusive: " << page_id << std::endl;
    mutex.lock();
    assert(lock == EXCLUSIVE_LOCKED);
    assert(!is_granting);
    assert(!is_decided_release && !is_in_delay_release_list);
    lock = 0;
    // 释放锁
    std::unique_lock<std::mutex> l(page_table->delay_release_list_mutex);
    release_time = std::chrono::steady_clock::now() + std::chrono::microseconds(DelayReleaseTime);
    page_table->delay_release_lock_list.emplace_back(this);
    delay_release_list_it = --page_table->delay_release_lock_list.end();
    page_table->delay_release_list_cv.notify_one();
    is_in_delay_release_list = true;
    mutex.unlock();
}