#pragma once
#include "common.h"
#include "config.h"

#include <mutex>
#include <cassert>
#include <iostream>
#include <list>
#include <condition_variable>
#include <thread>
#include <queue>
#include <atomic>

class DYLocalPageLockTable;

// 这里是想要使用DYLocalPageLock来实现Delay Release的功能
class DYLocalPageLock{ 
friend class DYLocalPageLockTable;
friend class ComputeServer;
private:
    page_id_t page_id;          // 数据页id
    lock_t lock;                // 读写锁, 记录当前数据页的ref
    LockMode remote_mode;       // 这个计算节点申请的远程节点的锁模式
    bool is_granting = false;   // 是否正在授权
    bool is_decided_release = false;    // 是否已经决定释放
    bool is_in_delay_release_list = false; // 是否在delay_release_lock_list中
    std::chrono::time_point<std::chrono::steady_clock> release_time;   // 释放时间
    std::list<DYLocalPageLock*>::iterator delay_release_list_it; // 在delay_release_lock_list中的迭代器

private:
    std::mutex mutex;           // 用于保护读写锁的互斥锁
    DYLocalPageLockTable* page_table;

public:
    DYLocalPageLock(page_id_t pid, DYLocalPageLockTable* table) {
        page_id = pid;
        lock = 0;
        remote_mode = LockMode::NONE;
        page_table = table;
    }

    bool LockShared();

    bool LockExclusive();

    // 调用LockExclusive()或者LockShared()之后, 如果返回true, 则需要调用这个函数将granting状态转换为shared或者exclusive
    void LockRemoteOK();

    void UnlockShared();

    void UnlockExclusive();
};

// Delay Release的锁表
class DYLocalPageLockTable{ 
friend class ComputeServer;
friend class DYLocalPageLock;
public:  
    DYLocalPageLockTable(){
        for(int i=0; i<ComputeNodeBufferPageSize; i++){
            DYLocalPageLock* lock = new DYLocalPageLock(i, this);
            page_table[i] = lock;
        }
    }

    DYLocalPageLock* GetLock(page_id_t page_id) {
        return page_table[page_id];
    }

private:
    DYLocalPageLock* page_table[ComputeNodeBufferPageSize];

    std::list<DYLocalPageLock*> delay_release_lock_list; // 延迟释放锁的链表，释放锁时，先加入这个链表，然后等待一段时间，如果没有请求这个锁，就释放
    std::mutex delay_release_list_mutex;
    std::condition_variable delay_release_list_cv;

    std::queue<page_id_t> decided_release_queue; // 决定释放队列, 不能反悔
    std::mutex decided_release_queue_mutex;
    std::condition_variable decided_release_queue_cv;

    // 统计信息
    std::atomic<int> hit_delayed_lock_cnt = 0;
};

