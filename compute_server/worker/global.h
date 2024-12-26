// Author: Hongyao Zhao
// Copyright (c) 2024
#pragma once 
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include "common.h"

class QWorkQueue;
class LockManager;
class BenchDTX;

extern int g_node_cnt;
extern bool system_finish;
extern uint32_t complete_worker_cnt;
extern LockManager * lock_manager;
extern QWorkQueue* queue;



// 帮忙写一个根据batch_id_t,tx_id_t的hash函数，然后放到calvin_txn_status里面
namespace std {
    template <>
    struct hash<std::pair<batch_id_t,tx_id_t>> {
        std::size_t operator()(const std::pair<batch_id_t,tx_id_t>& k) const {
            return std::hash<batch_id_t>()(k.first) ^ std::hash<tx_id_t>()(k.second);
        }
    };
}
extern std::unordered_map<std::pair<batch_id_t,tx_id_t>, BenchDTX*>* calvin_txn_status;
extern std::unordered_map<std::pair<batch_id_t,tx_id_t>, BenchDTX*>* fin_calvin_txn_status;
extern std::mutex* calvin_txn_mutex; 
extern node_id_t machine_id; //used for calvin

// extern std::unordered_set<BenchDTX*>* waiting_calvin_txns;
// extern std::mutex* wait_calvin_txn_mutex; 
// extern uint64_t lock_wait_tx_total;
// extern uint64_t lock_handover_tx_total;
// extern uint64_t lock_lock_total;
// extern uint64_t lock_unlock_total;

enum LockRC {
    FAILED = 0,  // 加锁失败
    SUCCESS = 1, // 已经加锁
    WAIT = 2,    // 等待加锁
};

// 写一个事务是否拿到锁的一个状态enum，包括还没开始加锁，加上了锁，等待加锁，不需要加锁
enum lock_status {
    LOCK_INIT = 0,
    LOCK_SUCCESS = 1,
    LOCK_WAIT = 2,
    LOCK_NO = 3,
};

enum lock_type {DLOCK_EX = 0, DLOCK_SH, LOCK_NONE };


#define PRINTF_CALVIN_INFO 0
#define CALVIN_LOG(INFO) if(PRINTF_CALVIN_INFO) LOG(INFO)