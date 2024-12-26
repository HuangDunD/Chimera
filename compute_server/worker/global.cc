// Author: Hongyao Zhao
// Copyright (c) 2023
#include "global.h"
#include "base/queue.h"
#include "dtx/dtx.h"

int g_node_cnt = 0;
uint32_t complete_worker_cnt = 0;
bool system_finish = false;
QWorkQueue* queue;
LockManager *lock_manager;
std::unordered_map<std::pair<batch_id_t,tx_id_t>, BenchDTX*>* calvin_txn_status;
std::unordered_map<std::pair<batch_id_t,tx_id_t>, BenchDTX*>* fin_calvin_txn_status;
std::mutex* calvin_txn_mutex; 
node_id_t machine_id; //used for calvin


// for debug
// uint64_t lock_wait_tx_total = 0;
// uint64_t lock_handover_tx_total = 0;
// uint64_t lock_lock_total = 0;
// uint64_t lock_unlock_total = 0;
// std::unordered_set<BenchDTX*>* waiting_calvin_txns;
// std::mutex* wait_calvin_txn_mutex; 
