// Author: hongyao zhao
// Copyright (c) 2024

#pragma once
// #define BOOST_COROUTINES_NO_DEPRECATION_WARNING

#include "dtx/dtx.h"
#include <boost/lockfree/queue.hpp>
#include "semaphore.h"
#include <vector>
// #include <pair>

#define QUEUE_PRINT 0
#define QUEUE_LOG(INFO) if(QUEUE_PRINT) LOG(INFO)

enum entry_type {
  RDONE = 0,
  TXN = 1
};

struct queue_entry {
  entry_type type;
  BenchDTX * dtx;
  batch_id_t batch_id;
  tx_id_t txn_id;
  queue_entry(BenchDTX *d, batch_id_t bid, tx_id_t tid, entry_type t) {
    dtx = d;
    batch_id = bid;
    txn_id = tid;
    type = t;
  }
};

struct rw_set_entry {
  node_id_t node_id;
  batch_id_t bid;
  tx_id_t txn_id;
  uint64_t seed;
  std::vector<std::pair<int, char*>> data_map;
  // 初始化，包括node_id, bid, txn_id, data_map
  rw_set_entry(node_id_t node_id, batch_id_t bid, tx_id_t txn_id, std::vector<std::pair<int, char*>> data_map, uint64_t seed) {
    this->bid = bid;
    this->txn_id = txn_id;
    this->data_map = data_map;
    this->node_id = node_id;
    this->seed = seed;
  }
};

struct batch_entry {
  std::vector<dtx_entry> txns;
  batch_id_t bid;
  node_id_t nid;
  batch_entry(std::vector<dtx_entry> txns, batch_id_t bid, node_id_t nid): txns(txns),bid(bid),nid(nid) {}
};

class QWorkQueue {
public:
  QWorkQueue() {
    seq_queue = new boost::lockfree::queue<batch_entry* > (0);
    work_queue = new boost::lockfree::queue<queue_entry* > (0);
    msg_queue = new boost::lockfree::queue<rw_set_entry* > (0);
    sched_queue = new boost::lockfree::queue<queue_entry* > * [g_node_cnt];
    for (int i = 0; i < g_node_cnt; i++) {
      sched_queue[i] = new boost::lockfree::queue<queue_entry* > (0);
    }
    sem_init(&_semaphore, 0, 1);
    sched_ptr = 0;
  }

  // 传来的事务，是seed+类型
  void seq_enqueue(std::vector<dtx_entry> txns, batch_id_t bid, node_id_t nid) {
    // LOG(INFO) << "QWorkQueue::seq_enqueue work_queue_entry alloc";
    batch_entry * entry = new batch_entry(txns, bid, nid);
    // LOG(INFO) <<  "Seq Enqueue ("<< entry->bid << ")";
    while (!seq_queue->push(entry)) {}
  }
  batch_entry * seq_dequeue() {
    batch_entry * entry;
    bool valid = seq_queue->pop(entry);
    if(valid) {
      return entry;
    }
    return nullptr;
  }

  void sched_enqueue(BenchDTX * dtx, batch_id_t bid, node_id_t nid, entry_type t) {
    // LOG(INFO) << "QWorkQueue::sched_enqueue work_queue_entry alloc";
    tx_id_t tx_id = dtx != nullptr ? dtx->dtx->tx_id : 0;
    queue_entry * entry = new queue_entry(dtx, bid, tx_id,t);
    // LOG(INFO) <<  "Sched Enqueue ("<< entry->txn_id << "," << entry->batch_id << ")";
    while (!sched_queue[nid]->push(entry)) {}
  }
  BenchDTX * sched_dequeue() {
    queue_entry * entry;
    bool valid = sched_queue[sched_ptr]->pop(entry);
    BenchDTX* dtx = nullptr;
    if(valid) {
      dtx = entry->dtx;
      // LOG(INFO) << "Sched Dequeue bid" << entry->batch_id << ", tid" << entry->txn_id;
      
      if(entry->type == RDONE) {
        // Advance to next queue or next epoch
        // LOG(INFO) << "Sched RDONE node" << sched_ptr << ", batch" << entry->batch_id;
        sched_ptr = (sched_ptr + 1) % g_node_cnt;
      }
      delete entry;
    }
    return dtx;
  }

  void enqueue(BenchDTX * dtx, batch_id_t bid, bool busy) {
    // LOG(INFO) << "QWorkQueue::enqueue work_queue_entry alloc";
    queue_entry * entry = new queue_entry(dtx, bid, dtx->dtx->tx_id,entry_type::TXN);
    QUEUE_LOG(INFO) <<  "Work Enqueue "<< entry->batch_id << "-" << entry->txn_id;
    while (!work_queue->push(entry)) {}
  }
  BenchDTX * dequeue(uint64_t thd_id) {
    queue_entry * entry;
    bool valid = work_queue->pop(entry);
    BenchDTX* dtx = nullptr;
    if(valid) {
      dtx = entry->dtx;
      // LOG(INFO) << "Work Dequeue (" << entry->txn_id << "," << entry->batch_id << ")";
      delete entry;
    }
    return dtx;
  }

  void msg_enqueue(rw_set_entry * entry) {
    // LOG(INFO) << "QWorkQueue::enqueue work_queue_entry alloc";
    while (!msg_queue->push(entry)) {}
  }
  rw_set_entry * msg_dequeue(uint64_t thd_id) {
    rw_set_entry * entry = nullptr;
    bool valid = msg_queue->pop(entry);
    if(!valid) {
      return nullptr;
    } else 
      return entry;
  }

  void statqueue(queue_entry * entry);
private:
  boost::lockfree::queue<queue_entry* > * work_queue;
  boost::lockfree::queue<rw_set_entry* > * msg_queue;

  boost::lockfree::queue<batch_entry* > * seq_queue;
  boost::lockfree::queue<queue_entry* > ** sched_queue;

  uint64_t sched_ptr;
  sem_t 	_semaphore;  
};

