// Author: Chunyue Huang
// Copyright (c) 2024

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <list>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <vector>
#include <brpc/channel.h>

#include "common.h"
#include "compute_server/server.h"
#include "storage/txn_log.h"
#include "base/data_item.h"
#include "cache/index_cache.h"
#include "connection/meta_manager.h"
#include "util/json_config.h"
#include "storage/log_record.h"
#include "scheduler/corotine_scheduler.h"
#include "remote_page_table/timestamp_rpc.h"

struct DataSetItem {
  DataSetItem(DataItemPtr item) {
    item_ptr = std::move(item);
    is_fetched = false;
    is_logged = false;
    has_locked = lock_status::LOCK_INIT; // for calvin
    lock_mode = lock_mode_type::NO_WAIT; // for calvin
    release_imme = false;
  }
  DataItemPtr item_ptr;
  bool is_fetched;
  bool is_logged;
  // for lock (calvin)
  lock_mode_type lock_mode;
  lock_status has_locked;
  node_id_t node_id;
  bool release_imme;
};

class DTX {
 public:
  /************ Interfaces for applications ************/
  void TxInit(tx_id_t txid);

  void TxBegin(tx_id_t txid);

  void AddToReadOnlySet(DataItemPtr item);

  void AddToReadWriteSet(DataItemPtr item, bool release_imme = false);

  void RemoveLastROItem();

  void ClearReadOnlySet();

  void ClearReadWriteSet();

  bool TxExe(coro_yield_t& yield, bool fail_abort = true);

  bool TxCommit(coro_yield_t& yield);

  void TxAbort(coro_yield_t& yield);

  /*****************************************************/

 public:
  DTX(MetaManager* meta_man,
      t_id_t tid,
      t_id_t local_tid,
      coro_id_t coroid,
      CoroutineScheduler* sched,
      IndexCache* index_cache,
      PageCache* page_cache,
      ComputeServer* compute_server,
      brpc::Channel* data_channel,
      brpc::Channel* log_channel,
      brpc::Channel* remote_server_channel,
      TxnLog* txn_log=nullptr, 
      CoroutineScheduler* sched_0=nullptr,
      int* using_which_coro_sched_=nullptr);
  ~DTX() {
    Clean();
  }

 public:
  // 发送日志到存储层
  TxnLog* txn_log;
  // for group commit
  uint32_t two_latency_c;

  brpc::Channel* storage_data_channel;
  brpc::Channel* storage_log_channel;
  brpc::Channel* remote_server_channel;

  // 计算事务的执行时间
  double tx_begin_time=0,tx_exe_time=0,tx_commit_time=0,tx_abort_time=0;
  double tx_get_timestamp_time1=0, tx_get_timestamp_time2=0, tx_write_commit_log_time=0, tx_write_prepare_log_time=0, tx_write_backup_log_time=0;
  double tx_fetch_exe_time=0, tx_fetch_commit_time=0, tx_release_exe_time=0, tx_release_commit_time=0;
  double tx_fetch_abort_time=0, tx_release_abort_time=0;
  int single_txn=0;
  int distribute_txn=0 ;

  void AddLogToTxn();
  void SendLogToStoragePool(uint64_t bid, brpc::CallId* cid); // use for rpc

 private:
  void Abort();

  void Clean();  // Clean data sets after commit/abort
 
 private:  

  timestamp_t global_timestamp = 0;
  timestamp_t local_timestamp = 0;
  timestamp_t GetTimestampRemote();

  inline Rid GetRidFromIndexCache(table_id_t table_id, itemkey_t key) { return index_cache->Search(table_id, key); }

  char* FetchSPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id);

  char* FetchXPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id);

  bool TxPrepare(coro_yield_t &yield);

  bool Tx2PCCommit(coro_yield_t &yield);

  void Tx2PCCommitAll(coro_yield_t &yield);

  void Tx2PCAbortAll(coro_yield_t &yield);

  bool TxCalvinCommit(coro_yield_t &yield);

  bool TxCalvinAbort(coro_yield_t &yield);

  bool TxCommitSingle(coro_yield_t& yield);

  void ReleaseSPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id);

  void ReleaseXPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id); 

  DataItemPtr GetDataItemFromPageRO(table_id_t table_id, char* data, Rid rid);

  DataItemPtr GetDataItemFromPageRW(table_id_t table_id, char* data, Rid rid, DataItem*& orginal_item);
   
  DataItemPtr UndoDataItem(DataItemPtr item);

 public:
  // for Calvin
  bool GetAllLock();
  // bool TxCalvinGetRemote(coro_yield_t& yield);
  bool TxCalvinSendReadSet(coro_yield_t& yield);
  void FillRemoteReadWriteSet(int idx, char* data);
  void InsertToCalvinStatus();
  void DeleteFromCalvinStatus();
  std::unordered_set<node_id_t> wait_ids;
  std::unordered_set<node_id_t> all_ids;
  // for debug
  std::unordered_set<node_id_t> another_wait_ids;
  bool has_send_read_set = false;
  // for long-running transactions
  void VarifyPhaseSwitch(coro_yield_t& yield);
  CoroutineScheduler* coro_sched_0;  // Thread local coroutine scheduler
  int* using_which_coro_sched;
  // GroupCommitInfo* group_commit_info;

 public:
  tx_id_t tx_id;  // Transaction ID
  t_id_t t_id;  // Thread ID(global)
  t_id_t local_t_id;  // Thread ID(local)
  coro_id_t coro_id;  // Coroutine ID
  batch_id_t epoch_id; // epoch id
  uint64_t start_ts;    // start timestamp
  uint64_t commit_ts;   // commit timestamp

 public:
  // for lock
  BenchDTX* bdtx;
 public:
  // For statistics
  struct timespec tx_start_time;

  std::vector<uint64_t> lock_durations;  // us

  MetaManager* global_meta_man;  // Global metadata manager

  CoroutineScheduler* coro_sched;  // Thread local coroutine scheduler

  ComputeServer* compute_server;  // Compute server
  
 public:

  TXStatus tx_status;

  std::vector<DataSetItem> read_only_set;

  std::vector<DataSetItem> read_write_set;

  IndexCache* index_cache;
  PageCache* page_cache;

  std::unordered_set<node_id_t> participants; // Participants in 2PC, only use in 2PC
};

enum class CalvinStages {
  INIT = 0,
  READ,
  WRITE,
  FIN
};

class BenchDTX {
public:
    DTX *dtx;
    batch_id_t bid;
    node_id_t node_id;
    uint64_t seed;
    bool is_partitioned;

    bool volatile lock_ready;
    // for calvin
    CalvinStages stage;
    BenchDTX() {
      dtx = nullptr;
      stage = CalvinStages::INIT;
      lock_ready = false;
    }
    virtual ~BenchDTX() {}
    // virtual bool TxGetRemote(coro_yield_t& yield) = 0;
    virtual bool StatCommit() = 0;
    virtual bool TxNeedWait() = 0;
};

/*************************************************************
 ************************************************************
 *********** Implementations of interfaces in DTX ***********
 ************************************************************
 **************************************************************/

ALWAYS_INLINE
void DTX::TxInit(tx_id_t txid) {
  Clean();  // Clean the last transaction states
  tx_id = txid;
  // start_ts = GetTimestampRemote();
}

ALWAYS_INLINE
void DTX::TxBegin(tx_id_t txid) {
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
    Clean();  // Clean the last transaction states
    tx_id = txid;
    struct timespec start_time1, end_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    start_ts = GetTimestampRemote();
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_get_timestamp_time1 += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
    clock_gettime(CLOCK_REALTIME, &end_time);
    tx_begin_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
}

ALWAYS_INLINE
void DTX::AddToReadOnlySet(DataItemPtr item) {
  DataSetItem data_set_item(item);
  read_only_set.emplace_back(data_set_item);
}

ALWAYS_INLINE
void DTX::AddToReadWriteSet(DataItemPtr item, bool release_imme) {
  DataSetItem data_set_item(item);
  data_set_item.release_imme = release_imme;
  read_write_set.emplace_back(data_set_item);
}

ALWAYS_INLINE
void DTX::ClearReadOnlySet() {
  read_only_set.clear();
}

ALWAYS_INLINE
void DTX::ClearReadWriteSet() {
  read_write_set.clear();
}

ALWAYS_INLINE
void DTX::Clean() {
  read_only_set.clear();
  read_write_set.clear();
  tx_status = TXStatus::TX_INIT;
  participants.clear();
}

ALWAYS_INLINE
void DTX::RemoveLastROItem() { read_only_set.pop_back(); }
