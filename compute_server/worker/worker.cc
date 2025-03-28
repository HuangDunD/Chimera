// Author: Chunyue Huang
// Copyright (c) 2024

#include "worker.h"
#include <time.h>

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <brpc/channel.h>

#include "dtx/dtx.h"
#include "dtx/batch.h"
#include "scheduler/coroutine.h"
#include "scheduler/corotine_scheduler.h"
#include "util/fast_random.h"
#include "storage/storage_service.pb.h"
#include "cache/index_cache.h"
#include "global.h"
#include "base/queue.h"

#include "smallbank/smallbank_txn.h"
#include "tpcc/tpcc_txn.h"

using namespace std::placeholders;

// All the functions are executed in each thread
std::mutex mux;

extern std::atomic<uint64_t> tx_id_generator;
extern std::vector<double> lock_durations;
extern std::vector<t_id_t> tid_vec;
extern std::vector<double> attemp_tp_vec;
extern std::vector<double> tp_vec;
extern std::vector<double> ab_rate;
extern std::vector<double> medianlat_vec;
extern std::vector<double> taillat_vec;
extern std::set<double> fetch_remote_vec;
extern std::set<double> fetch_all_vec;
extern std::set<double> lock_remote_vec;
extern double all_time;
extern double  tx_begin_time,tx_exe_time,tx_commit_time,tx_abort_time,tx_update_time;
extern double tx_get_timestamp_time1, tx_get_timestamp_time2, tx_write_commit_log_time, tx_write_prepare_log_time, tx_write_backup_log_time;
extern double tx_fetch_exe_time, tx_fetch_commit_time, tx_release_exe_time, tx_release_commit_time;
extern double tx_fetch_abort_time, tx_release_abort_time;


extern int single_txn, distribute_txn;

extern std::vector<uint64_t> total_try_times;
extern std::vector<uint64_t> total_commit_times;

DEFINE_string(protocol, "baidu_std", "Protocol type");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_int32(timeout_ms, 0x7fffffff, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

__thread uint64_t seed;                        // Thread-global random seed
__thread FastRandom* random_generator = NULL;  // Per coroutine random generator
__thread t_id_t thread_gid;
__thread t_id_t thread_local_id;
__thread t_id_t thread_num;

std::string bench_name;

__thread SmallBank* smallbank_client = nullptr;
__thread TPCC* tpcc_client = nullptr;

__thread IndexCache* index_cache;
__thread PageCache* page_cache;
__thread MetaManager* meta_man;
__thread ComputeServer* compute_server;

__thread SmallBankTxType* smallbank_workgen_arr;
__thread TPCCTxType* tpcc_workgen_arr;

__thread coro_id_t coro_num;
__thread CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler
__thread CoroutineScheduler* coro_sched_0; // Coroutine 0, use a single sheduler to manage it, only use in long transactions evaluation
__thread int* using_which_coro_sched; // 0=>coro_sched_0, 1=>coro_sched

// Performance measurement (thread granularity)
__thread struct timespec msr_start, msr_end;
__thread double* timer;
__thread std::atomic<uint64_t> stat_attempted_tx_total = 0;  // Issued transaction number
__thread std::atomic<uint64_t> stat_committed_tx_total = 0;  // Committed transaction number
__thread std::atomic<uint64_t> stat_enter_commit_tx_total = 0; // for group commit

__thread brpc::Channel* data_channel;
__thread brpc::Channel* log_channel;
__thread brpc::Channel* remote_server_channel;

// Stat the commit rate
__thread uint64_t* thread_local_try_times;
__thread uint64_t* thread_local_commit_times;

// for thread group commit
__thread bool just_group_commit = false;
__thread std::vector<Txn_request_info>* txn_request_infos;
__thread struct timespec last_commit_log_ts;
__thread TxnLog* thread_txn_log = nullptr;

// for calvin
BatchStore *batch_store;

void RecordTpLat(double msr_sec, DTX* dtx) {
    mux.lock();
  all_time += msr_sec;
  tx_begin_time += dtx->tx_begin_time;
    tx_exe_time += dtx->tx_exe_time;
    tx_commit_time += dtx->tx_commit_time;
    tx_abort_time += dtx->tx_abort_time;
    tx_update_time += dtx->compute_server->tx_update_time;
    tx_fetch_exe_time += dtx->tx_fetch_exe_time;
    tx_fetch_commit_time += dtx->tx_fetch_commit_time;
    tx_fetch_abort_time += dtx->tx_fetch_abort_time;
    tx_release_exe_time += dtx->tx_release_exe_time;
    tx_release_commit_time += dtx->tx_release_commit_time;
    tx_release_abort_time += dtx->tx_release_abort_time;
    tx_get_timestamp_time1 += dtx->tx_get_timestamp_time1;
    tx_get_timestamp_time2 += dtx->tx_get_timestamp_time2;
    tx_write_commit_log_time += dtx->tx_write_commit_log_time;
    tx_write_prepare_log_time += dtx->tx_write_prepare_log_time;
    tx_write_backup_log_time += dtx->tx_write_backup_log_time;

  single_txn += dtx->single_txn;
  distribute_txn += dtx->distribute_txn;
  
  double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
  double tx_tput = (double)stat_committed_tx_total / msr_sec;
  double abort_rate = (double)(stat_attempted_tx_total - stat_committed_tx_total) / stat_attempted_tx_total;
  
  // assert(dtx != nullptr);
  double fetch_remote;
  double fetch_all;
  double lock_remote;
  if (SYSTEM_MODE != 8) {
    fetch_remote = (double)dtx->compute_server->get_node()->get_fetch_remote_cnt() ;
    fetch_all = (double)dtx->compute_server->get_node()->get_fetch_allpage_cnt();
    lock_remote = (double)dtx->compute_server->get_node()->get_lock_remote_cnt();
  }

  std::sort(timer, timer + stat_committed_tx_total);
  double percentile_50 = timer[stat_committed_tx_total / 2];
  double percentile_90 = timer[stat_committed_tx_total * 90 / 100];

  std::cout << "RecordTpLat......" << std::endl;

  tid_vec.push_back(thread_gid);
  attemp_tp_vec.push_back(attemp_tput);
  tp_vec.push_back(tx_tput);
  medianlat_vec.push_back(percentile_50);
  taillat_vec.push_back(percentile_90);
  ab_rate.push_back(abort_rate);
  fetch_remote_vec.emplace(fetch_remote);
  fetch_all_vec.emplace(fetch_all);
  lock_remote_vec.emplace(lock_remote);

  for (size_t i = 0; i < total_try_times.size(); i++) {
    total_try_times[i] += thread_local_try_times[i];
    total_commit_times[i] += thread_local_commit_times[i];
  }

  mux.unlock();
}

void RunSmallBankPSLong(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  struct timespec tx_end_time;
  bool tx_committed = false;
  DTX* dtx = new DTX(meta_man,
                     thread_gid,
                     thread_local_id,
                     coro_id,
                     coro_sched,
                     index_cache,
                     page_cache,
                     compute_server,
                     data_channel,
                     log_channel,
                     remote_server_channel,
                     thread_txn_log, coro_sched_0, using_which_coro_sched);
  SmallBankDTX* bench_dtx = new SmallBankDTX();
  bench_dtx->dtx = dtx;
  dtx->bdtx = bench_dtx;
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  assert(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 7 || SYSTEM_MODE == 12);

  while(compute_server->get_node()->get_phase() == Phase::BEGIN);
  while (true) {
    int cur_epoch = compute_server->get_node()->epoch;
    if(compute_server->get_node()->get_phase() == Phase::SWITCH_TO_GLOBAL || compute_server->get_node()->get_phase() == Phase::SWITCH_TO_PAR){
      if(*using_which_coro_sched == 0){
        coro_sched_0->StopCoroutine(0);
        assert(coro_sched_0->isAllCoroStopped()); // as only one coroutine
        compute_server->get_node()->threads_switch[thread_local_id] = true;
        struct timespec now_time;
        clock_gettime(CLOCK_REALTIME, &now_time);
        auto cid = new brpc::CallId();
        dtx->SendLogToStoragePool(dtx->tx_id, cid);
        brpc::Join(*cid);
        just_group_commit = true;
        last_commit_log_ts = now_time;
        // LOG(INFO) << "group commit..." ;
        if (just_group_commit) {
          clock_gettime(CLOCK_REALTIME, &tx_end_time);
          for(auto r: *txn_request_infos){
            double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
            timer[stat_committed_tx_total++] = tx_usec;
          }
          txn_request_infos->clear();
          just_group_commit = false;
        }
        while(cur_epoch >= compute_server->get_node()->epoch); // wait for switch
        for(coro_id_t coro_id=0; coro_id<coro_num; coro_id++){
          coro_sched->StartCoroutine(coro_id);
        }
        *using_which_coro_sched = 1;
        coro_sched->RunCoroutine(yield, 0); // run coroutine 0
        continue;
      }
      else{
        coro_sched->StopCoroutine(coro_id); // stop this coroutine
        while(!coro_sched->isAllCoroStopped() && cur_epoch >= compute_server->get_node()->epoch){
          coro_sched->Yield(yield, coro_id);
          // LOG(INFO) << "coro_id: " << coro_id << " is yielded";
        }
        if(coro_sched->isAllCoroStopped()){
          compute_server->get_node()->threads_switch[thread_local_id] = true; // stop and ready to switch
          struct timespec now_time;
          clock_gettime(CLOCK_REALTIME, &now_time);
          auto cid = new brpc::CallId();
          dtx->SendLogToStoragePool(dtx->tx_id, cid);
          brpc::Join(*cid);
          just_group_commit = true;
          last_commit_log_ts = now_time;
          // LOG(INFO) << "group commit..." ;
          if (just_group_commit) {
            clock_gettime(CLOCK_REALTIME, &tx_end_time);
            for(auto r: *txn_request_infos){
              double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
              timer[stat_committed_tx_total++] = tx_usec;
            }
            txn_request_infos->clear();
            just_group_commit = false;
          }
        }
        while(cur_epoch >= compute_server->get_node()->epoch); // wait for switch
        if(coro_sched->isAllCoroStopped()){
          coro_sched_0->StartCoroutine(0);
          *using_which_coro_sched = 0;
          coro_sched_0->RunCoroutine(yield, 0);
          continue;
        }
      }
    }

    // run transaction
    bool fill_txn = false;
    Txn_request_info txn_meta;
    bool is_par_txn;
    if(compute_server->get_node()->get_phase() == Phase::PARTITION || compute_server->get_node()->get_phase() == Phase::SWITCH_TO_GLOBAL){
      // 判断partitioned_txn_list是否为空, 如果为空则生成新的page id
      is_par_txn = true;
      std::unique_lock<std::mutex> l(compute_server->get_node()->getTxnQueueMutex());
      if(!compute_server->get_node()->get_partitioned_txn_queue().empty()){
        // 从partitioned_txn_list取出一个seed
        txn_meta = compute_server->get_node()->get_partitioned_txn_queue().front();
        compute_server->get_node()->get_partitioned_txn_queue().pop();
        fill_txn = true;
      }
      else {
        // 生成一个partitioned txn
        bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
        if(!is_partitioned){
          if(compute_server->get_node()->get_global_txn_queue().size() >= (size_t)compute_server->get_node()->max_global_txn_queue) continue;
          // push back to global_txn_list
          txn_meta.seed = FastRand(&seed); // 暂存seed
          clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
          compute_server->get_node()->get_global_txn_queue().push(txn_meta);
        }
        else {
          txn_meta.seed = FastRand(&seed); // 暂存seed
          clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
          fill_txn = true;
        }
      }
    }
    else if(compute_server->get_node()->get_phase() == Phase::GLOBAL || compute_server->get_node()->get_phase() == Phase::SWITCH_TO_PAR){
      // 判断global_txn_list是否为空, 如果为空则生成新的page id
      is_par_txn = false;
      std::unique_lock<std::mutex> l(compute_server->get_node()->getTxnQueueMutex());
      if(!compute_server->get_node()->get_global_txn_queue().empty()){
        // 从global_txn_list取出一个seed
        txn_meta = compute_server->get_node()->get_global_txn_queue().front();
        compute_server->get_node()->get_global_txn_queue().pop();
        fill_txn = true;
      }
      else {
        // 生成一个global txn
        bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
        if(is_partitioned){
          if(compute_server->get_node()->get_partitioned_txn_queue().size() >= (size_t)compute_server->get_node()->max_par_txn_queue) continue;
          // push back to partitioned_txn_list
          txn_meta.seed = FastRand(&seed); // 暂存seed
          clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
          compute_server->get_node()->get_partitioned_txn_queue().push(txn_meta);
        }
        else {
          txn_meta.seed = FastRand(&seed); // 暂存seed
          clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
          fill_txn = true;
        }
      }
    }
    else assert(false);
    
    for(int i = 0; i < 5; i++){
      FastRand(&seed); // 更新seed, 防止生成相同的seed, 为每个事务分配了5个seed
    }
    if(!fill_txn) continue;

    SmallBankTxType tx_type = smallbank_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;
    
    // TLOG(INFO, thread_gid) << "tx: " << iter << " coroutine: " << coro_id << " tx_type: " << (int)tx_type;
    // printf("worker.cc:326, start a new txn\n");
    if(is_par_txn) {
      compute_server->get_node()->partition_cnt++;
      compute_server->get_node()->stat_partition_cnt++;
      // // ! 与原写法不同，暂时
      // if(compute_server->get_node()->partition_cnt >= EpochOptCount*(1-CrossNodeAccessRatio)){
      //     // 通知切换线程
      //     std::unique_lock<std::mutex> lck(compute_server->get_node()->phase_switch_mutex);
      //     compute_server->get_node()->phase_switch_cv.notify_one();
      // }
    }
    else{
      compute_server->get_node()->global_cnt++;
      compute_server->get_node()->stat_global_cnt++;
      // // ! 与原写法不同，暂时
      // if(compute_server->get_node()->global_cnt >= EpochOptCount*CrossNodeAccessRatio){
      //     // 通知切换线程
      //     std::unique_lock<std::mutex> lck(compute_server->get_node()->phase_switch_mutex);
      //     compute_server->get_node()->phase_switch_cv.notify_one();
      // }
    }
    
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxAmalgamate(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          } else{
            tx_committed = bench_dtx->TxAmalgamate(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kBalance: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxBalance(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          } else{
            tx_committed = bench_dtx->TxBalance(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kDepositChecking: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxDepositChecking(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          } else{
            tx_committed = bench_dtx->TxDepositChecking(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kSendPayment: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxSendPayment(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          } else{
            tx_committed = bench_dtx->TxSendPayment(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kTransactSaving: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxTransactSaving(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          } else{
            tx_committed = bench_dtx->TxTransactSaving(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kWriteCheck: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxWriteCheck(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          } else{
            tx_committed = bench_dtx->TxWriteCheck(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    // printf("try %d transaction commit? %s\n", stat_attempted_tx_total, tx_committed?"true":"false");
  #if !GroupCommit
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
  #else
    if(SYSTEM_MODE <=7){
      if(is_par_txn){
        // 增加一个误判记录
        if(FastRand(&seed) % 100 < WrongPrediction * 100){
          compute_server->get_node()->partition_cnt--;
          tx_committed = false;
          thread_local_commit_times[uint64_t(tx_type)]--;
        }
      }
      if (tx_committed) {
        // 记录事务开始时间
        txn_request_infos->push_back(txn_meta);
        if(is_par_txn){
          compute_server->get_node()->stat_commit_partition_cnt++;
        } else{
          compute_server->get_node()->stat_commit_global_cnt++;
        }
      }
      else{
        if(is_par_txn){
          compute_server->get_node()->partition_cnt--;
        } else{
          compute_server->get_node()->global_cnt--;
        }
      }
      stat_enter_commit_tx_total++;
    }
    else assert(false);
  #endif
    if (stat_attempted_tx_total >= ATTEMPTED_NUM || stat_enter_commit_tx_total >= ATTEMPTED_NUM) {
  #if GroupCommit
    if(SYSTEM_MODE <=7){
      struct timespec now_time;
      clock_gettime(CLOCK_REALTIME, &now_time);
      auto cid = new brpc::CallId();
        dtx->SendLogToStoragePool(dtx->tx_id, cid);
        brpc::Join(*cid);
        just_group_commit = true;
        last_commit_log_ts = now_time;
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
        for(auto r: *txn_request_infos){
          double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
          timer[stat_committed_tx_total++] = tx_usec;
        }
      txn_request_infos->clear();
      just_group_commit = false;
    }
  #endif
      break;
    }
    // coro_sched->Yield(yield, coro_id);
  }
    /********************************** Stat end *****************************************/
  // remove from the coroutine scheduler
  if(*using_which_coro_sched == 0){
    coro_sched_0->FinishCorotine(0);
  }else{
    coro_sched->FinishCorotine(coro_id);
    LOG(INFO) << "thread_local_id: " << thread_local_id << " coro_id: " << coro_id << " is stopped";
    while(coro_sched->isAllCoroStopped() == false) {
        // LOG(INFO) << coro_id << " *yield ";
        coro_sched->Yield(yield, coro_id);
    }
  }
  if(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 7 || SYSTEM_MODE == 12) 
        compute_server->get_node()->threads_finish[thread_local_id] = true;
  // A coroutine calculate the total execution time and exits
  clock_gettime(CLOCK_REALTIME, &msr_end);
  // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
  double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
  RecordTpLat(msr_sec,dtx);
  delete bench_dtx;
}

void RunSmallBankPS(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  struct timespec tx_end_time;
  bool tx_committed = false;
  DTX* dtx = new DTX(meta_man,
                     thread_gid,
                     thread_local_id,
                     coro_id,
                     coro_sched,
                     index_cache,
                     page_cache,
                     compute_server,
                     data_channel,
                     log_channel,
                     remote_server_channel,
                     thread_txn_log);
  SmallBankDTX* bench_dtx = new SmallBankDTX();
  bench_dtx->dtx = dtx;
  dtx->bdtx = bench_dtx;
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  assert(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 7 || SYSTEM_MODE == 12);

  while(compute_server->get_node()->get_phase() == Phase::BEGIN);
  while (true) {

    int cur_epoch = compute_server->get_node()->epoch;
    if(SYSTEM_MODE == 12 && compute_server->get_node()->get_phase() == Phase::GLOBAL && compute_server->get_node()->getNodeID() != 0){
       continue; // star
    }
    // 判断是否需要切换
    if(compute_server->get_node()->get_phase() == Phase::SWITCH_TO_GLOBAL || compute_server->get_node()->get_phase() == Phase::SWITCH_TO_PAR){
        // LOG(INFO) << "Node " << compute_server->get_node()->getNodeID() << " Thread " << thread_local_id << " is ready to switch";
        coro_sched->StopCoroutine(coro_id); // stop this coroutine
        // LOG(INFO) << "coro_id: " << coro_id << " is stopped";
        while(!coro_sched->isAllCoroStopped() && cur_epoch >= compute_server->get_node()->epoch){
          coro_sched->Yield(yield, coro_id);
          // LOG(INFO) << "coro_id: " << coro_id << " is yielded";
        }
        if(coro_sched->isAllCoroStopped()){
          compute_server->get_node()->threads_switch[thread_local_id] = true; // stop and ready to switch
          struct timespec now_time;
          clock_gettime(CLOCK_REALTIME, &now_time);
          auto cid = new brpc::CallId();
          dtx->SendLogToStoragePool(dtx->tx_id, cid);
          brpc::Join(*cid);
          just_group_commit = true;
          last_commit_log_ts = now_time;
          // LOG(INFO) << "group commit..." ;
          if (just_group_commit) {
            clock_gettime(CLOCK_REALTIME, &tx_end_time);
            for(auto r: *txn_request_infos){
              double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
              timer[stat_committed_tx_total++] = tx_usec;
            }
            txn_request_infos->clear();
            just_group_commit = false;
          }
        }
        while(cur_epoch >= compute_server->get_node()->epoch); // wait for switch
        // while(compute_server->get_node()->get_phase() == Phase::SWITCH_TO_GLOBAL || compute_server->get_node()->get_phase() == Phase::SWITCH_TO_PAR){
        //   // ! 注意， 此处代码块在多线程可能会卡住，如果epoch count设置过小导致此线程还未开始执行事务，又进入了切换阶段, 此时会导致线程卡住
        //   // ! 因为单节点没有global 阶段，所以更容易出现这种情况
        //     // std::this_thread::sleep_for(std::chrono::microseconds(1)); // sleep 5us
        // } // wait for switch
        if(coro_sched->isAllCoroStopped()){
          // LOG(INFO) << "coro_id: " << coro_id << " start all coroutines";
          for(coro_id_t coro_id=0; coro_id<coro_num; coro_id++){
            coro_sched->StartCoroutine(coro_id);
          }
        }
        continue;
    }

    // run transaction
    bool fill_txn = false;
    Txn_request_info txn_meta;
    bool is_par_txn;
    if(compute_server->get_node()->get_phase() == Phase::PARTITION || compute_server->get_node()->get_phase() == Phase::SWITCH_TO_GLOBAL){
      // 判断partitioned_txn_list是否为空, 如果为空则生成新的page id
      is_par_txn = true;
      std::unique_lock<std::mutex> l(compute_server->get_node()->getTxnQueueMutex());
      if(!compute_server->get_node()->get_partitioned_txn_queue().empty()){
        // 从partitioned_txn_list取出一个seed
        txn_meta = compute_server->get_node()->get_partitioned_txn_queue().front();
        compute_server->get_node()->get_partitioned_txn_queue().pop();
        fill_txn = true;
      }
      else {
        // 生成一个partitioned txn
        bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
        if(!is_partitioned){
          if(compute_server->get_node()->get_global_txn_queue().size() >= (size_t)compute_server->get_node()->max_global_txn_queue) continue;
          // push back to global_txn_list
          txn_meta.seed = FastRand(&seed); // 暂存seed
          clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
          compute_server->get_node()->get_global_txn_queue().push(txn_meta);
        }
        else {
          txn_meta.seed = FastRand(&seed); // 暂存seed
          clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
          fill_txn = true;
        }
      }
    }
    else if(compute_server->get_node()->get_phase() == Phase::GLOBAL || compute_server->get_node()->get_phase() == Phase::SWITCH_TO_PAR){
      // 判断global_txn_list是否为空, 如果为空则生成新的page id
      is_par_txn = false;
      std::unique_lock<std::mutex> l(compute_server->get_node()->getTxnQueueMutex());
      if(!compute_server->get_node()->get_global_txn_queue().empty()){
        // 从global_txn_list取出一个seed
        txn_meta = compute_server->get_node()->get_global_txn_queue().front();
        compute_server->get_node()->get_global_txn_queue().pop();
        fill_txn = true;
      }
      else {
        // 生成一个global txn
        bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
        if(is_partitioned){
          if(compute_server->get_node()->get_partitioned_txn_queue().size() >= (size_t)compute_server->get_node()->max_par_txn_queue) continue;
          // push back to partitioned_txn_list
          txn_meta.seed = FastRand(&seed); // 暂存seed
          clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
          compute_server->get_node()->get_partitioned_txn_queue().push(txn_meta);
        }
        else {
          txn_meta.seed = FastRand(&seed); // 暂存seed
          clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
          fill_txn = true;
        }
      }
    }
    else assert(false);
    
    for(int i = 0; i < 5; i++){
      FastRand(&seed); // 更新seed, 防止生成相同的seed, 为每个事务分配了5个seed
    }
    if(!fill_txn) continue;

    SmallBankTxType tx_type = smallbank_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;
    
    // TLOG(INFO, thread_gid) << "tx: " << iter << " coroutine: " << coro_id << " tx_type: " << (int)tx_type;
    // printf("worker.cc:326, start a new txn\n");
    if(is_par_txn) {
      compute_server->get_node()->partition_cnt++;
      compute_server->get_node()->stat_partition_cnt++;
      // // ! 与原写法不同，暂时
      // if(compute_server->get_node()->partition_cnt >= EpochOptCount*(1-CrossNodeAccessRatio)){
      //     // 通知切换线程
      //     std::unique_lock<std::mutex> lck(compute_server->get_node()->phase_switch_mutex);
      //     compute_server->get_node()->phase_switch_cv.notify_one();
      // }
    }
    else{
      compute_server->get_node()->global_cnt++;
      compute_server->get_node()->stat_global_cnt++;
      // // ! 与原写法不同，暂时
      // if(compute_server->get_node()->global_cnt >= EpochOptCount*CrossNodeAccessRatio){
      //     // 通知切换线程
      //     std::unique_lock<std::mutex> lck(compute_server->get_node()->phase_switch_mutex);
      //     compute_server->get_node()->phase_switch_cv.notify_one();
      // }
    }
    
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxAmalgamate(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kBalance: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxBalance(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kDepositChecking: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxDepositChecking(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kSendPayment: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxSendPayment(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kTransactSaving: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxTransactSaving(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kWriteCheck: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxWriteCheck(smallbank_client, &txn_meta.seed, yield, iter, dtx, is_par_txn);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    // printf("try %d transaction commit? %s\n", stat_attempted_tx_total, tx_committed?"true":"false");
  #if !GroupCommit
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
  #else
    if(SYSTEM_MODE <=7 || SYSTEM_MODE == 12){
      if(is_par_txn){
        // 增加一个误判记录
        if(FastRand(&seed) % 100 < WrongPrediction * 100){
          compute_server->get_node()->partition_cnt--;
          tx_committed = false;
          thread_local_commit_times[uint64_t(tx_type)]--;
        }
      }
      if (tx_committed) {
        // 记录事务开始时间
        txn_request_infos->push_back(txn_meta);
        if(is_par_txn){
          compute_server->get_node()->stat_commit_partition_cnt++;
        } else{
          compute_server->get_node()->stat_commit_global_cnt++;
        }
      }
      else{
        if(is_par_txn){
          compute_server->get_node()->partition_cnt--;
        } else{
          compute_server->get_node()->global_cnt--;
        }
      }
      stat_enter_commit_tx_total++;
    }
    else assert(false);
  #endif
    if (stat_attempted_tx_total >= ATTEMPTED_NUM || stat_enter_commit_tx_total >= ATTEMPTED_NUM || (SYSTEM_MODE == 12 && compute_server->get_node()->getNodeID() != 0 && stat_enter_commit_tx_total >= ATTEMPTED_NUM * (1-CrossNodeAccessRatio))) {
  #if GroupCommit
    if(SYSTEM_MODE <=7 || SYSTEM_MODE == 12){
      struct timespec now_time;
      clock_gettime(CLOCK_REALTIME, &now_time);
      auto cid = new brpc::CallId();
        dtx->SendLogToStoragePool(dtx->tx_id, cid);
        brpc::Join(*cid);
        just_group_commit = true;
        last_commit_log_ts = now_time;
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
        for(auto r: *txn_request_infos){
          double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
          timer[stat_committed_tx_total++] = tx_usec;
        }
      txn_request_infos->clear();
      just_group_commit = false;
    }
  #endif
      break;
    }
    coro_sched->Yield(yield, coro_id);
  }
    /********************************** Stat end *****************************************/
  // remove from the coroutine scheduler
  coro_sched->FinishCorotine(coro_id);
  LOG(INFO) << "thread_local_id: " << thread_local_id << " coro_id: " << coro_id << " is stopped";
  while(coro_sched->isAllCoroStopped() == false) {
      // LOG(INFO) << coro_id << " *yield ";
      coro_sched->Yield(yield, coro_id);
  }
  if(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 7 || SYSTEM_MODE == 12) 
        compute_server->get_node()->threads_finish[thread_local_id] = true;
  // A coroutine calculate the total execution time and exits
  clock_gettime(CLOCK_REALTIME, &msr_end);
  // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
  double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
  RecordTpLat(msr_sec,dtx);
  delete bench_dtx;
}

void RunSmallBankLong(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  struct timespec tx_end_time;
  bool tx_committed = false;

  DTX* dtx = new DTX(meta_man,
                     thread_gid,
                     thread_local_id,
                     coro_id,
                     coro_sched,
                     index_cache,
                     page_cache,
                     compute_server,
                     data_channel,
                     log_channel,
                     remote_server_channel,
                     thread_txn_log);
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  SmallBankDTX* bench_dtx = new SmallBankDTX();
  bench_dtx->dtx = dtx;
  dtx->bdtx = bench_dtx;
  while (true) {
    bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
    SmallBankTxType tx_type = smallbank_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;

    // TLOG(INFO, thread_gid) << "tx: " << iter << " coroutine: " << coro_id << " tx_type: " << (int)tx_type;
    
    Txn_request_info txn_meta;
    clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);

    // printf("worker.cc:326, start a new txn\n");
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxAmalgamate(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          } else{
            tx_committed = bench_dtx->TxAmalgamate(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kBalance: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxBalance(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          } else{
            tx_committed = bench_dtx->TxBalance(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kDepositChecking: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxDepositChecking(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          } else{
            tx_committed = bench_dtx->TxDepositChecking(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kSendPayment: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxSendPayment(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          } else{
            tx_committed = bench_dtx->TxSendPayment(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kTransactSaving: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxTransactSaving(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          } else{
            tx_committed = bench_dtx->TxTransactSaving(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kWriteCheck: {
          thread_local_try_times[uint64_t(tx_type)]++;
          if(FastRand(&seed) % 100 < LongTxnRate * 100){ // is long transaction
            tx_committed = bench_dtx->LongTxWriteCheck(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          } else{
            tx_committed = bench_dtx->TxWriteCheck(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          }
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    // printf("try %d transaction commit? %s\n", stat_attempted_tx_total, tx_committed?"true":"false");
  #if !GroupCommit
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
  #else
    if(SYSTEM_MODE <=7 || SYSTEM_MODE == 10 || SYSTEM_MODE == 11){
      if (tx_committed) {
        // 记录事务开始时间
        txn_request_infos->push_back(txn_meta);
      }
      stat_enter_commit_tx_total++;
      struct timespec now_time;
      clock_gettime(CLOCK_REALTIME, &now_time);
      auto diff_ms = (now_time.tv_sec - last_commit_log_ts.tv_sec)* 1000LL + (double)(now_time.tv_nsec - last_commit_log_ts.tv_nsec) / 1000000;
      if(diff_ms >= EpochTime){
        auto cid = new brpc::CallId();
        dtx->SendLogToStoragePool(dtx->tx_id, cid);
        brpc::Join(*cid);
        just_group_commit = true;
        last_commit_log_ts = now_time;
        // LOG(INFO) << "group commit..." ;
      }
      if (just_group_commit) {
        clock_gettime(CLOCK_REALTIME, &tx_end_time);
        for(auto r: *txn_request_infos){
          double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
          timer[stat_committed_tx_total++] = tx_usec;
        }
        txn_request_infos->clear();
        just_group_commit = false;
      }
    }
    else if(SYSTEM_MODE == 9){
      if (tx_committed) {
        clock_gettime(CLOCK_REALTIME, &tx_end_time);
        double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
        timer[stat_committed_tx_total++] = tx_usec + dtx->two_latency_c * 1000;
      }
    }
  #endif
    if (stat_attempted_tx_total >= ATTEMPTED_NUM || stat_enter_commit_tx_total >= ATTEMPTED_NUM) {
  #if GroupCommit
    if(SYSTEM_MODE <=7 || SYSTEM_MODE == 10 || SYSTEM_MODE == 11){
      struct timespec now_time;
      clock_gettime(CLOCK_REALTIME, &now_time);
      auto cid = new brpc::CallId();
        dtx->SendLogToStoragePool(dtx->tx_id, cid);
        brpc::Join(*cid);
        just_group_commit = true;
        last_commit_log_ts = now_time;
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
        for(auto r: *txn_request_infos){
          double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
          timer[stat_committed_tx_total++] = tx_usec;
        }
      txn_request_infos->clear();
      just_group_commit = false;
    }
  #endif
      break;
    }
    /********************************** Stat end *****************************************/
    coro_sched->Yield(yield, coro_id);
  }
  coro_sched->FinishCorotine(coro_id);
  LOG(INFO) << "thread_local_id: " << thread_local_id << " coro_id: " << coro_id << " is stopped";
  while(coro_sched->isAllCoroStopped() == false) {
      // LOG(INFO) << coro_id << " *yield ";
      coro_sched->Yield(yield, coro_id);
  }
  // A coroutine calculate the total execution time and exits
  clock_gettime(CLOCK_REALTIME, &msr_end);
  // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
  double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
  RecordTpLat(msr_sec,dtx);
  delete bench_dtx;
}

void RunSmallBank(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  struct timespec tx_end_time;
  bool tx_committed = false;

  DTX* dtx = new DTX(meta_man,
                     thread_gid,
                     thread_local_id,
                     coro_id,
                     coro_sched,
                     index_cache,
                     page_cache,
                     compute_server,
                     data_channel,
                     log_channel,
                     remote_server_channel,
                     thread_txn_log);
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  SmallBankDTX* bench_dtx = new SmallBankDTX();
  bench_dtx->dtx = dtx;
  dtx->bdtx = bench_dtx;
  while (true) {
    bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
    SmallBankTxType tx_type = smallbank_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;

    // TLOG(INFO, thread_gid) << "tx: " << iter << " coroutine: " << coro_id << " tx_type: " << (int)tx_type;
    
    Txn_request_info txn_meta;
    clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);

    // printf("worker.cc:326, start a new txn\n");
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxAmalgamate(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kBalance: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxBalance(smallbank_client, &seed, yield, iter, dtx,is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kDepositChecking: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxDepositChecking(smallbank_client, &seed, yield, iter, dtx,is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kSendPayment: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxSendPayment(smallbank_client, &seed, yield, iter, dtx,is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kTransactSaving: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxTransactSaving(smallbank_client, &seed, yield, iter, dtx,is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kWriteCheck: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxWriteCheck(smallbank_client, &seed, yield, iter, dtx,is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    // printf("try %d transaction commit? %s\n", stat_attempted_tx_total, tx_committed?"true":"false");
  #if !GroupCommit
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
  #else
    if(SYSTEM_MODE <=7 || SYSTEM_MODE == 10 || SYSTEM_MODE == 11){
      if (tx_committed) {
        // 记录事务开始时间
        txn_request_infos->push_back(txn_meta);
      }
      stat_enter_commit_tx_total++;
      struct timespec now_time;
      clock_gettime(CLOCK_REALTIME, &now_time);
      auto diff_ms = (now_time.tv_sec - last_commit_log_ts.tv_sec)* 1000LL + (double)(now_time.tv_nsec - last_commit_log_ts.tv_nsec) / 1000000;
      if(diff_ms >= EpochTime){
        auto cid = new brpc::CallId();
        dtx->SendLogToStoragePool(dtx->tx_id, cid);
        brpc::Join(*cid);
        just_group_commit = true;
        last_commit_log_ts = now_time;
        // LOG(INFO) << "group commit..." ;
      }
      if (just_group_commit) {
        clock_gettime(CLOCK_REALTIME, &tx_end_time);
        for(auto r: *txn_request_infos){
          double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
          timer[stat_committed_tx_total++] = tx_usec;
        }
        txn_request_infos->clear();
        just_group_commit = false;
      }
    }
    else if(SYSTEM_MODE == 9){
      if (tx_committed) {
        clock_gettime(CLOCK_REALTIME, &tx_end_time);
        double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
        timer[stat_committed_tx_total++] = tx_usec + dtx->two_latency_c * 1000;
      }
    }
  #endif
    if (stat_attempted_tx_total >= ATTEMPTED_NUM || stat_enter_commit_tx_total >= ATTEMPTED_NUM) {
  #if GroupCommit
    if(SYSTEM_MODE <=7 || SYSTEM_MODE == 10 || SYSTEM_MODE == 11){
      struct timespec now_time;
      clock_gettime(CLOCK_REALTIME, &now_time);
      auto cid = new brpc::CallId();
        dtx->SendLogToStoragePool(dtx->tx_id, cid);
        brpc::Join(*cid);
        just_group_commit = true;
        last_commit_log_ts = now_time;
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
        for(auto r: *txn_request_infos){
          double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
          timer[stat_committed_tx_total++] = tx_usec;
        }
      txn_request_infos->clear();
      just_group_commit = false;
    }
  #endif
      break;
    }
    /********************************** Stat end *****************************************/
    coro_sched->Yield(yield, coro_id);
  }
  coro_sched->FinishCorotine(coro_id);
  LOG(INFO) << "thread_local_id: " << thread_local_id << " coro_id: " << coro_id << " is stopped";
  while(coro_sched->isAllCoroStopped() == false) {
      // LOG(INFO) << coro_id << " *yield ";
      coro_sched->Yield(yield, coro_id);
  }
  // A coroutine calculate the total execution time and exits
  clock_gettime(CLOCK_REALTIME, &msr_end);
  // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
  double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
  RecordTpLat(msr_sec,dtx);
  delete bench_dtx;
}

void LockDTX(coro_yield_t& yield, coro_id_t coro_id, ComputeServer* compute_server) {
  // Running transactions
  while (!system_finish) {
    BenchDTX* dtx;
    dtx = queue->sched_dequeue();
    if (!dtx) continue;
    /********************************** Start locking *****************************************/
    // clock_gettime(CLOCK_REALTIME, &tx_start_time);
    // dtx->dtx->tx_start_time = tx_start_time;
    // 本地加锁
    for (auto &dataitem : dtx->dtx->read_only_set) {
      DataItemPtr item = dataitem.item_ptr;
      Rid rid = dtx->dtx->index_cache->Search(item->table_id,item->key);
      node_id_t node_id = compute_server->get_node_id_by_page_id_new(item->table_id, rid.page_no_);
      dataitem.node_id = node_id;
      dtx->dtx->all_ids.insert(node_id);
      if(node_id != machine_id){
        dataitem.lock_mode = lock_mode_type::WAIT_DIE;
        dataitem.has_locked = lock_status::LOCK_NO;
        dtx->dtx->wait_ids.insert(node_id);
        dtx->dtx->another_wait_ids.insert(node_id);
        continue;
      }
      dataitem.lock_mode = lock_mode_type::WAIT_DIE;
      lock_manager->GetSHLock(dtx,&dataitem);
      // item->lock_manager.GetSHLock(dtx,&dataitem);
    }
    for (auto &dataitem : dtx->dtx->read_write_set) {
      DataItemPtr item = dataitem.item_ptr;
      Rid rid = dtx->dtx->index_cache->Search(item->table_id,item->key);
      node_id_t node_id = compute_server->get_node_id_by_page_id_new(item->table_id, rid.page_no_);
      dataitem.node_id = node_id;
      dtx->dtx->all_ids.insert(node_id);
      if(node_id != machine_id){
        dataitem.lock_mode = lock_mode_type::WAIT_DIE;
        dataitem.has_locked = lock_status::LOCK_NO;
        dtx->dtx->wait_ids.insert(node_id);
        dtx->dtx->another_wait_ids.insert(node_id);
        continue;
      }

      dataitem.lock_mode = lock_mode_type::WAIT_DIE;
      lock_manager->GetEXLock(dtx,&dataitem);
      // item->lock_manager.GetEXLock(dtx,&dataitem);
    }
    // 如果不是本地的事务，而且在本地没有任何操作，则直接丢弃
    if (dtx->node_id!=machine_id && dtx->dtx->all_ids.find(machine_id) == dtx->dtx->all_ids.end()) {
      // LOG(FATAL) << "tx: " << dtx->dtx->tx_id << " not need execute";
      continue;
    }
    if (dtx->dtx->GetAllLock()) {
      if(__sync_bool_compare_and_swap(&dtx->lock_ready,false,true)) {
        dtx->stage = CalvinStages::READ;
        // 如果是本地的事务，但是在本地没有任何操作，那么扔到worker队列中
        if (dtx->dtx->all_ids.find(machine_id) == dtx->dtx->all_ids.end()) {
          dtx->stage = CalvinStages::WRITE;
        } 
        queue->enqueue(dtx,dtx->bid,true);
        CALVIN_LOG(INFO) << "DTX: "<< dtx->bid << "-"<< dtx->dtx->tx_id << " enqueue, ready to enter read";
      }
    } else {
      // 没加到锁，输出一个日志
      // LOG(INFO) << "tx: " << dtx->dtx->tx_id << " not get all lock";
      // lock_wait_tx_total++;
      // wait_calvin_txn_mutex->lock();
      // waiting_calvin_txns->insert(dtx);
      // wait_calvin_txn_mutex->unlock();
    }
    /********************************** End locking *****************************************/
  }
}

void SendBatchToLocal(coro_yield_t& yield, coro_id_t coro_id, Batch* batch, t_id_t node_id) {
  for (int i = 0; i < batch->current_txn_cnt; i ++) {
    // Send to local scheduler
    queue->sched_enqueue(batch->txn_list[i], batch->batch_id, node_id, TXN);
  }
  queue->sched_enqueue(nullptr, batch->batch_id, node_id, RDONE);
}

void SendBatchToRemote(coro_yield_t& yield, coro_id_t coro_id, Batch* batch, t_id_t node_id) {
  std::vector<dtx_entry> txns;
  // 目标节点为0到g_node_cnt，不包括本地节点
  std::vector<t_id_t> target_nodes;
  for (int i = 0; i < g_node_cnt; i++) {
    if (i != machine_id) {
      target_nodes.push_back(i);
    }
  }

  for (int i = 0; i < batch->current_txn_cnt; i++) {
    if (bench_name == "smallbank") {
      SmallBankDTX* bench_dtx = (SmallBankDTX*)batch->txn_list[i];
      txns.push_back(dtx_entry(bench_dtx->seed,(int)bench_dtx->type,bench_dtx->dtx->tx_id,bench_dtx->is_partitioned));
    }
  }
  for (t_id_t target:target_nodes) {
    // 写一个日志，输出发送的batch
    // LOG(INFO) << "Send a batch to remote, bid: "<< batch->batch_id << " target: " << target;
    compute_server->SendBatch(txns, batch->batch_id, target, machine_id);
  }
}

void GenerateSmmallBankFromRemote(coro_yield_t& yield, coro_id_t coro_id) {
  while (!system_finish) {
    batch_entry *entry = queue->seq_dequeue();
    if (!entry) continue;
    Batch* batch = new Batch(entry->bid);
    // 帮忙输出接收到远程事务的日志
    // LOG(INFO) << "Receive a remote txn, bid: "<< entry->bid;
    for (int i = 0; i < entry->txns.size(); i++) {
      DTX* dtx = new DTX(meta_man,
                     thread_gid,
                     thread_local_id,
                     coro_id,
                     coro_sched,
                     index_cache,
                     page_cache,
                     compute_server,
                     data_channel,
                     log_channel,
                     remote_server_channel);
      SmallBankTxType tx_type = (SmallBankTxType)entry->txns[i].type;

      SmallBankDTX* bench_dtx = new SmallBankDTX();
      bench_dtx->dtx = dtx;
      bench_dtx->type = tx_type;
      
      dtx->bdtx = bench_dtx;
      bench_dtx->node_id = entry->nid;

      
      bool is_partitioned = entry->txns[i].is_partitioned;
      uint64_t rseed = entry->txns[i].seed;
      bench_dtx->seed = rseed;
      bench_dtx->is_partitioned = is_partitioned;
      bench_dtx->bid = entry->bid;
      node_id_t node_id = entry->nid;
      switch (tx_type) {
        case SmallBankTxType::kAmalgamate: {
            thread_local_try_times[uint64_t(tx_type)]++;
            bench_dtx->GenTxAmalgamate(smallbank_client, &rseed, yield, entry->txns[i].tid, dtx,is_partitioned, node_id);
            // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case SmallBankTxType::kBalance: {
            thread_local_try_times[uint64_t(tx_type)]++;
            bench_dtx->GenTxBalance(smallbank_client, &rseed, yield, entry->txns[i].tid, dtx,is_partitioned, node_id);
            // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case SmallBankTxType::kDepositChecking: {
            thread_local_try_times[uint64_t(tx_type)]++;
            bench_dtx->GenTxDepositChecking(smallbank_client, &rseed, yield, entry->txns[i].tid, dtx,is_partitioned, node_id);
            // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case SmallBankTxType::kSendPayment: {
            thread_local_try_times[uint64_t(tx_type)]++;
            bench_dtx->GenTxSendPayment(smallbank_client, &rseed, yield, entry->txns[i].tid, dtx,is_partitioned, node_id);
            // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case SmallBankTxType::kTransactSaving: {
            thread_local_try_times[uint64_t(tx_type)]++;
            bench_dtx->GenTxTransactSaving(smallbank_client, &rseed, yield, entry->txns[i].tid, dtx,is_partitioned, node_id);
            // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        case SmallBankTxType::kWriteCheck: {
            thread_local_try_times[uint64_t(tx_type)]++;
            bench_dtx->GenTxWriteCheck(smallbank_client, &rseed, yield, entry->txns[i].tid, dtx,is_partitioned, node_id);
            // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
          break;
        }
        default:
          printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
          abort();
      }
      /*************************** Generate batch and send begin ***************************/
      batch->InsertTxn(bench_dtx);
    }
    // todo: 目前没有考虑消息发过来的顺序，之后得确认下batch顺序是否一致
    SendBatchToLocal(yield, coro_id, batch, entry->nid);
    // 如果当前收到的bid大于本地的bid，就应该暂时停止接收,break出去
    // if (entry->bid > batch_store->GetMaxBatchID()) {
    //   break;
    // }
  }
}

void GenerateSmallBank(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  // Running transactions
  Batch* batch = new Batch(batch_store->GenerateBatchID());
  while (!system_finish) {
    // 处理远程的batch
    // GenerateSmmallBankFromRemote(yield, coro_id);
    // todo 先检查batch执行中的batch是不是满了
    // if (batch_store->isFull()) {
      // continue;
    // }
    // 若没有满，则生成新事务
    DTX* dtx = new DTX(meta_man,
                     thread_gid,
                     thread_local_id,
                     coro_id,
                     coro_sched,
                     index_cache,
                     page_cache,
                     compute_server,
                     data_channel,
                     log_channel,
                     remote_server_channel);
    
    clock_gettime(CLOCK_REALTIME, &dtx->tx_start_time);

    SmallBankDTX* bench_dtx = new SmallBankDTX();
    bench_dtx->dtx = dtx;
    bench_dtx->node_id = machine_id;
    dtx->bdtx = bench_dtx;
    SmallBankTxType tx_type = smallbank_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    
    bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
    bench_dtx->type = tx_type;
    bench_dtx->seed=seed;
    bench_dtx->is_partitioned = is_partitioned;
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate: {
          thread_local_try_times[uint64_t(tx_type)]++;
          bench_dtx->GenTxAmalgamate(smallbank_client, &seed, yield, iter, dtx,is_partitioned, machine_id);
          // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kBalance: {
          thread_local_try_times[uint64_t(tx_type)]++;
          bench_dtx->GenTxBalance(smallbank_client, &seed, yield, iter, dtx,is_partitioned, machine_id);
          // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kDepositChecking: {
          thread_local_try_times[uint64_t(tx_type)]++;
          bench_dtx->GenTxDepositChecking(smallbank_client, &seed, yield, iter, dtx,is_partitioned, machine_id);
          // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kSendPayment: {
          thread_local_try_times[uint64_t(tx_type)]++;
          bench_dtx->GenTxSendPayment(smallbank_client, &seed, yield, iter, dtx,is_partitioned, machine_id);
          // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kTransactSaving: {
          thread_local_try_times[uint64_t(tx_type)]++;
          bench_dtx->GenTxTransactSaving(smallbank_client, &seed, yield, iter, dtx,is_partitioned, machine_id);
          // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kWriteCheck: {
          thread_local_try_times[uint64_t(tx_type)]++;
          bench_dtx->GenTxWriteCheck(smallbank_client, &seed, yield, iter, dtx,is_partitioned, machine_id);
          // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    /*************************** Generate batch and send begin ***************************/
    // 将事务插入batch
    if (batch->isFull()) {
      SendBatchToLocal(yield, coro_id, batch, machine_id);
      // todo: 把batch发送到远程
      SendBatchToRemote(yield, coro_id, batch, machine_id);
      batch_store->InsertBatch(batch);
      batch = new Batch(batch_store->GenerateBatchID());
    }
    batch->InsertTxn(bench_dtx);

    if (tx_id_generator > ATTEMPTED_NUM * (thread_num - 3)) {
      break;
    }
  }
}

void RunSubSmallBank(coro_yield_t& yield, coro_id_t coro_i, BenchDTX* dtx) {
// Each coroutine has a dtx: Each coroutine is a coordinator
  struct timespec tx_end_time;
  // Running transactions
  bool tx_committed = false;
  /********************************** Start execute *****************************************/
  SmallBankDTX* bench_dtx = (SmallBankDTX*)dtx;
  if (dtx->dtx->all_ids.find(machine_id) == dtx->dtx->all_ids.end()) {
    stat_attempted_tx_total++;
    dtx->stage = CalvinStages::FIN;
    tx_committed = true;
    dtx->dtx->TxCommit(yield);
    goto SubSmallBankStat;
  } 
  if (dtx->node_id == machine_id) stat_attempted_tx_total++;
  switch (bench_dtx->type) {
    case SmallBankTxType::kAmalgamate: {
        thread_local_try_times[uint64_t(bench_dtx->type)]++;
        tx_committed = bench_dtx->ReTxAmalgamate(yield);
        if (tx_committed) thread_local_commit_times[uint64_t(bench_dtx->type)]++;
      break;
    }
    case SmallBankTxType::kBalance: {
        thread_local_try_times[uint64_t(bench_dtx->type)]++;
        tx_committed = bench_dtx->ReTxBalance(yield);
        if (tx_committed) thread_local_commit_times[uint64_t(bench_dtx->type)]++;
      break;
    }
    case SmallBankTxType::kDepositChecking: {
        thread_local_try_times[uint64_t(bench_dtx->type)]++;
        tx_committed = bench_dtx->ReTxDepositChecking(yield);
        if (tx_committed) thread_local_commit_times[uint64_t(bench_dtx->type)]++;
      break;
    }
    case SmallBankTxType::kSendPayment: {
        thread_local_try_times[uint64_t(bench_dtx->type)]++;
        tx_committed = bench_dtx->ReTxSendPayment(yield);
        if (tx_committed) thread_local_commit_times[uint64_t(bench_dtx->type)]++;
      break;
    }
    case SmallBankTxType::kTransactSaving: {
        thread_local_try_times[uint64_t(bench_dtx->type)]++;
        tx_committed = bench_dtx->ReTxTransactSaving(yield);
        if (tx_committed) thread_local_commit_times[uint64_t(bench_dtx->type)]++;
      break;
    }
    case SmallBankTxType::kWriteCheck: {
        thread_local_try_times[uint64_t(bench_dtx->type)]++;
        tx_committed = bench_dtx->ReTxWriteCheck(yield);
        if (tx_committed) thread_local_commit_times[uint64_t(bench_dtx->type)]++;
      break;
    }
    default:
      printf("Unexpected transaction type %d\n", static_cast<int>(bench_dtx->type));
      abort();
  }
SubSmallBankStat:
  /********************************** Stat begin *****************************************/
  // Stat after one transaction finishes
  // printf("try %d transaction commit? %s\n", stat_attempted_tx_total, tx_committed?"true":"false");
  if (bench_dtx->node_id == machine_id) {
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - dtx->dtx->tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - dtx->dtx->tx_start_time.tv_sec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
    else{
      // printf("worker.cc: RunSmallBank, tx %ld not committed\n", iter);
    }
  }
  // delete dtx;
  /********************************** End execute *****************************************/
}

void RunTxn(coro_yield_t& yield, coro_id_t coro_id, BenchDTX* dtx) {
  assert(dtx->stage != CalvinStages::INIT && dtx->stage != CalvinStages::FIN);
  if (dtx->stage == CalvinStages::READ) {
    SmallBankDTX* bench_dtx = (SmallBankDTX*)dtx;
    CALVIN_LOG(INFO) << "DTX: " << dtx->bid << "-" << dtx->dtx->tx_id << " start to read.";
    assert(dtx->dtx->TxExe(yield));
    CALVIN_LOG(INFO) << "DTX: " << dtx->bid << "-" << dtx->dtx->tx_id << " send read set";
    if (dtx->TxNeedWait() && !dtx->dtx->wait_ids.empty()) 
      dtx->dtx->TxCalvinSendReadSet(yield);
    else {
      dtx->stage = CalvinStages::WRITE;
      CALVIN_LOG(INFO) << "DTX: " << dtx->bid << "-" << dtx->dtx->tx_id << " ready to enter write";
    }

    if (SYSTEM_MODE == 8) {
      dtx->dtx->InsertToCalvinStatus();
    }
  } 
  if (dtx->stage == CalvinStages::WRITE) {
    CALVIN_LOG(INFO) << "DTX: " << dtx->bid << "-" << dtx->dtx->tx_id << " start to write";
    RunSubSmallBank(yield, coro_id, dtx);
    dtx->stage = CalvinStages::FIN;
    CALVIN_LOG(INFO) << "DTX: " << dtx->bid << "-" << dtx->dtx->tx_id << " finish";
    // delete dtx;
  }
}

void RunMsg(coro_yield_t& yield, coro_id_t coro_id, rw_set_entry* entry) {
  BenchDTX* dtx = nullptr;
  // 如果不存在
  calvin_txn_mutex->lock();
  if(calvin_txn_status->find({entry->bid,entry->txn_id})==calvin_txn_status->end()){
    calvin_txn_mutex->unlock();
    queue->msg_enqueue(entry);
    return;
  } else{
    dtx = calvin_txn_status->at({entry->bid,entry->txn_id});
    calvin_txn_mutex->unlock();
  }

  // if (dtx->stage >= CalvinStages::READ) {
  //   // LOG(INFO) << "tx: " << dtx->dtx->tx_id << " has finished";
  //   return;
  // }
  assert(dtx->seed == entry->seed);
  assert(dtx->stage == CalvinStages::READ);
  for (auto item : entry->data_map) {
    int idx = item.first;
    char* data = new char[sizeof(DataItem)];
    memcpy(data, item.second, sizeof(DataItem));
    dtx->dtx->FillRemoteReadWriteSet(idx, data);
    // res_item->push_back(std::make_pair(idx,data));
    // res_item->at(response->node_id()) = std::make_pair(idx,data);
  }
  dtx->dtx->wait_ids.erase(entry->node_id);
  CALVIN_LOG(INFO) << "DTX: " << dtx->bid << "-" << dtx->dtx->tx_id << " get remote data from " << entry->node_id << " also needs to wait " << dtx->dtx->wait_ids.size();
  if (dtx->dtx->wait_ids.empty()) {
    dtx->stage = CalvinStages::WRITE;
    queue->enqueue(dtx, dtx->bid, false);
    CALVIN_LOG(INFO) << "DTX: " << dtx->bid << "-" << dtx->dtx->tx_id << " enqueue, ready to enter write";
  }
  // delete entry;
  return;
}

void RunWorker(coro_yield_t& yield, coro_id_t coro_id) {
  clock_gettime(CLOCK_REALTIME, &msr_start);
  bool hasRecordTpLat = false;
  while (true) {
    rw_set_entry* entry = queue->msg_dequeue(thread_local_id);
    if (entry) {
      RunMsg(yield, coro_id, entry);
    } 

    BenchDTX* dtx;
    dtx = queue->dequeue(thread_local_id);
    if (dtx && dtx->dtx) {
      RunTxn(yield, coro_id, dtx);
    }
    
    if (!hasRecordTpLat && stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      RecordTpLat(msr_sec,nullptr);
      complete_worker_cnt ++;
      if (complete_worker_cnt >= thread_num - 3 /*除去调度器、远程消息接收器和定序器*/) system_finish = true;
      hasRecordTpLat = true;
      // 如果当前线程不是第一个工作线程，就退出
      if (thread_local_id != 3) break;
    }
    // 如果当前线程是第一个工作线程，就继续执行，处理其他节点发来的事务
    if (system_finish && entry == nullptr && dtx == nullptr) {
      break;
    }
  }
  // sleep(5);
  LOG(INFO) << "Worker " << thread_local_id << " exits. remaing unrunning transactions count" << calvin_txn_status->size();
}

void RunTPCCPS(coro_yield_t& yield, coro_id_t coro_id) {
    // Each coroutine has a dtx: Each coroutine is a coordinator
    DTX* dtx = new DTX(meta_man,
                       thread_gid,
                       thread_local_id,
                       coro_id,
                       coro_sched,
                       index_cache,
                       page_cache,
                       compute_server,
                       data_channel,
                       log_channel,
                       remote_server_channel,
                       thread_txn_log);
    struct timespec tx_end_time;
    bool tx_committed = false;

    // Running transactions
    clock_gettime(CLOCK_REALTIME, &msr_start);
    assert(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 7 || SYSTEM_MODE == 12);
    
    while(compute_server->get_node()->get_phase() == Phase::BEGIN);
    while (true) {
      // 判断是否需要切换
      int cur_epoch = compute_server->get_node()->epoch;
      if(SYSTEM_MODE == 12 && compute_server->get_node()->get_phase() == Phase::GLOBAL && compute_server->get_node()->getNodeID() != 0){
        continue; // star
      }
      if(compute_server->get_node()->get_phase() == Phase::SWITCH_TO_GLOBAL || compute_server->get_node()->get_phase() == Phase::SWITCH_TO_PAR){
          // LOG(INFO) << "Node " << compute_server->get_node()->getNodeID() << " Thread " << thread_local_id << " is ready to switch";
          coro_sched->StopCoroutine(coro_id); // stop this coroutine
          // LOG(INFO) << "coro_id: " << coro_id << " is stopped";
          while(!coro_sched->isAllCoroStopped() && cur_epoch >= compute_server->get_node()->epoch){
            coro_sched->Yield(yield, coro_id);
            // LOG(INFO) << "coro_id: " << coro_id << " is yielded";
          }
          if(coro_sched->isAllCoroStopped()){
            compute_server->get_node()->threads_switch[thread_local_id] = true; // stop and ready to switch
            struct timespec now_time;
            clock_gettime(CLOCK_REALTIME, &now_time);
            auto cid = new brpc::CallId();
            dtx->SendLogToStoragePool(dtx->tx_id, cid);
            brpc::Join(*cid);
            just_group_commit = true;
            last_commit_log_ts = now_time;
            // LOG(INFO) << "group commit..." ;
            if (just_group_commit) {
              clock_gettime(CLOCK_REALTIME, &tx_end_time);
              for(auto r: *txn_request_infos){
                double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
                timer[stat_committed_tx_total++] = tx_usec;
              }
              txn_request_infos->clear();
              just_group_commit = false;
            }
          }
          while(cur_epoch >= compute_server->get_node()->epoch); // wait for switch
          // while(compute_server->get_node()->get_phase() == Phase::SWITCH_TO_GLOBAL || compute_server->get_node()->get_phase() == Phase::SWITCH_TO_PAR){
          //   // ! 注意， 此处代码块在多线程可能会卡住，如果epoch count设置过小导致此线程还未开始执行事务，又进入了切换阶段, 此时会导致线程卡住
          //   // ! 因为单节点没有global 阶段，所以更容易出现这种情况
          //     // std::this_thread::sleep_for(std::chrono::microseconds(1)); // sleep 5us
          // } // wait for switch
          if(coro_sched->isAllCoroStopped()){
            // LOG(INFO) << "coro_id: " << coro_id << " start all coroutines";
            for(coro_id_t coro_id=0; coro_id<coro_num; coro_id++){
              coro_sched->StartCoroutine(coro_id);
            }
          }
          continue;
      }
      // run transaction
      bool fill_txn = false;
      Txn_request_info txn_meta;
      bool is_par_txn;
      if(compute_server->get_node()->get_phase() == Phase::PARTITION || compute_server->get_node()->get_phase() == Phase::SWITCH_TO_GLOBAL){
        // 判断partitioned_txn_list是否为空, 如果为空则生成新的page id
        is_par_txn = true;
        std::unique_lock<std::mutex> l(compute_server->get_node()->getTxnQueueMutex());
        if(!compute_server->get_node()->get_partitioned_txn_queue().empty()){
          // 从partitioned_txn_list取出一个seed
          txn_meta = compute_server->get_node()->get_partitioned_txn_queue().front();
          compute_server->get_node()->get_partitioned_txn_queue().pop();
          fill_txn = true;
        }
        else {
          // 生成一个partitioned txn
          bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
          if(!is_partitioned){
            if(compute_server->get_node()->get_global_txn_queue().size() > (size_t)compute_server->get_node()->max_global_txn_queue) continue;
            // push back to global_txn_list
            txn_meta.seed = FastRand(&seed); // 暂存seed
            clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
            compute_server->get_node()->get_global_txn_queue().push(txn_meta);
          }
          else {
            txn_meta.seed = FastRand(&seed); // 暂存seed
            clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
            fill_txn = true;
          }
        }
      }
      else if(compute_server->get_node()->get_phase() == Phase::GLOBAL || compute_server->get_node()->get_phase() == Phase::SWITCH_TO_PAR){
        // 判断global_txn_list是否为空, 如果为空则生成新的page id
        is_par_txn = false;
        std::unique_lock<std::mutex> l(compute_server->get_node()->getTxnQueueMutex());
        if(!compute_server->get_node()->get_global_txn_queue().empty()){
          // 从global_txn_list取出一个seed
          txn_meta = compute_server->get_node()->get_global_txn_queue().front();
          compute_server->get_node()->get_global_txn_queue().pop();
          fill_txn = true;
        }
        else {
          // 生成一个global txn
          bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
          if(is_partitioned){
            if(compute_server->get_node()->get_partitioned_txn_queue().size() > (size_t)compute_server->get_node()->max_par_txn_queue) continue;
            // push back to partitioned_txn_list
            txn_meta.seed = FastRand(&seed); // 暂存seed
            clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
            compute_server->get_node()->get_partitioned_txn_queue().push(txn_meta);
          }
          else {
            txn_meta.seed = FastRand(&seed); // 暂存seed
            clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
            fill_txn = true;
          }
        }
      }
      else assert(false);

      for(int i = 0; i < 20; i++){
        FastRand(&seed); // 更新seed, 防止生成相同的seed, 为每个事务分配了5个seed
      }
      if(!fill_txn) continue;

      // Guarantee that each coroutine has a different seed
      TPCCTxType tx_type;
      if(is_par_txn) {
            tx_type = tpcc_workgen_arr[FastRand(&seed) % 100];
      } else {
            tx_type = tpcc_workgen_arr[100 - FastRand(&seed) % 88];
        }
      uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
      stat_attempted_tx_total++;

      if(is_par_txn) {
        compute_server->get_node()->partition_cnt++;
        compute_server->get_node()->stat_partition_cnt++;
        // ! 与原写法不同，暂时
//        if(compute_server->get_node()->partition_cnt >= EpochOptCount*(1-CrossNodeAccessRatio)){
//            // 通知切换线程
//            std::unique_lock<std::mutex> lck(compute_server->get_node()->phase_switch_mutex);
//            compute_server->get_node()->phase_switch_cv.notify_one();
//        }
      }
      else{
        compute_server->get_node()->global_cnt++;
        compute_server->get_node()->stat_global_cnt++;
        // ! 与原写法不同，暂时
//        if(compute_server->get_node()->global_cnt >= EpochOptCount*CrossNodeAccessRatio){
//            // 通知切换线程
//            std::unique_lock<std::mutex> lck(compute_server->get_node()->phase_switch_mutex);
//            compute_server->get_node()->phase_switch_cv.notify_one();
//        }
      }

      switch (tx_type) {
          case TPCCTxType::kDelivery: {
              thread_local_try_times[uint64_t(tx_type)]++;
              // RDMA_LOG(DBG) << "Tx[" << iter << "] [Delivery] thread id: " << thread_gid << " coro id: " << coro_id;
              tx_committed = TxDelivery(tpcc_client, random_generator, yield, iter, dtx);
              if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
              // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
          } break;
          case TPCCTxType::kNewOrder: {
              thread_local_try_times[uint64_t(tx_type)]++;
              // RDMA_LOG(DBG) << "Tx[" << iter << "] [NewOrder] thread id: " << thread_gid << " coro id: " << coro_id;
              tx_committed = TxNewOrder(tpcc_client, random_generator, yield, iter, dtx, is_par_txn);
              if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
              // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
          } break;
          case TPCCTxType::kOrderStatus: {
              thread_local_try_times[uint64_t(tx_type)]++;
              // RDMA_LOG(DBG) << "Tx[" << iter << "] [OrderStatus] thread id: " << thread_gid << " coro id: " << coro_id;
              tx_committed = TxOrderStatus(tpcc_client, random_generator, yield, iter, dtx);
              if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
              // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
          } break;
          case TPCCTxType::kPayment: {
              thread_local_try_times[uint64_t(tx_type)]++;
              // RDMA_LOG(DBG) << "Tx[" << iter << "] [Payment] thread id: " << thread_gid << " coro id: " << coro_id;
              tx_committed = TxPayment(tpcc_client, random_generator, yield, iter, dtx, is_par_txn);
              if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
              // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
          } break;
          case TPCCTxType::kStockLevel: {
              thread_local_try_times[uint64_t(tx_type)]++;
              // RDMA_LOG(DBG) << "Tx[" << iter << "] [StockLevel] thread id: " << thread_gid << " coro id: " << coro_id;
              tx_committed = TxStockLevel(tpcc_client, random_generator, yield, iter, dtx);
              if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
              // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
          } break;
          default:
              printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
              abort();
        }
        /********************************** Stat begin *****************************************/
        // Stat after one transaction finishes
      #if !GroupCommit
        if (tx_committed) {
          clock_gettime(CLOCK_REALTIME, &tx_end_time);
          double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
          timer[stat_committed_tx_total++] = tx_usec;
        }
      #else
        if(SYSTEM_MODE <=7 || SYSTEM_MODE == 12){
          if (tx_committed) {
            // 记录事务开始时间
            txn_request_infos->push_back(txn_meta);
            if(is_par_txn){
              compute_server->get_node()->stat_commit_partition_cnt++;
            } else{
              compute_server->get_node()->stat_commit_global_cnt++;
            }
          }
          // else{
          //   if(is_par_txn){
          //     compute_server->get_node()->partition_cnt--;
          //   } else{
          //     compute_server->get_node()->global_cnt--;
          //   }
          // }
          stat_enter_commit_tx_total++;
        }
        else assert(false);
      #endif
        if (stat_attempted_tx_total >= ATTEMPTED_NUM || stat_enter_commit_tx_total >= ATTEMPTED_NUM || (SYSTEM_MODE == 12 && compute_server->get_node()->getNodeID() != 0 && stat_enter_commit_tx_total >= ATTEMPTED_NUM * (1-CrossNodeAccessRatio))) {
      #if GroupCommit
        if(SYSTEM_MODE <=7 || SYSTEM_MODE == 12){
          struct timespec now_time;
          clock_gettime(CLOCK_REALTIME, &now_time);
          auto cid = new brpc::CallId();
            dtx->SendLogToStoragePool(dtx->tx_id, cid);
            brpc::Join(*cid);
            just_group_commit = true;
            last_commit_log_ts = now_time;
          clock_gettime(CLOCK_REALTIME, &tx_end_time);
            for(auto r: *txn_request_infos){
              double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
              timer[stat_committed_tx_total++] = tx_usec;
            }
          txn_request_infos->clear();
          just_group_commit = false;
        }
        else assert(false);
      #endif
          break;
        }
        /********************************** Stat end *****************************************/
    }
    coro_sched->FinishCorotine(coro_id);
    LOG(INFO) << "thread_local_id: " << thread_local_id << " coro_id: " << coro_id << " is stopped";
    while(coro_sched->isAllCoroStopped() == false) {
        // LOG(INFO) << coro_id << " *yield ";
        coro_sched->Yield(yield, coro_id);
    }
    if(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 7 || SYSTEM_MODE == 12)
        compute_server->get_node()->threads_finish[thread_local_id] = true;
    // A coroutine calculate the total execution time and exits
    clock_gettime(CLOCK_REALTIME, &msr_end);
    // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
    double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
    RecordTpLat(msr_sec,dtx);
    delete dtx;
}

void RunTPCC(coro_yield_t& yield, coro_id_t coro_id) {
    // Each coroutine has a dtx: Each coroutine is a coordinator
    DTX* dtx = new DTX(meta_man,
                       thread_gid,
                       thread_local_id,
                       coro_id,
                       coro_sched,
                       index_cache,
                       page_cache,
                       compute_server,
                       data_channel,
                       log_channel,
                       remote_server_channel,
                       thread_txn_log);
    struct timespec tx_end_time;
    bool tx_committed = false;

    // Running transactions
    clock_gettime(CLOCK_REALTIME, &msr_start);
    while (true) {
        // Guarantee that each coroutine has a different seed
        bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
        TPCCTxType tx_type;
        if(is_partitioned) {
            tx_type = tpcc_workgen_arr[FastRand(&seed) % 100];
        } else {
            tx_type = tpcc_workgen_arr[100 - FastRand(&seed) % 88];
        }
        uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
        stat_attempted_tx_total++;

        Txn_request_info txn_meta;
        clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);

        switch (tx_type) {
            case TPCCTxType::kDelivery: {
                thread_local_try_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "] [Delivery] thread id: " << thread_gid << " coro id: " << coro_id;
                tx_committed = TxDelivery(tpcc_client, random_generator, yield, iter, dtx);
                if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
            } break;
            case TPCCTxType::kNewOrder: {
                thread_local_try_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "] [NewOrder] thread id: " << thread_gid << " coro id: " << coro_id;
                tx_committed = TxNewOrder(tpcc_client, random_generator, yield, iter, dtx, is_partitioned);
                if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
            } break;
            case TPCCTxType::kOrderStatus: {
                thread_local_try_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "] [OrderStatus] thread id: " << thread_gid << " coro id: " << coro_id;
                tx_committed = TxOrderStatus(tpcc_client, random_generator, yield, iter, dtx);
                if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
            } break;
            case TPCCTxType::kPayment: {
                thread_local_try_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "] [Payment] thread id: " << thread_gid << " coro id: " << coro_id;
                tx_committed = TxPayment(tpcc_client, random_generator, yield, iter, dtx,is_partitioned);
                if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
            } break;
            case TPCCTxType::kStockLevel: {
                thread_local_try_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "] [StockLevel] thread id: " << thread_gid << " coro id: " << coro_id;
                tx_committed = TxStockLevel(tpcc_client, random_generator, yield, iter, dtx);
                if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
            } break;
            default:
                printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
                abort();
        }
        /********************************** Stat begin *****************************************/
        // Stat after one transaction finishes
      #if !GroupCommit
        if (tx_committed) {
          clock_gettime(CLOCK_REALTIME, &tx_end_time);
          double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
          timer[stat_committed_tx_total++] = tx_usec;
        }
      #else
        if(SYSTEM_MODE <=7 || SYSTEM_MODE == 10 || SYSTEM_MODE == 11){
          if (tx_committed) {
            // 记录事务开始时间
            txn_request_infos->push_back(txn_meta);
          }
          stat_enter_commit_tx_total++;
          struct timespec now_time;
          clock_gettime(CLOCK_REALTIME, &now_time);
          auto diff_ms = (now_time.tv_sec - last_commit_log_ts.tv_sec)* 1000LL + (double)(now_time.tv_nsec - last_commit_log_ts.tv_nsec) / 1000000;
          if(diff_ms >= EpochTime){
            auto cid = new brpc::CallId();
            dtx->SendLogToStoragePool(dtx->tx_id, cid);
            brpc::Join(*cid);
            just_group_commit = true;
            last_commit_log_ts = now_time;
            // LOG(INFO) << "group commit..." ;
          }
          if (just_group_commit) {
            clock_gettime(CLOCK_REALTIME, &tx_end_time);
            for(auto r: *txn_request_infos){
              double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
              timer[stat_committed_tx_total++] = tx_usec;
            }
            txn_request_infos->clear();
            just_group_commit = false;
          }
        }
        else if(SYSTEM_MODE == 9){
          if (tx_committed) {
            clock_gettime(CLOCK_REALTIME, &tx_end_time);
            double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
            timer[stat_committed_tx_total++] = tx_usec + dtx->two_latency_c * 1000;
          }
        }
      #endif
        if (stat_attempted_tx_total >= ATTEMPTED_NUM || stat_enter_commit_tx_total >= ATTEMPTED_NUM) {
      #if GroupCommit
        if(SYSTEM_MODE <=7 || SYSTEM_MODE == 10 || SYSTEM_MODE == 11){
          struct timespec now_time;
          clock_gettime(CLOCK_REALTIME, &now_time);
          auto cid = new brpc::CallId();
            dtx->SendLogToStoragePool(dtx->tx_id, cid);
            brpc::Join(*cid);
            just_group_commit = true;
            last_commit_log_ts = now_time;
          clock_gettime(CLOCK_REALTIME, &tx_end_time);
            for(auto r: *txn_request_infos){
              double tx_usec = (tx_end_time.tv_sec - r.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - r.start_time.tv_nsec) / 1000;
              timer[stat_committed_tx_total++] = tx_usec;
            }
          txn_request_infos->clear();
          just_group_commit = false;
        }
      #endif
          break;
        }
        /********************************** Stat end *****************************************/
        coro_sched->Yield(yield, coro_id);
    }
    coro_sched->FinishCorotine(coro_id);
    LOG(INFO) << "thread_local_id: " << thread_local_id << " coro_id: " << coro_id << " is stopped";
    while(coro_sched->isAllCoroStopped() == false) {
        // LOG(INFO) << coro_id << " *yield ";
        coro_sched->Yield(yield, coro_id);
    }
    // A coroutine calculate the total execution time and exits
    clock_gettime(CLOCK_REALTIME, &msr_end);
    // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
    double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
    RecordTpLat(msr_sec,dtx);
    delete dtx;
}

void run_thread(thread_params* params,
                SmallBank* smallbank_cli,
                TPCC* tpcc_cli) {
  bench_name = params->bench_name;
  std::string config_filepath = "../../config/" + bench_name + "_config.json";

  auto json_config = JsonConfig::load_file(config_filepath);
  auto conf = json_config.get(bench_name);
  ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();
  
  if (bench_name == "smallbank") { // SmallBank benchmark
    smallbank_client = smallbank_cli;
    smallbank_workgen_arr = smallbank_client->CreateWorkgenArray(READONLY_TXN_RATE);
    thread_local_try_times = new uint64_t[SmallBank_TX_TYPES]();
    thread_local_commit_times = new uint64_t[SmallBank_TX_TYPES]();
  } else if(bench_name == "tpcc") { // TPCC benchmark
    tpcc_client = tpcc_cli;
    tpcc_workgen_arr = tpcc_client->CreateWorkgenArray(READONLY_TXN_RATE);
    thread_local_try_times = new uint64_t[TPCC_TX_TYPES]();
    thread_local_commit_times = new uint64_t[TPCC_TX_TYPES]();
  } else {
    LOG(FATAL) << "Unsupported benchmark: " << bench_name;
  }

  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_id;
  thread_num = params->thread_num_per_machine;
  machine_id = params->machine_id;
  meta_man = params->global_meta_man;
  index_cache = params->index_cache;
  page_cache = params->page_cache;
  compute_server = params->compute_server;

  coro_num = (coro_id_t)params->coro_num;
  // Init coroutines
  if(SYSTEM_MODE != 6 && SYSTEM_MODE != 7) coro_num = 1;// 0-5只使用一个协程
  if(SYSTEM_MODE == 6 || SYSTEM_MODE == 7) {
    if (bench_name == "smallbank"){
      if(smallbank_client->num_hot_global <= 10000) {
        // coro_num = 16;
        delay_time = DelayFetchTime;
      }
      else {
        coro_num = 1;
        delay_time = 0;
      }
    }
    else{
      delay_time = DelayFetchTime;
    }
  }

#if SupportLongRunningTrans 
  if(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 7 || SYSTEM_MODE == 12){
    coro_sched_0 = new CoroutineScheduler(thread_gid, 1);
    using_which_coro_sched = new int(0);
    if (bench_name == "smallbank") {
      coro_sched_0->coro_array[0].func = coro_call_t(bind(RunSmallBankPSLong, _1, 0));
    } else if (bench_name == "tpcc") {
      coro_sched_0->coro_array[0].func = coro_call_t(bind(RunTPCCPS, _1, 0));
    } else {
      LOG(FATAL) << "Unsupported benchmark: " << bench_name;
    }
  }
#endif
  coro_sched = new CoroutineScheduler(thread_gid, coro_num);

  timer = new double[ATTEMPTED_NUM+50]();
  // Init queue
#if GroupCommit
  txn_request_infos = new std::vector<Txn_request_info>();
  thread_txn_log = new TxnLog();
#endif

  if (SYSTEM_MODE == 8) {
    batch_store = new BatchStore();
  }
  
  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;

  for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
    uint64_t coro_seed = static_cast<uint64_t>((static_cast<uint64_t>(thread_gid) << 32) | static_cast<uint64_t>(coro_i));
    random_generator[coro_i].SetSeed(coro_seed);
    coro_sched->coro_array[coro_i].coro_id = coro_i;
    // Bind workload to coroutine
    if (bench_name == "smallbank") {
      if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 4 || SYSTEM_MODE == 6 || SYSTEM_MODE == 9 || SYSTEM_MODE == 10 || SYSTEM_MODE == 11){
    #if SupportLongRunningTrans
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunSmallBankLong, _1, coro_i));
    #else
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunSmallBank, _1, coro_i));
    #endif
      }
      else if(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 7 || SYSTEM_MODE == 12){
    #if SupportLongRunningTrans 
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunSmallBankPSLong, _1, coro_i));
    #else
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunSmallBankPS, _1, coro_i));
    #endif
      } 
      else if (SYSTEM_MODE == 8) { // CALVIN
        if (thread_local_id == 0) {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(GenerateSmallBank, _1, coro_i));
        } else if (thread_local_id == 1) {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(GenerateSmmallBankFromRemote, _1, coro_i));
        } else if (thread_local_id == 2) {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(LockDTX, _1, coro_i, compute_server));
        } 
        else coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunWorker, _1, coro_i));
      }
    } else if (bench_name == "tpcc") {
      if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 4 || SYSTEM_MODE == 6 || SYSTEM_MODE == 9 || SYSTEM_MODE == 10 || SYSTEM_MODE == 11){
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTPCC, _1, coro_i));
      }
      else if(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 7 || SYSTEM_MODE == 12){
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTPCCPS, _1, coro_i));
      }
      // else if (SYSTEM_MODE == 8) { // CALVIN
      //   if (thread_local_id == 0) {
      //     coro_sched->coro_array[coro_i].func = coro_call_t(bind(GenerateTPCC, _1, coro_i));
      //   } else if (thread_local_id == 1) {
      //     coro_sched->coro_array[coro_i].func = coro_call_t(bind(LockDTX, _1, coro_i));
      //   }
      //   else coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunSubTPCC, _1, coro_i));
      // }
    } else {
      LOG(FATAL) << "Unsupported benchmark: " << bench_name;
    }
  }
  data_channel = new brpc::Channel();
  log_channel = new brpc::Channel();
  remote_server_channel = new brpc::Channel();

  // Init Brpc channel
  brpc::ChannelOptions options;
  // brpc::Channel channel;
  options.use_rdma = false;
  options.protocol = FLAGS_protocol;
  options.connection_type = FLAGS_connection_type;
  options.timeout_ms = FLAGS_timeout_ms;
  options.max_retry = FLAGS_max_retry;
  
  std::string storage_node = meta_man->remote_storage_nodes[0].ip + ":" + std::to_string(meta_man->remote_storage_nodes[0].port);
  if(data_channel->Init(storage_node.c_str(), &options) != 0) {
      LOG(FATAL) << "Fail to initialize channel";
  }
  if(log_channel->Init(storage_node.c_str(), &options) != 0) {
      LOG(FATAL) << "Fail to initialize channel";
  }
  std::string remote_server_node = meta_man->remote_server_nodes[0].ip + ":" + std::to_string(meta_man->remote_server_nodes[0].port);
  if(remote_server_channel->Init(remote_server_node.c_str(), &options) != 0) {
      LOG(FATAL) << "Fail to initialize channel";
  }
  
  // // Link all coroutines via pointers in a loop manner
  // coro_sched->LoopLinkCoroutine(coro_num);
#if SupportLongRunningTrans
  if(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 7 || SYSTEM_MODE == 12){
    coro_sched_0->StartCoroutine(0);
    coro_sched_0->coro_array[0].func();
    *using_which_coro_sched = 0;
  }
  else{
    for(coro_id_t coro_i = 0; coro_i < coro_num; coro_i++){
      coro_sched->StartCoroutine(coro_i); // Start all coroutines
    }
    // Start the first coroutine
    coro_sched->coro_array[0].func();
  }
#else
  for(coro_id_t coro_i = 0; coro_i < coro_num; coro_i++){
    coro_sched->StartCoroutine(coro_i); // Start all coroutines
  }
  // Start the first coroutine
  coro_sched->coro_array[0].func();
#endif

  // Wait for all coroutines to finish

  // Clean
  delete[] timer;
  if (smallbank_workgen_arr) delete[] smallbank_workgen_arr;
  if (random_generator) delete[] random_generator;
  delete coro_sched;
  delete thread_local_try_times;
  delete thread_local_commit_times;
}
