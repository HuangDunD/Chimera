// Author: Chunyue Huang
// Copyright (c) 2024

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>

#include "handler.h"
#include "compute_server/server.h"
#include "connection/meta_manager.h"
#include "cache/index_cache.h"
#include "util/json_config.h"
#include "worker.h"
#include "global.h"
#include "base/queue.h"

std::atomic<uint64_t> tx_id_generator;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> ab_rate;
std::vector<double> medianlat_vec;
std::vector<double> taillat_vec;
std::set<double> fetch_remote_vec;
std::set<double> fetch_all_vec;
std::set<double> lock_remote_vec;
std::vector<double> lock_durations;
std::vector<uint64_t> total_try_times;
std::vector<uint64_t> total_commit_times;
double all_time = 0;
double tx_begin_time = 0,tx_exe_time = 0,tx_commit_time = 0,tx_abort_time = 0,tx_update_time = 0;
double tx_get_timestamp_time1=0, tx_get_timestamp_time2=0, tx_write_commit_log_time=0, tx_write_prepare_log_time=0, tx_write_backup_log_time=0;
double tx_fetch_exe_time=0, tx_fetch_commit_time=0, tx_release_exe_time=0, tx_release_commit_time=0;
double tx_fetch_abort_time=0, tx_release_abort_time=0;
int single_txn =0, distribute_txn=0;

void Handler::ConfigureComputeNode(int argc, char* argv[]) {
  std::string config_file = "../../config/compute_node_config.json";
  std::string system_name = std::string(argv[2]);
  // ./run <benchmark_name> <system_name> <thread_num> <coroutine_num> <read_only_ratio> <local_transaction_ratio>
  if (argc == 7) {

    std::string s2 = "sed -i '5c \"thread_num_per_machine\": " + std::string(argv[3]) + ",' " + config_file;
    thread_num_per_node = std::stoi(argv[3]);
    std::string s3 = "sed -i '6c \"coroutine_num\": " + std::string(argv[4]) + ",' " + config_file;
    system(s2.c_str());
    system(s3.c_str());
    READONLY_TXN_RATE = std::stod(argv[5]);
    LOCAL_TRASACTION_RATE = std::stod(argv[6]);
    CrossNodeAccessRatio = 1 - LOCAL_TRASACTION_RATE;
  }

  // read compute node count
  auto json_config = JsonConfig::load_file(config_file);
  auto local_compute_node = json_config.get("local_compute_node");
  ComputeNodeCount = (int)local_compute_node.get("machine_num").get_int64();

  // Customized test without modifying configs
  int txn_system_value = 0;
  if (system_name.find("eager") != std::string::npos) {
    txn_system_value = 0;
  } else if (system_name.find("lazy") != std::string::npos) {
    txn_system_value = 1;
  } else if (system_name.find("chimeraA") != std::string::npos) {
    txn_system_value = 2;
  } else if (system_name.find("chimeraB") != std::string::npos) {
    txn_system_value = 3;
  } else if (system_name.find("chimeraC") != std::string::npos) {
    txn_system_value = 3;
  } else if (system_name.find("chimeraD") != std::string::npos) {
    txn_system_value = 5;
  } else if (system_name.find("chimeraE") != std::string::npos) {
    txn_system_value = 6;
  } else if (system_name.find("chimeraS") != std::string::npos) {
    txn_system_value = 7;
  } else if (system_name.find("calvin") != std::string::npos) {
    txn_system_value = 8;
  } else if (system_name.find("2pc") != std::string::npos) {
    txn_system_value = 9;
  }
  SYSTEM_MODE = txn_system_value;
  std::string s = "sed -i '7c \"txn_system\": " + std::to_string(txn_system_value) + ",' " + config_file;
  system(s.c_str());
  return;
}

void Handler::GenThreads(std::string bench_name) {
    if (bench_name == "smallbank") {
        WORKLOAD_MODE = 0;
    } else if(bench_name == "tpcc") {
        WORKLOAD_MODE = 1;
    } else {
        LOG(FATAL) << "Unsupported benchmark name: " << bench_name;
    }
  std::string config_filepath = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  node_id_t machine_num = (node_id_t)client_conf.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)client_conf.get("machine_id").get_int64();
  t_id_t thread_num_per_machine = (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
  const int coro_num = (int)client_conf.get("coroutine_num").get_int64();

  LOCAL_BATCH_TXN_SIZE = (int)client_conf.get("batch_size").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);

  /* Start working */
  tx_id_generator = 0;  // Initial transaction id == 0
  auto thread_arr = new std::thread[thread_num_per_machine];
  auto* index_cache = new IndexCache();
  auto* page_cache = new PageCache();
  auto* global_meta_man = new MetaManager(bench_name, index_cache, page_cache);
  auto* param_arr = new struct thread_params[thread_num_per_machine];

  // Create a compute node object
  std::string remote_server_ip = global_meta_man->remote_server_nodes[0].ip;
  int remote_server_port = global_meta_man->remote_server_nodes[0].port;
  std::string remote_storage_ip = global_meta_man->remote_storage_nodes[0].ip;
  int remote_storage_port = global_meta_man->remote_storage_nodes[0].port;

  auto* compute_node = new ComputeNode(machine_id, remote_server_ip, remote_server_port, global_meta_man);
  std::vector<std::string> compute_ips(machine_num);
  std::vector<int> compute_ports(machine_num);
  for (node_id_t i = 0; i < machine_num; i++) {
    compute_ips[i] = global_meta_man->remote_compute_nodes[i].ip;
    compute_ports[i] = global_meta_man->remote_compute_nodes[i].port;
  }
  auto* compute_server = new ComputeServer(compute_node, compute_ips, compute_ports);

  std::this_thread::sleep_for(std::chrono::seconds(3));  // Wait for 3s to ensure that the compute node server has started

  // Send TCP requests to remote servers here, and the remote server establishes a connection with the compute node
  socket_start_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);

  SmallBank* smallbank_client = nullptr;
  TPCC* tpcc_client = nullptr;

  if (bench_name == "smallbank") {
    smallbank_client = new SmallBank(nullptr);
    total_try_times.resize(SmallBank_TX_TYPES, 0);
    total_commit_times.resize(SmallBank_TX_TYPES, 0);
  } else if(bench_name == "tpcc") {
    tpcc_client = new TPCC(nullptr);
    total_try_times.resize(TPCC_TX_TYPES, 0);
    total_commit_times.resize(TPCC_TX_TYPES, 0);
  } else {
    LOG(FATAL) << "Unsupported benchmark name: " << bench_name;
  }

  // for calvin
  g_node_cnt = machine_num;
  queue = new QWorkQueue();
  // queue.init();
  lock_manager = new LockManager();
  calvin_txn_status = new std::unordered_map<std::pair<batch_id_t,tx_id_t>, BenchDTX*>();
  fin_calvin_txn_status = new std::unordered_map<std::pair<batch_id_t,tx_id_t>, BenchDTX*>();
  calvin_txn_mutex = new std::mutex();

  // waiting_calvin_txns = new std::unordered_set<BenchDTX*>();
  // wait_calvin_txn_mutex = new std::mutex();

  LOG(INFO) << "Spawn threads to execute...";
  t_id_t i = 0;
  for (; i < thread_num_per_machine; i++) { 
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].thread_id = i;
    param_arr[i].machine_id = machine_id;
    param_arr[i].coro_num = coro_num;
    param_arr[i].bench_name = bench_name;
    param_arr[i].index_cache = index_cache;
    param_arr[i].page_cache = page_cache;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].compute_server = compute_server;
    param_arr[i].thread_num_per_machine = thread_num_per_machine;
    param_arr[i].total_thread_num = thread_num_per_machine * machine_num;
    thread_arr[i] = std::thread(run_thread,
                                &param_arr[i],
                                smallbank_client,
                                tpcc_client);

    /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }

  std::thread switch_thread;
if(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 7){
  // 生成切换线程
  switch_thread = std::thread([compute_server](){
      // 修改线程名称
      prctl(PR_SET_NAME, ("switch_thd_n" + std::to_string(compute_server->get_node()->getNodeID())).c_str());
      compute_server->switch_phase();
  });
}

  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    if (thread_arr[i].joinable()) {
      thread_arr[i].join();
      std::cout << "thread " << i << " joined" << std::endl;
    }
  }
  LOG(INFO) << "All workers DONE, Waiting for all compute nodes to finish...";

  if(SYSTEM_MODE == 1){
    // 该线程结束, 释放持有的页锁
    compute_server->rpc_lazy_release_all_page_new();
  }

if(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 5 || SYSTEM_MODE == 7){
  compute_server->get_node()->setNodeRunning(false);
  // 通知切换线程
  while (!compute_server->get_node()->is_phase_switch_finish) {
      compute_server->get_node()->phase_switch_cv.notify_one();
  }
  switch_thread.join();
  LOG(INFO) << "phase-switch thread DONE, Waiting for all compute nodes to finish...";
}
  // Wait for all compute nodes to finish
  socket_finish_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);

  LOG(INFO) << "All compute nodes have finished";

  std::ofstream result_file("delay_fetch_remote.txt");
  if(SYSTEM_MODE == 6 || SYSTEM_MODE == 7) {
      LOG(INFO) << "delay_fetch_remote: " << compute_server->get_node()->getDelayFetchRemoteCnt(0);
      LOG(INFO) << "delay_fetch_ref: " << compute_server->get_node()->getDelayFetchRemoteRef(0);

      result_file << "delay_fetch_remote: " << compute_server->get_node()->getDelayFetchRemoteCnt(0) << std::endl;
      result_file << "delay_fetch_remote: " << compute_server->get_node()->getDelayFetchRemoteCnt(1) << std::endl;
      result_file << "delay_fetch_ref: " << compute_server->get_node()->getDelayFetchRemoteRef(0) << std::endl;
      result_file << "delay_fetch_ref: " << compute_server->get_node()->getDelayFetchRemoteRef(1) << std::endl;
      result_file << "delay_hit_ref: " << compute_server->get_node()->getDelayHitRef(0) << std::endl;
      result_file << "delay_hit_ref: " << compute_server->get_node()->getDelayHitRef(1) << std::endl;
      result_file << "rpc_hit_ref: " << compute_server->get_node()->getRPCHitRef(0) << std::endl;
      result_file << "rpc_hit_ref: " << compute_server->get_node()->getRPCHitRef(1) << std::endl;
      result_file << "hold_period_hit_ref: " << compute_server->get_node()->getHoldPrriodHitRef(0) << std::endl;
      result_file << "hold_period_hit_ref: " << compute_server->get_node()->getHoldPrriodHitRef(1) << std::endl;
  }
  if(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE ==7) {
      result_file << "partition: " << compute_server->get_node()->stat_partition_cnt << std::endl;
      result_file << "global: " << compute_server->get_node()->stat_global_cnt << std::endl;
      result_file << "partition commit: " << compute_server->get_node()->stat_commit_partition_cnt << std::endl;
      result_file << "global commit: " << compute_server->get_node()->stat_commit_global_cnt << std::endl;
  }
  result_file << "fetch_all: " << *fetch_all_vec.rbegin() << std::endl;
  result_file << "fetch_remote: " << *fetch_remote_vec.rbegin() << std::endl;
  result_file << "lock_remote: " << *lock_remote_vec.rbegin() << std::endl;
  delete[] param_arr;
  delete global_meta_man;
  if (smallbank_client) delete smallbank_client;
  if(tpcc_client) delete tpcc_client;
}
