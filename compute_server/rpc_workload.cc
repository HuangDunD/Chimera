#include <random>
#include <chrono>
#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/prctl.h>
#include <brpc/rdma/rdma_helper.h>

#include "compute_server/server.h"

double ReadOperationRatio = 0.8;
int TryOperationCnt = 1000000;
double ConsecutiveAccessRatio = 0;
double HotPageRatio = 0.05;
double HotPageRange = 0.05;

int main(int argc, char* argv[]){

    // brpc::rdma::GlobalRdmaInitializeOrDie();

    // 0. Initialize the global variables
    if(argc == 8){
        ReadOperationRatio = atof(argv[1]);
        CrossNodeAccessRatio = atof(argv[2]);
        HotPageRatio = atof(argv[3]);
        HotPageRange = atof(argv[4]);
        ConsecutiveAccessRatio = atof(argv[5]);
        TryOperationCnt = atoi(argv[6]);
        ComputeNodeCount = atoi(argv[7]);
    }
    if(argc == 6){
        ReadOperationRatio = atof(argv[1]);
        CrossNodeAccessRatio = atof(argv[2]);
        ConsecutiveAccessRatio = atof(argv[3]);
        TryOperationCnt = atoi(argv[4]);
        ComputeNodeCount = atoi(argv[5]);
    }
    if(argc == 5){
        ReadOperationRatio = atof(argv[1]);
        CrossNodeAccessRatio = atof(argv[2]);
        TryOperationCnt = atoi(argv[3]);
        ComputeNodeCount = atoi(argv[4]);
    }

    if(argc == 4){
        ReadOperationRatio = atof(argv[1]);
        CrossNodeAccessRatio = atof(argv[2]);
        TryOperationCnt = atoi(argv[3]);
    }

    std::cout << "ComputeNodeCount: " << ComputeNodeCount << std::endl;

    // 3. Create ComputeNode object
    ComputeNode** compute_nodes = new ComputeNode*[ComputeNodeCount];
    for(int i=0; i<ComputeNodeCount; i++){
        compute_nodes[i] = new ComputeNode(i, "127.0.0.1", 33101);
    }

    // 4. Create compute server objects
    std::vector<std::string> compute_ips(ComputeNodeCount);
    std::vector<int> compute_ports(ComputeNodeCount);
    for(int i=0; i<ComputeNodeCount; i++){
        compute_ips[i] = "127.0.0.1";
        compute_ports[i] = 34002 + i;
    }
    ComputeServer** compute_servers = new ComputeServer*[ComputeNodeCount];
    for(int i=0; i<ComputeNodeCount; i++){
        compute_servers[i] = new ComputeServer(compute_nodes[i], compute_ips, compute_ports);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // 等待500ms，确保计算节点server已经启动

    // 这里向远程服务器发送TCP请求，远程服务器与计算节点建立连接
    socket_start_client("127.0.0.1", 33102);

    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // 等待500ms，确保内存节点已经和计算节点建立连接

    // 计时
    auto startTime = std::chrono::high_resolution_clock::now();

    // 4. Create a thread for each ComputeNode object
    std::thread* threads = new std::thread[ComputeNodeCount];
    for(int i=0; i<ComputeNodeCount; i++){
        threads[i] = std::thread([compute_servers, i]() { 
            std::string thread_name = "Node:" + std::to_string(i) + '\0';
            prctl(PR_SET_NAME, thread_name.c_str());
            if(SYSTEM_MODE == 0) {
                compute_servers[i]->rpc_run(TryOperationCnt / ComputeNodeCount);
            }
            else if(SYSTEM_MODE == 1) {
                compute_servers[i]->rpc_lazy_run(TryOperationCnt / ComputeNodeCount);
            }
            else if(SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 5) {
                compute_servers[i]->rpc_phase_switch_run(TryOperationCnt / ComputeNodeCount);
            }
            else if(SYSTEM_MODE == 4) {
                compute_servers[i]->rpc_delay_release_run(TryOperationCnt / ComputeNodeCount);
            }
            else if(SYSTEM_MODE == 6) {
                compute_servers[i]->rpc_delay_fetch_run(TryOperationCnt / ComputeNodeCount);
            }
        });
    }

    int fetch_remote_cnt = 0;
    int lock_request_cnt  = 0;
    int hit_delayed_release_lock_cnt = 0;
    std::vector<double> latency_vecs;
    // 5. Join all threads
    for(int i=0; i<ComputeNodeCount; i++){
        threads[i].join();
        fetch_remote_cnt += compute_nodes[i]->get_fetch_remote_cnt();
        lock_request_cnt += compute_nodes[i]->get_lock_remote_cnt();
        hit_delayed_release_lock_cnt += compute_nodes[i]->get_hit_delayed_release_lock_cnt();
        latency_vecs.insert(latency_vecs.end(), compute_nodes[i]->get_latency_vec().begin(), compute_nodes[i]->get_latency_vec().end());
    }

    // 计时
    auto endTime = std::chrono::high_resolution_clock::now();
    
    // 计算时间差
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime); // 毫秒
    double throughput = 1.0 * TryOperationCnt / duration.count();
    double fetch_remote_ratio = 1.0 * fetch_remote_cnt / TryOperationCnt;
    double lock_request_ratio = 1.0 * lock_request_cnt / TryOperationCnt;
    double hit_delayed_release_lock_ratio = 1.0 * hit_delayed_release_lock_cnt / TryOperationCnt;
    double sum_latency = 0;
    for(auto latency: latency_vecs){
        sum_latency += latency;
    }
    double avg_latency = sum_latency / latency_vecs.size();
    std::sort(latency_vecs.begin(), latency_vecs.end()); // 升序

    // std::vector<std::pair<Page_request_info, double>> latency_pair_vecs;
    // for(int i=0; i<ComputeNodeCount; i++){
    //     std::cout << "Node " << i << " latency: " << std::endl;
    //     latency_pair_vecs = compute_nodes[i]->get_latency_pair_vec();
    //     std::sort(latency_pair_vecs.begin(), latency_pair_vecs.end(), [](const std::pair<Page_request_info, double>& a, const std::pair<Page_request_info, double>& b){
    //         return a.second < b.second;
    //     });
    //     for(auto latency: latency_pair_vecs){
    //         std::cout << latency.first.page_id << " " << bool(latency.first.operation_type == OperationType::READ) << " " 
    //             << " " << latency.second << std::endl;
    //     }
    // }

    double p50_latency = latency_vecs[latency_vecs.size()/2];
    double p95_latency = latency_vecs[(int)(latency_vecs.size()*0.95)];

    // 输出时间
    usleep(1000); // sleep 1ms
    std::cout << "Time taken by function: " << duration.count() << " milliseconds" << std::endl;
    // 输出吞吐量，保留两位小数
    std::cout << "Throughput: " << throughput << " k operations per second" << std::endl;
    // 输出远程访问比例
    // std::cout << "Fetch remote cnt: " << fetch_remote_cnt << std::endl;
    // std::cout << "Remote Lock request cnt: " << lock_request_cnt << std::endl;
    std::cout << "Fetch remote ratio: " << fetch_remote_ratio << std::endl;
    std::cout << "Lock request ratio: " << lock_request_ratio << std::endl;
    std::cout << "Hit delayed release lock: " << hit_delayed_release_lock_ratio << std::endl;
    std::cout << "Average latency: " << avg_latency << " us" << std::endl;
    std::cout << "p50 latency: " << p50_latency << " us" << std::endl;
    std::cout << "P95 latency: " << p95_latency << " us" << std::endl;
    
    // 输出到result.txt, 如果没有则创建, 如果有则覆盖
    FILE* fp = fopen("result.txt", "w");
    fprintf(fp, "%ld\n", duration.count());
    fprintf(fp, "%f\n", throughput);
    fprintf(fp, "%f\n", fetch_remote_ratio);
    fprintf(fp, "%f\n", lock_request_ratio);
    fprintf(fp, "%f\n", hit_delayed_release_lock_ratio);
    fprintf(fp, "%f\n", avg_latency);
    fprintf(fp, "%f\n", p50_latency);
    fprintf(fp, "%f\n", p95_latency);
    return 0;
};