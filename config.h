#pragma once
#include <gflags/gflags.h>

/*********************** For common **********************/
// Max data item size.
// 8: smallbank
// 40: tatp
// 664: tpcc
// 1024:ycsb
// 40: micro-benchmark

// ! pay attention: need modify this when use different workload
// Max data item size.
// 8: smallbank
// 40: tatp
// 664: tpcc
// 40: micro-benchmark
#define MAX_ITEM_SIZE 8

#define ComputeNodeBufferPageSize 262144 // 262144*4KB = 1GB
// #define ComputeNodeBufferPageSize 2621440 // for leap

#define BufferFusionSize ComputeNodeBufferPageSize
#define PartitionDataSize (ComputeNodeBufferPageSize / ComputeNodeCount)
#define MaxComputeNodeCount 128

// 定义算法版本 0:baseline, 1:lazy release, 2: phase switch-baseline 3: phase switch-lazy release 4: delay release 5: phase switch-delay release
// #define SYSTEM_MODE 1
extern int SYSTEM_MODE;

extern int LOCAL_BATCH_TXN_SIZE;

// 定义所跑的workload 0:smallbank 1:tpcc
extern int WORKLOAD_MODE;

extern bool use_rdma;
extern int ComputeNodeCount;
extern int thread_num_per_node;
extern double READONLY_TXN_RATE;
extern double LOCAL_TRASACTION_RATE;
extern uint64_t ATTEMPTED_NUM;
extern double CrossNodeAccessRatio;
extern int delay_time;
extern double LongTxnRate;

#define MaxPartitionCount 1024

// 定义计算节点的各个阶段
enum class Phase {PARTITION, GLOBAL, SWITCH_TO_PAR, SWITCH_TO_GLOBAL, BEGIN};
enum class OperationType {READ, WRITE};

// #define PartitionPhaseDuration 30000 // us, 1000us = 1ms
// #define GlobalPhaseDuration 10000 // us, 1000us = 1ms
#define EpochOptCount 1000 // 一个epoch中操作的次数
#define EpochTime 100
#define EarlyStopEpoch 1

#define HHH 6
#define KKK 5

#define RunOperationTime 500 // us, 1000us = 1ms

#define DelayReleaseTime 2000 // us, 1000us = 1ms

#define DelayFetchTime 5000

#define NetworkLatency 100 // us, 1000us = 1ms

#define BatchTimeStamp 200 

#define SupportLongRunningTrans true 
#define UniformHot true

#define LongTxnSize 10 // 长事务大小

#define GroupCommit true

#define WrongPrediction 0

#define SINGLE_MISS_CACHE_RATE 0.875 // 7/8

#define ATOM_FETCH_ADD(dest, value) __sync_fetch_and_add(&(dest), value)

enum lock_mode_type {NO_WAIT = 0, WAIT_DIE = 1 };
extern int LOCK_MODE;