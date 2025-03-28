#include "config.h"

int SYSTEM_MODE = 6;
int LOCAL_BATCH_TXN_SIZE = 100;
int WORKLOAD_MODE = 0;
int ComputeNodeCount = 8;
bool use_rdma = false;
int thread_num_per_node = 1;
double READONLY_TXN_RATE = 0.8;
double LOCAL_TRASACTION_RATE = 0.8;
uint64_t ATTEMPTED_NUM = 1000;
double CrossNodeAccessRatio = 0.1;
int LOCK_MODE = NO_WAIT;
int delay_time = 0;
double LongTxnRate = 0.10;