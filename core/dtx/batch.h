// Author: hongyao zhao
// Copyright (c) 2024

#pragma once
// #include "worker/global.h"
#include "dtx.h" 
#include <unordered_map>

class Batch{
public:
	batch_id_t batch_id;
	pthread_mutex_t latch;
	BenchDTX** txn_list; // 事务列表

	int current_txn_cnt;    // 当前batch中已经绑定的事务数量
	// int start_commit_txn_cnt;   // 刚刚插入的事务数量
	int finish_commit_txn_cnt;  // 已经完成的事务的数量

	Batch() {
		current_txn_cnt = 0;
		finish_commit_txn_cnt = 0;
		batch_id = 0;
		txn_list = (BenchDTX**)malloc(sizeof(BenchDTX*) * LOCAL_BATCH_TXN_SIZE);
		pthread_mutex_init(&latch, nullptr);
	}
	void SetBatchID(batch_id_t id) { 
		batch_id = id;
	}

	Batch(batch_id_t id) {
		current_txn_cnt = 0;
		finish_commit_txn_cnt = 0;
		txn_list = (BenchDTX**)malloc(sizeof(BenchDTX*) * LOCAL_BATCH_TXN_SIZE);
		pthread_mutex_init(&latch, nullptr);
		SetBatchID(id);
	}

	bool InsertTxn(BenchDTX* txn) {
		if (current_txn_cnt < LOCAL_BATCH_TXN_SIZE) {
            txn->bid = batch_id;
			txn_list[current_txn_cnt] = txn;
			// printf("local_batch.h:64 insert SmallBankDTX dtx id %ld into %ld\n", txn->dtx->tx_id, current_txn_cnt);
			current_txn_cnt++;
			finish_commit_txn_cnt++;
			return true;
		} else {
			assert(false);
		}
	}

    void SetTxnFinish(int count) {
        finish_commit_txn_cnt += count;
    }

    bool isFull() {return current_txn_cnt >= LOCAL_BATCH_TXN_SIZE;}

	void Clean() {
		for (int i = 0; i < current_txn_cnt; i ++) {
			BenchDTX* dtx = txn_list[i];
			// printf("local_batch.h:86 free SmallBankDTX dtx id %ld\n", dtx->dtx->tx_id);
			delete dtx;
			
		}
		current_txn_cnt = 0;
		// start_commit_txn_cnt = 0;
		finish_commit_txn_cnt = 0;
		batch_id = 0;
	}
};

// 这里应该存储的是，已经开始执行的batch
class BatchStore{ 
private:
	inline int GetBatchCoroutineID(coro_id_t coro_id) {
		return coro_id - 1;
	}
public:  
	BatchStore(){
        batch_id_count = machine_id;
		current_batch_cnt = 0; // 这玩意暂时没用到
		pthread_mutex_init(&latch, nullptr);
	}

	batch_id_t GenerateBatchID() {
		batch_id_t id = ATOM_FETCH_ADD(batch_id_count,g_node_cnt);
		return id;
	}

	batch_id_t GetMaxBatchID() {
		return batch_id_count;
	}


	Batch* GetBatchByID(batch_id_t bid) {
		for (int i = 0; i < LOCAL_BATCH_TXN_SIZE; i++) {
			if (local_store[i]->batch_id == bid) return local_store[i];
		}
		return nullptr;
	}

    void InsertBatch(Batch* batch) {
        local_store.insert(std::make_pair(batch->batch_id,batch));
    }

    bool isFull() {return current_batch_cnt >= LOCAL_BATCH_TXN_SIZE;}
	

private:
	pthread_mutex_t latch;
	
public:
    std::unordered_map<batch_id_t, Batch*> local_store;
	int current_batch_cnt;
private:
	batch_id_t batch_id_count;
};
