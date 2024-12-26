// Author: Chunyue Huang
// Copyright (c) 2024

#include "dtx/dtx.h"
#include "compute_server/worker/global.h"
#include "lock.h"
#include "base/queue.h"
#define PRINTF_LOCK_INFO 0
// 帮忙写一个根据PRINTF_LOCK_INFO，打印加锁信息的宏
#define LOCK_LOG(INFO) if(PRINTF_LOCK_INFO) LOG(INFO)

LockRC Lock::NoWaitGetSHLock(BenchDTX* dtx,DataSetItem * item) {
  LockRC rc = FAILED;
  if (lock_type_t != DLOCK_EX) {
    rc = SUCCESS;
    lock_type_t = DLOCK_SH;
    owners.push_back(LockEntry(DLOCK_SH,dtx,item));
    item->has_locked = lock_status::LOCK_SUCCESS;
    // lock_lock_total++;
  }
  return rc;
}
LockRC Lock::NoWaitGetEXLock(BenchDTX* dtx,DataSetItem * item) {
  LockRC rc = FAILED;
  if (lock_type_t == LOCK_NONE) {
    rc = SUCCESS;
    lock_type_t = DLOCK_EX;
    owners.push_back(LockEntry(DLOCK_EX,dtx,item));
    item->has_locked = lock_status::LOCK_SUCCESS;
    // lock_lock_total++;
  }
  return rc;
}
void Lock::NoWaitReleaseSHLock(BenchDTX* dtx,DataSetItem * item) {
  assert(lock_type_t == DLOCK_SH);
  auto it = find(owners.begin(), owners.end(), LockEntry(DLOCK_SH,dtx,item));
  if (it != owners.end()) {
    owners.erase(it);
  }
}
void Lock::NoWaitReleaseEXLock(BenchDTX* dtx,DataSetItem * item) {
  assert(lock_type_t == DLOCK_EX);
  assert(owners.size() == 1);
  lock_type_t = LOCK_NONE;
  owners.clear();
}

// 写一个专门用来打印加锁是否成功的函数，参数有事务dtx和数据项，输出事务id和锁的类型
void Lock::PrintLockSuccess(BenchDTX* dtx,DataSetItem * item) {
  // 把lock_type_t转化为字符串
  #if PRINTF_LOCK_INFO
  std::string lock_str;
  if (lock_type_t == DLOCK_SH) lock_str = "SH";
  else if (lock_type_t == DLOCK_EX) lock_str = "EX";
  else lock_str = "NONE";
  LOG(INFO) << "DTX: "<< dtx->dtx->tx_id << " Lock data: "<< item->item_ptr->table_id << ":" << item->item_ptr->key <<" Success, Lock Type: " << lock_str;
  #endif
}
// 再写一个加锁失败的打印函数
void Lock::PrintLockFailed(BenchDTX* dtx,DataSetItem * item,lock_type try_type) {
  // 把lock_type_t转化为字符串
  #if PRINTF_LOCK_INFO
  std::string lock_str;
  if (lock_type_t == DLOCK_SH) lock_str = "SH";
  else if (lock_type_t == DLOCK_EX) lock_str = "EX";
  else lock_str = "NONE";
  std::string try_str;
  if (try_type == DLOCK_SH) try_str = "SH";
  else if (try_type == DLOCK_EX) try_str = "EX";
  else try_str = "NONE";
  LOG(INFO) << "DTX: "<< dtx->bid << "-"<< dtx->dtx->tx_id << " try "<< try_str << " Lock data: "<< item->item_ptr->table_id << ":" << item->item_ptr->key <<" Failed, Lock Type: " << lock_str;
  #endif
}
// 写一个释放锁的函数，参数有事务dtx和数据项，输出事务id和锁的类型
void Lock::PrintLockRelease(BenchDTX* dtx,DataSetItem * item) {
  // 把lock_type_t转化为字符串
  #if PRINTF_LOCK_INFO
  std::string lock_str;
  if (lock_type_t == DLOCK_SH) lock_str = "SH";
  else if (lock_type_t == DLOCK_EX) lock_str = "EX";
  else lock_str = "NONE";
  LOG(INFO) << "DTX: "<< dtx->bid << "-"<< dtx->dtx->tx_id << " Release data: "<< item->item_ptr->table_id << ":" << item->item_ptr->key <<" Success, Lock Type: " << lock_str;
  #endif
}
// 写一个转交锁的函数，参数有事务dtx和数据项，输出事务id和锁的类型
void Lock::PrintLockHandOver(BenchDTX* src_dtx,BenchDTX* dst_dtx,DataSetItem * item) {
  // 把lock_type_t转化为字符串
  #if PRINTF_LOCK_INFO
  std::string lock_str;
  if (lock_type_t == DLOCK_SH) lock_str = "SH";
  else if (lock_type_t == DLOCK_EX) lock_str = "EX";
  else lock_str = "NONE";
  LOG(INFO) << "DTX: "<< src_dtx->bid << "-"<< src_dtx->dtx->tx_id << " Hand Over data: "<< item->item_ptr->table_id << ":" << item->item_ptr->key << " to DTX:" << dst_dtx->bid << "-"<< dst_dtx->dtx->tx_id << " Success, Lock Type: " << lock_str;
  #endif
}

bool Lock::IsConflict(lock_type l1, lock_type l2) {
  if (l1 == LOCK_NONE || l2 == LOCK_NONE)
      return false;
  else if (l1 == DLOCK_EX || l2 == DLOCK_EX)
      return true;
  else
      return false;
}

LockRC Lock::WaitGetSHLock(BenchDTX* dtx,DataSetItem * item) {
  LockRC rc = FAILED;
  bool conflict = IsConflict(lock_type_t,DLOCK_SH);
  if (SYSTEM_MODE == 8 && !waiters.empty()) {
    conflict = true;
  }

  if (!conflict) {
    // 修改锁的状态
    lock_type_t = DLOCK_SH;
    owners.push_back(LockEntry(DLOCK_SH,dtx,item));
    // 修改数据项的状态
    rc = SUCCESS;
    item->has_locked = lock_status::LOCK_SUCCESS;
    // 处理统计信息和打印
    // lock_lock_total++;
    PrintLockSuccess(dtx,item);
  } else {
    // 将事务加入等待队列
    waiters.push_back(LockEntry(DLOCK_SH,dtx,item));
    // 处理打印
    PrintLockFailed(dtx,item,DLOCK_SH);
  }
  return rc;
}
LockRC Lock::WaitGetEXLock(BenchDTX* dtx,DataSetItem * item) {
  LockRC rc = FAILED;
  bool conflict = IsConflict(lock_type_t,DLOCK_EX);
  if (SYSTEM_MODE == 8 && !waiters.empty()) {
    conflict = true;
  }
  if (!conflict) {
    // 修改锁的状态
    assert(owners.empty());
    lock_type_t = DLOCK_EX;
    owners.push_back(LockEntry(DLOCK_EX,dtx,item));
    // 修改数据项的状态
    rc = SUCCESS;
    item->has_locked = lock_status::LOCK_SUCCESS;
    // 处理统计信息和打印
    // lock_lock_total++;
    PrintLockSuccess(dtx,item);
  } else {
    // 将事务加入等待队列
    waiters.push_back(LockEntry(DLOCK_EX,dtx,item));
    // 处理打印
    PrintLockFailed(dtx,item,DLOCK_EX);
  }
  return rc;
}

void Lock::HandOverLock(BenchDTX* dtx) {
  while(!waiters.empty()) {
    LockEntry entry = waiters.front();
    if (IsConflict(lock_type_t,entry.type)) {
      break;
    }
    if (entry.type == DLOCK_EX) {
      LockRC rc = NoWaitGetEXLock(entry.dtx,entry.item);
      assert(rc == LockRC::SUCCESS);
      // 写一个锁被转交的log
      PrintLockHandOver(dtx,entry.dtx,entry.item);
    } else {
      LockRC rc = NoWaitGetSHLock(entry.dtx,entry.item);
      assert(rc == LockRC::SUCCESS);
      // 写一个锁被转交的log
      PrintLockHandOver(dtx,entry.dtx,entry.item);
    }
    // 通知事务
    if (entry.dtx->dtx->GetAllLock()) {
      if(__sync_bool_compare_and_swap(&entry.dtx->lock_ready,false,true)) {
        entry.dtx->stage = CalvinStages::READ;
        LOCK_LOG(INFO) << "DTX: "<< entry.dtx->bid << "-"<< entry.dtx->dtx->tx_id << " Get All Lock";
        CALVIN_LOG(INFO) << "DTX: "<< entry.dtx->bid << "-"<< entry.dtx->dtx->tx_id << " enqueue, ready to enter read";
        queue->enqueue(entry.dtx,entry.dtx->bid,true);
        // lock_handover_tx_total++;
        // wait_calvin_txn_mutex->lock();
        // waiting_calvin_txns->erase(entry.dtx);
        // wait_calvin_txn_mutex->unlock();
      }
    }
    waiters.pop_front();
  }
}

void Lock::WaitReleaseSHLock(BenchDTX* dtx,DataSetItem * item) {
  assert(lock_type_t == DLOCK_SH);
  // 从owners中找到这个事务，然后删除
  auto it = find(owners.begin(), owners.end(), LockEntry(DLOCK_SH,dtx,item));
  if (it != owners.end()) {
    owners.erase(it);
  } else {
    assert(false);
  }
  // lock_unlock_total++;
  if (owners.empty()) {
    lock_type_t = LOCK_NONE;
    // 写一个锁被释放的log
    PrintLockRelease(dtx,item);
    // 转交锁
    HandOverLock(dtx);
  }
}
void Lock::WaitReleaseEXLock(BenchDTX* dtx,DataSetItem * item) {
  assert(lock_type_t == DLOCK_EX);
  assert(owners.size() == 1);
  assert(owners.front() == LockEntry(DLOCK_EX,dtx,item));

  lock_type_t = LOCK_NONE;
  owners.clear();
  // lock_unlock_total++;
  // 写一个锁被释放的log
  PrintLockRelease(dtx,item);
  // 转交锁
  HandOverLock(dtx);
}



LockRC Lock::GetSHLock(BenchDTX* dtx,DataSetItem * item) {
  LockRC rc;
  pthread_mutex_lock( latch );
  if (item->lock_mode == NO_WAIT)
    rc = NoWaitGetSHLock(dtx,item);
  else
    rc =  WaitGetSHLock(dtx,item);
  
  pthread_mutex_unlock( latch );
  return rc;
}
LockRC Lock::GetEXLock(BenchDTX* dtx,DataSetItem * item) {
  LockRC rc;
  pthread_mutex_lock( latch );
  if (item->lock_mode == NO_WAIT)
    rc = NoWaitGetEXLock(dtx,item);
  else
    rc = WaitGetEXLock(dtx,item);
  pthread_mutex_unlock( latch );
  return rc;
}
void Lock::ReleaseSHLock(BenchDTX* dtx,DataSetItem * item) {
  pthread_mutex_lock( latch );
  if (item->lock_mode == NO_WAIT)
    NoWaitReleaseSHLock(dtx,item);
  else
    WaitReleaseSHLock(dtx,item);
  pthread_mutex_unlock( latch );
}
void Lock::ReleaseEXLock(BenchDTX* dtx,DataSetItem * item) {
  pthread_mutex_lock( latch );
  if (item->lock_mode == NO_WAIT)
    NoWaitReleaseEXLock(dtx,item);
  else
    WaitReleaseEXLock(dtx,item);
  
  pthread_mutex_unlock( latch );
}

// 帮忙实现LockManager的四个函数
LockRC LockManager::GetSHLock(BenchDTX* dtx,DataSetItem * item) {
  auto pair = std::make_pair(item->item_ptr->table_id,item->item_ptr->key);
  pthread_mutex_lock( latch );
  auto it = lock_map.find(pair);
  if (it == lock_map.end()) {
    lock_map[pair] = new Lock();
  }
  Lock *lock = lock_map[pair];
  pthread_mutex_unlock( latch );
  return lock_map[pair]->GetSHLock(dtx,item);
}
LockRC LockManager::GetEXLock(BenchDTX* dtx,DataSetItem * item) {
  auto pair = std::make_pair(item->item_ptr->table_id,item->item_ptr->key);
  pthread_mutex_lock( latch );
  auto it = lock_map.find(pair);
  if (it == lock_map.end()) {
    lock_map[pair] = new Lock();
  }
  Lock *lock = lock_map[pair];
  pthread_mutex_unlock( latch );
  return lock->GetEXLock(dtx,item);
}
void LockManager::ReleaseSHLock(BenchDTX* dtx,DataSetItem * item) {
  auto pair = std::make_pair(item->item_ptr->table_id,item->item_ptr->key);
  Lock *lock;
  pthread_mutex_lock( latch );
  auto it = lock_map.find(pair);
  if (it != lock_map.end()) {
    lock = lock_map[pair];
  } else {
    assert(false);
  }
  pthread_mutex_unlock( latch );
  lock->ReleaseSHLock(dtx,item);
}
void LockManager::ReleaseEXLock(BenchDTX* dtx,DataSetItem * item) {
  auto pair = std::make_pair(item->item_ptr->table_id,item->item_ptr->key);
  Lock *lock;
  pthread_mutex_lock( latch );
  auto it = lock_map.find(pair);
  if (it != lock_map.end()) {
    lock = lock_map[pair];
  } else {
    assert(false);
  }
  pthread_mutex_unlock( latch );
  lock->ReleaseEXLock(dtx,item);
}