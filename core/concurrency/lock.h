#pragma once
// #include "dtx/dtx.h"
#include "compute_server/worker/global.h"
#include <vector>
#include <list>
#include <queue>
#include <unordered_map>
#include <functional>

class BenchDTX;
class DataSetItem;

struct LockEntry {
  lock_type type;
  BenchDTX * dtx;
  DataSetItem * item;
  LockEntry(lock_type t, BenchDTX* d, DataSetItem * i): type(t), dtx(d), item(i) {}

  bool operator==(const LockEntry& other) const {
    // 这里根据你的具体需求定义相等条件
    return this->dtx == other.dtx && this->item == other.item;
  }
};

class Lock {
public:
  Lock() {
    latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);
    lock_type_t = LOCK_NONE;
  }
private:
  LockRC NoWaitGetSHLock(BenchDTX* dtx,DataSetItem* item);
  LockRC NoWaitGetEXLock(BenchDTX* dtx,DataSetItem* item);
  void NoWaitReleaseSHLock(BenchDTX* dtx,DataSetItem * item);
  void NoWaitReleaseEXLock(BenchDTX* dtx,DataSetItem * item);

  LockRC WaitGetSHLock(BenchDTX* dtx,DataSetItem* item);
  LockRC WaitGetEXLock(BenchDTX* dtx,DataSetItem* item);
  void HandOverLock(BenchDTX* dtx);
  void WaitReleaseSHLock(BenchDTX* dtx,DataSetItem * item);
  void WaitReleaseEXLock(BenchDTX* dtx,DataSetItem * item);

  bool IsConflict(lock_type l1, lock_type l2);

  void PrintLockSuccess(BenchDTX* dtx,DataSetItem * item);
  void PrintLockFailed(BenchDTX* dtx,DataSetItem * item,lock_type try_type);
  void PrintLockRelease(BenchDTX* dtx,DataSetItem * item);
  // 转交锁打印的函数
  void PrintLockHandOver(BenchDTX* src_dtx,BenchDTX* dst_dtx,DataSetItem * item);

public:
  LockRC GetSHLock(BenchDTX* dtx,DataSetItem* item);
  LockRC GetEXLock(BenchDTX* dtx,DataSetItem* item);
  void ReleaseSHLock(BenchDTX* dtx,DataSetItem * item);
  void ReleaseEXLock(BenchDTX* dtx,DataSetItem * item);

  pthread_mutex_t * latch;

  lock_type lock_type_t;
  std::list<LockEntry> owners;
	std::list<LockEntry> waiters;
};

// 写一个全局的锁管理器，管理所有数据项的锁
struct pair_hash {
  template <class T1, class T2>
  std::size_t operator() (const std::pair<T1, T2> &pair) const {
    return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
  }
};

// 写一个pair的equal函数
struct pair_equal {
  template <class T1, class T2>
  bool operator() (const std::pair<T1, T2> &lhs, const std::pair<T1, T2> &rhs) const {
    return lhs.first == rhs.first && lhs.second == rhs.second;
  }
};

class LockManager {
public:
  LockManager() {
    latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);
  }
  LockRC GetSHLock(BenchDTX* dtx,DataSetItem* item);
  LockRC GetEXLock(BenchDTX* dtx,DataSetItem* item);
  void ReleaseSHLock(BenchDTX* dtx,DataSetItem * item);
  void ReleaseEXLock(BenchDTX* dtx,DataSetItem * item);
private:
  // 改成用unordered_map的方式
  std::unordered_map<std::pair<table_id_t, itemkey_t>, Lock*, pair_hash, pair_equal> lock_map;
  pthread_mutex_t * latch;
};
