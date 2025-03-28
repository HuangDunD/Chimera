#include "server.h"
#include "remote_page_table/remote_partition_table_rpc.h"

std::mutex terminal_mutex;

// 在Partition Phase阶段执行对本计算节点分片的读写操作
// 在Global Phase阶段执行对全局数据(除了本地分片)的读写操作
Page* ComputeServer::rpc_phase_switch_fetch_s_page(page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    // assert(((node_->phase == Phase::SWITCH_TO_GLOBAL || node_->phase == Phase::PARTITION) && is_partitioned_page(page_id))
    //     || ((node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL) && !is_partitioned_page(page_id)));
    if(is_partitioned_page(page_id)) assert(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL);
    else assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL);

    Page* page = node_->local_buffer_pool->pages_ + page_id;
    if((node_->phase == Phase::PARTITION && is_partitioned_page(page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL){
        node_->partition_cnt++;
        if(node_->partition_cnt >= EpochOptCount*(1-CrossNodeAccessRatio) && node_->partitioned_page_queue.empty()){
            // 通知切换线程
            std::unique_lock<std::mutex> lck(node_->phase_switch_mutex);
            node_->phase_switch_cv.notify_one();
        }
        node_->local_page_lock_table->GetLock(page_id)->LockShared();
        // no need lock_remote
    }
    else{
        // Global Phase
        node_->global_cnt++;
        if(node_->global_cnt >= EpochOptCount*CrossNodeAccessRatio && node_->global_page_queue.empty()){
            // 通知切换线程
            std::unique_lock<std::mutex> lck(node_->phase_switch_mutex);
            node_->phase_switch_cv.notify_one();
        }
  if(SYSTEM_MODE == 2) {
      // use baseline
      bool lock_remote = node_->eager_local_page_lock_table->GetLock(page_id)->LockShared();
      if (lock_remote) {
          // Lock Remote
          // 再在远程加锁
          node_->lock_remote_cnt++;
          brpc::Controller cntl;
          page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
          page_table_service::PSLockRequest request;
          page_table_service::PSLockResponse *response = new page_table_service::PSLockResponse();
          page_table_service::PageID *page_id_pb = new page_table_service::PageID();
          page_id_pb->set_page_no(page_id);
          request.set_allocated_page_id(page_id_pb);
          request.set_node_id(node_->node_id);
          pagetable_stub.PSLock(&cntl, &request, response, NULL);
          if (cntl.Failed()) {
              LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
              exit(0);
          }
          node_id_t valid_node = response->newest_node();
          // 如果valid是false, 则需要去远程取这个数据页
          if (valid_node != -1) {
              assert(valid_node != node_->node_id);
              UpdatePageFromRemoteCompute(page, page_id, valid_node);
          }
          node_->eager_local_page_lock_table->GetLock(page_id)->LockRemoteOK();
          delete response;
      }
  }
  else if(SYSTEM_MODE == 3) {
      // 使用lazy release的模式进行加锁
      bool lock_remote = node_->lazy_local_page_lock_table->GetLock(page_id)->LockShared();
      if (lock_remote) {
          // 再在远程加锁
          node_->lock_remote_cnt++;
          brpc::Controller cntl;
          page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
          page_table_service::PSLockRequest request;
          page_table_service::PSLockResponse *response = new page_table_service::PSLockResponse();
          page_table_service::PageID *page_id_pb = new page_table_service::PageID();
          page_id_pb->set_page_no(page_id);
          request.set_allocated_page_id(page_id_pb);
          request.set_node_id(node_->node_id);

          pagetable_stub.LRPSLock(&cntl, &request, response, NULL);
          if (cntl.Failed()) {
              LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
              exit(0);
          }
          node_id_t valid_node = response->newest_node();
          // 如果valid是false, 则需要去远程取这个数据页
          if (valid_node != -1) {
              assert(valid_node != node_->node_id);
              UpdatePageFromRemoteCompute(page, page_id, valid_node);
          }
          //! lock remote ok and unlatch local
          node_->lazy_local_page_lock_table->GetLock(page_id)->LockRemoteOK(node_->node_id);
          // delete response;
          delete response;
      }
  }
  else if(SYSTEM_MODE == 5) {
      // 使用delay release的模式进行加锁
      bool lock_remote = node_->delay_local_page_lock_table->GetLock(page_id)->LockShared();
      // 再在远程加锁
      if (lock_remote) {
          node_->lock_remote_cnt++;
          page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
          page_table_service::PSLockRequest request;
          page_table_service::PSLockResponse *response = new page_table_service::PSLockResponse();
          page_table_service::PageID *page_id_pb = new page_table_service::PageID();
          page_id_pb->set_page_no(page_id);
          request.set_allocated_page_id(page_id_pb);
          request.set_node_id(node_->node_id);

          brpc::Controller cntl;
          pagetable_stub.PSLock(&cntl, &request, response, NULL);
          if (cntl.Failed()) {
              LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
          }
          node_id_t valid_node = response->newest_node();
          // 如果valid是false, 则需要去远程取这个数据页
          if (valid_node != -1) {
              assert(valid_node != node_->node_id);
              UpdatePageFromRemoteCompute(page, page_id, valid_node);
          }
          node_->delay_local_page_lock_table->GetLock(page_id)->LockRemoteOK();
          delete response;
      }
  }
    }
    return page;
}

Page* ComputeServer::rpc_phase_switch_fetch_x_page(page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    // assert(((node_->phase == Phase::SWITCH_TO_GLOBAL || node_->phase == Phase::PARTITION) && is_partitioned_page(page_id))
    //     || ((node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL) && !is_partitioned_page(page_id)));
    if(is_partitioned_page(page_id)) assert(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL);
    else assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL);
    
    Page* page = node_->local_buffer_pool->pages_ + page_id;
    if((node_->phase == Phase::PARTITION && is_partitioned_page(page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL){
        node_->partition_cnt++;
        if(node_->partition_cnt >= EpochOptCount*(1-CrossNodeAccessRatio) && node_->partitioned_page_queue.empty()){
            // 通知切换线程
            std::unique_lock<std::mutex> lck(node_->phase_switch_mutex);
            node_->phase_switch_cv.notify_one();
        }
        node_->local_page_lock_table->GetLock(page_id)->LockExclusive();
        // no need lock_remote
    }
    else{
        // Global Phase
        node_->global_cnt++;
        if(node_->global_cnt >= EpochOptCount*CrossNodeAccessRatio && node_->global_page_queue.empty()){
            // 通知切换线程
            std::unique_lock<std::mutex> lck(node_->phase_switch_mutex);
            node_->phase_switch_cv.notify_one();
        }
  if(SYSTEM_MODE == 2) {
      // use baseline
      bool lock_remote = node_->eager_local_page_lock_table->GetLock(page_id)->LockExclusive();
      if (lock_remote) {
          // Lock Remote
          // 再在远程加锁
          node_->lock_remote_cnt++;
          brpc::Controller cntl;
          page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
          page_table_service::PXLockRequest request;
          page_table_service::PXLockResponse *response = new page_table_service::PXLockResponse();
          page_table_service::PageID *page_id_pb = new page_table_service::PageID();
          page_id_pb->set_page_no(page_id);
          request.set_allocated_page_id(page_id_pb);
          request.set_node_id(node_->node_id);

          pagetable_stub.PXLock(&cntl, &request, response, NULL);
          if (cntl.Failed()) {
              LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
              exit(0);
          }
          node_id_t valid_node = response->newest_node();
          // 如果valid是false, 则需要去远程取这个数据页
          if (valid_node != -1) {
              assert(valid_node != node_->node_id);
              UpdatePageFromRemoteCompute(page, page_id, valid_node);
          }
          node_->eager_local_page_lock_table->GetLock(page_id)->LockRemoteOK();
          delete response;
      }
  }
  else if(SYSTEM_MODE == 3) {
      // 使用lazy release的模式进行加锁
      bool lock_remote = node_->lazy_local_page_lock_table->GetLock(page_id)->LockExclusive();
      if (lock_remote) {
          // 再在远程加锁
          node_->lock_remote_cnt++;
          brpc::Controller cntl;
          page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
          page_table_service::PXLockRequest request;
          page_table_service::PXLockResponse *response = new page_table_service::PXLockResponse();
          page_table_service::PageID *page_id_pb = new page_table_service::PageID();
          page_id_pb->set_page_no(page_id);
          request.set_allocated_page_id(page_id_pb);
          request.set_node_id(node_->node_id);

          pagetable_stub.LRPXLock(&cntl, &request, response, NULL);
          if (cntl.Failed()) {
              LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
              exit(0);
          }
          node_id_t valid_node = response->newest_node();
          // 如果valid是false, 则需要去远程取这个数据页
          if (valid_node != -1) {
              assert(valid_node != node_->node_id);
              UpdatePageFromRemoteCompute(page, page_id, valid_node);
          }
          //! lock remote ok and unlatch local
          node_->lazy_local_page_lock_table->GetLock(page_id)->LockRemoteOK(node_->node_id);
          // delete response;
          delete response;
      }
  }
  else if(SYSTEM_MODE == 5) {
      // 使用delay release的模式进行加锁
      bool lock_remote = node_->delay_local_page_lock_table->GetLock(page_id)->LockExclusive();
      // 再在远程加锁
      if (lock_remote) {
          node_->lock_remote_cnt++;
          page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
          page_table_service::PXLockRequest request;
          page_table_service::PXLockResponse *response = new page_table_service::PXLockResponse();
          page_table_service::PageID *page_id_pb = new page_table_service::PageID();
          page_id_pb->set_page_no(page_id);
          request.set_allocated_page_id(page_id_pb);
          request.set_node_id(node_->node_id);

          brpc::Controller cntl;
          pagetable_stub.PXLock(&cntl, &request, response, NULL);
          if (cntl.Failed()) {
              LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
          }
          node_id_t valid_node = response->newest_node();
          // 如果valid是false, 则需要去远程取这个数据页
          if (valid_node != -1) {
              assert(valid_node != node_->node_id);
              UpdatePageFromRemoteCompute(page, page_id, valid_node);
          }
          node_->delay_local_page_lock_table->GetLock(page_id)->LockRemoteOK();
          delete response;
      }
  }
    }
    return page;
}

void ComputeServer::rpc_phase_switch_release_s_page(page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    if(is_partitioned_page(page_id)) assert(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL);
    else assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL);
    
    if((node_->phase == Phase::PARTITION && is_partitioned_page(page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL){
        node_->local_page_lock_table->GetLock(page_id)->UnlockShared();
    }
    else{
        // Global Phase
if(SYSTEM_MODE == 2) {
    // use baseline
    bool unlock_remote = node_->eager_local_page_lock_table->GetLock(page_id)->UnlockShared();
    if (unlock_remote) {
        // UnLock Remote
        // 再在远程解锁
        brpc::Controller cntl;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PSUnlockRequest request;
        page_table_service::PSUnlockResponse *response = new page_table_service::PSUnlockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        pagetable_stub.PSUnlock(&cntl, &request, response, NULL);
        if (cntl.Failed()) {
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
            exit(0);
        }
        node_->eager_local_page_lock_table->GetLock(page_id)->UnlockRemoteOK();
        delete response;
    }
}
  else if(SYSTEM_MODE == 3) {
    int unlock_remote = node_->lazy_local_page_lock_table->GetLock(page_id)->UnlockShared();
    if (unlock_remote > 0) {
        // rpc release page
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PAnyUnLockRequest request;
        page_table_service::PAnyUnLockResponse *response = new page_table_service::PAnyUnLockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.LRPAnyUnLock(&cntl, &request, response, NULL);
        if (cntl.Failed()) {
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
        //! unlock remote ok and unlatch local
        node_->lazy_local_page_lock_table->GetLock(page_id)->UnlockRemoteOK();
        // delete response;
        delete response;
    }
}
  else if(SYSTEM_MODE == 5) {
    // 使用delay release的模式进行释放锁
    node_->delay_local_page_lock_table->GetLock(page_id)->UnlockShared();
}
    }
}

void ComputeServer::rpc_phase_switch_release_x_page(page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    if(is_partitioned_page(page_id)) assert(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL);
    else assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL);
    
    if((node_->phase == Phase::PARTITION && is_partitioned_page(page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL){
        node_->local_page_lock_table->GetLock(page_id)->UnlockExclusive();
    }
    else{
        // Global Phase
  if(SYSTEM_MODE == 2) {
      // use baseline
      bool unlock_remote = node_->eager_local_page_lock_table->GetLock(page_id)->UnlockExclusive();
      if (unlock_remote) {
          // UnLock Remote
          // 再在远程解锁
          brpc::Controller cntl;
          page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
          page_table_service::PXUnlockRequest request;
          page_table_service::PXUnlockResponse *response = new page_table_service::PXUnlockResponse();
          page_table_service::PageID *page_id_pb = new page_table_service::PageID();
          page_id_pb->set_page_no(page_id);
          request.set_allocated_page_id(page_id_pb);
          request.set_node_id(node_->node_id);

          pagetable_stub.PXUnlock(&cntl, &request, response, NULL);
          if (cntl.Failed()) {
              LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
              exit(0);
          }
          node_->eager_local_page_lock_table->GetLock(page_id)->UnlockRemoteOK();
          delete response;
      }
  }
  else if(SYSTEM_MODE == 3) {
      // release page
      int unlock_remote = node_->lazy_local_page_lock_table->GetLock(page_id)->UnlockExclusive();
      if (unlock_remote > 0) {
          assert(unlock_remote == 2);
          // 1. rpc release page
          page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
          page_table_service::PAnyUnLockRequest unlock_request;
          page_table_service::PAnyUnLockResponse *unlock_response = new page_table_service::PAnyUnLockResponse();
          page_table_service::PageID *page_id_pb3 = new page_table_service::PageID();
          page_id_pb3->set_page_no(page_id);
          unlock_request.set_allocated_page_id(page_id_pb3);
          unlock_request.set_node_id(node_->node_id);

          brpc::Controller cntl;
          pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
          if (cntl.Failed()) {
              LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
          }
          //! unlock remote ok and unlatch local
          node_->lazy_local_page_lock_table->GetLock(page_id)->UnlockRemoteOK();
          delete unlock_response;
      }
  }
  else if(SYSTEM_MODE == 5) {
      // 使用delay release的模式进行释放锁
      node_->delay_local_page_lock_table->GetLock(page_id)->UnlockExclusive();
  }
    }
}

Page* ComputeServer::rpc_phase_switch_fetch_s_page_new(table_id_t table_id, page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);

    // if(!is_partitioned_page_new(table_id, page_id) && table_id != 7) {// item表为只读表不需要进行判断
    //     if(node_->phase != Phase::SWITCH_TO_PAR && node_->phase != Phase::GLOBAL){
    //         LOG(ERROR) << "table_id: " << table_id << " page_id: " << page_id << " is not local page when fetch s page";
    //     }
    //     assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL); // 非本地逻辑分区只在Global Phase访问
    // }
    // if((node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL) && table_id != 7) assert(is_partitioned_page_new(table_id, page_id)); // Partition Phase和Switch to Global Phase只访问本地逻辑分区
    
    // 计数
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
#if UniformHot
    if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL || table_id == 7){
#else
    if((node_->phase == Phase::PARTITION && is_partitioned_page_new(table_id, page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL || table_id == 7){
#endif
        node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
        if(valid_node != -1){
            // 从远程更新数据页
            UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UpdateLocalOK();
        }
    }
    else{
        // Global Phase
        if(SYSTEM_MODE == 2) {
            // use baseline
            bool lock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
            if (lock_remote) {
                // Lock Remote
                // 再在远程加锁
                node_->lock_remote_cnt++;
                brpc::Controller cntl;
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PSLockRequest request;
                page_table_service::PSLockResponse *response = new page_table_service::PSLockResponse();
                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                request.set_allocated_page_id(page_id_pb);
                request.set_node_id(node_->node_id);
                pagetable_stub.PSLock(&cntl, &request, response, NULL);
                if (cntl.Failed()) {
                    LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
                    exit(0);
                }
                node_id_t valid_node = response->newest_node();
                // 如果valid是false, 则需要去远程取这个数据页
                if (valid_node != -1) {
                    assert(valid_node != node_->node_id);
                    UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
                }
                node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK();
                delete response;
            }
        }
        else if(SYSTEM_MODE == 3) {
            // 使用lazy release的模式进行加锁
            bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
            if (lock_remote) {
                // 再在远程加锁
                node_->lock_remote_cnt++;
                brpc::Controller cntl;
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PSLockRequest request;
                page_table_service::PSLockResponse *response = new page_table_service::PSLockResponse();
                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                request.set_allocated_page_id(page_id_pb);
                request.set_node_id(node_->node_id);

                pagetable_stub.LRPSLock(&cntl, &request, response, NULL);
                if (cntl.Failed()) {
                    LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
                    exit(0);
                }
                node_id_t valid_node;
                if(response->wait_lock_release()){
                    // 等待加锁成功
                    valid_node = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess();
                } else{
                    // 加锁已经成功
                    valid_node = response->newest_node();
                }
                // 如果valid是false, 则需要去远程取这个数据页
                if (valid_node != -1) {
                    assert(valid_node != node_->node_id);
                    UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
                }
                //! lock remote ok and unlatch local
                node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
                // delete response;
                delete response;
            }
        }
        else assert(false);
    }
    return page;
}


Page* ComputeServer::rpc_phase_switch_coro_fetch_s_page_new(CoroutineScheduler* coro_sched, coro_yield_t &yield, coro_id_t coro_id, table_id_t table_id, page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    assert(SYSTEM_MODE == 7);
    
    // if(!is_partitioned_page_new(table_id, page_id) && table_id != 7) {// item表为只读表不需要进行判断
    //     if(node_->phase != Phase::SWITCH_TO_PAR && node_->phase != Phase::GLOBAL){
    //         LOG(ERROR) << "table_id: " << table_id << " page_id: " << page_id << " is not local page when fetch s page";
    //     }
    //     assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL); // 非本地逻辑分区只在Global Phase访问
    // }
    // if((node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL) && table_id != 7) assert(is_partitioned_page_new(table_id, page_id)); // Partition Phase和Switch to Global Phase只访问本地逻辑分区
    
    // 计数
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
#if UniformHot
    if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL || table_id == 7){
#else
    if((node_->phase == Phase::PARTITION && is_partitioned_page_new(table_id, page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL || table_id == 7){
#endif
        node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
        if(valid_node != -1){
            // 从远程更新数据页
            UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UpdateLocalOK();
        }
    }
    else{
        // Global Phase
        // 使用delay fetch的模式进行加锁
        // bool delay = true;
        bool delay = ((table_id) <= 1);
        double hot_num = HHH;
        int page_num = node_->meta_manager_->GetMaxPageNumPerTable(table_id);
        if(page_id % ( page_num / ComputeNodeCount) > hot_num){
            delay = false;
        }
        LockMode lockmode = node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->LockShared(coro_sched, yield, coro_id, delay);
        // 再在远程加锁
        switch (lockmode)
        {
        case LockMode::SHARED:{
            // LOG(INFO) << "Lock SHARED " << table_id << " " << page_id;
            node_->lock_remote_cnt++;
            page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
            page_table_service::PSLockRequest request;
            page_table_service::PSLockResponse* response = new page_table_service::PSLockResponse();
            page_table_service::PageID *page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            request.set_allocated_page_id(page_id_pb);
            request.set_node_id(node_->node_id);

            brpc::Controller* cntl = new brpc::Controller();
            brpc::CallId call_id = cntl->call_id();
            std::atomic<bool>* finish = new std::atomic<bool>(false);
            // ! 注意, 如果这里写成同步, 当所有计算节点都要远程拿控制权时, 就会卡住, 因为不会切换到其他协程去释放锁
            // ! 因此这里需要异步, 等到远程拿到控制权后, 会调用LockRemoteOK()函数, 释放锁
            pagetable_stub.LRPSLock(cntl, &request, response, brpc::NewCallback(PSlockRPCDone, response, cntl, finish));
        
            while((*finish) == false){
                coro_sched->Yield(yield, coro_id);
            }
            brpc::Join(call_id);
            node_id_t valid_node;
            if(response->wait_lock_release()){
                // 等待加锁成功
                while(node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess(&valid_node)==false){
                    coro_sched->Yield(yield, coro_id);
                }
            } else{
                // 加锁已经成功
                valid_node = response->newest_node();
            }
            delete finish;
            delete response;

            // 如果valid是false, 则需要去远程取这个数据页
            if(valid_node != -1){
                assert(valid_node != node_->node_id);
                UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
            }
            node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
            node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->ReLockShared();
            // LOG(INFO) << "Lock SHARED " << table_id << " " << page_id << " OK";
            break;
        }
        case LockMode::EXCLUSIVE:{
            // LOG(INFO) << "*Lock EXCLUSIVE " << table_id << " " << page_id;
            node_->lock_remote_cnt++;
            page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
            page_table_service::PXLockRequest request;
            page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
            page_table_service::PageID *page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            request.set_allocated_page_id(page_id_pb);
            request.set_node_id(node_->node_id);

            brpc::Controller* cntl = new brpc::Controller();
            brpc::CallId call_id = cntl->call_id();
            std::atomic<bool>* finish = new std::atomic<bool>(false);
            // ! 注意, 如果这里写成同步, 当所有计算节点都要远程拿控制权时, 就会卡住, 因为不会切换到其他协程去释放锁
            // ! 因此这里需要异步, 等到远程拿到控制权后, 会调用LockRemoteOK()函数, 释放锁
            pagetable_stub.LRPXLock(cntl, &request, response, brpc::NewCallback(PXlockRPCDone, response, cntl, finish));

            while((*finish) == false){
                coro_sched->Yield(yield, coro_id);
            }
            brpc::Join(call_id);
            node_id_t valid_node;
            if(response->wait_lock_release()){
                // 等待加锁成功
                while(node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess(&valid_node)==false){
                    coro_sched->Yield(yield, coro_id);
                }
            } else{
                // 加锁已经成功
                valid_node = response->newest_node();
            }
            delete finish;
            delete response;

            // 如果valid是false, 则需要去远程取这个数据页
            if(valid_node != -1){
                assert(valid_node != node_->node_id);
                UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
            }
            node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
            node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->ReLockShared();
            break;
        }
        case LockMode::NONE:
            // get the page lock, do nothing
            break;
        default:
            assert(false);
            break;
        }
    }
    return page;
}

Page* ComputeServer::rpc_phase_switch_fetch_x_page_new(table_id_t table_id, page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);

    // if(!is_partitioned_page_new(table_id, page_id)) assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL); // 非本地逻辑分区只在Global Phase访问
    // if(!is_partitioned_page_new(table_id, page_id) && table_id != 7) {// item表为只读表不需要进行判断
    //     if(node_->phase != Phase::SWITCH_TO_PAR && node_->phase != Phase::GLOBAL){
    //         LOG(ERROR) << "table_id: " << table_id << " page_id: " << page_id << " is not local page when fetch x page";
    //     }
    //     assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL); // 非本地逻辑分区只在Global Phase访问
    // }
    // if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL) assert(is_partitioned_page_new(table_id, page_id)); // Partition Phase和Switch to Global Phase只访问本地逻辑分区
    
    // 计数
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
#if UniformHot
    if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL){
#else
    if((node_->phase == Phase::PARTITION && is_partitioned_page_new(table_id, page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL){
#endif
        node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
        if(valid_node != -1){
            // 从远程更新数据页
            UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UpdateLocalOK();
        }
    }
    else{
        // Global Phase
        if(SYSTEM_MODE == 2) {
            // use baseline
            bool lock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
            if (lock_remote) {
                // Lock Remote
                // 再在远程加锁
                node_->lock_remote_cnt++;
                brpc::Controller cntl;
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PXLockRequest request;
                page_table_service::PXLockResponse *response = new page_table_service::PXLockResponse();
                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                request.set_allocated_page_id(page_id_pb);
                request.set_node_id(node_->node_id);
                pagetable_stub.PXLock(&cntl, &request, response, NULL);
                if (cntl.Failed()) {
                    LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
                    exit(0);
                }
                node_id_t valid_node = response->newest_node();
                // 如果valid是false, 则需要去远程取这个数据页
                if (valid_node != -1) {
                    assert(valid_node != node_->node_id);
                    UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
                }
                node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK();
                delete response;
            }
        }
        else if(SYSTEM_MODE == 3) {
            // 使用lazy release的模式进行加锁
            bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
            if (lock_remote) {
                // 再在远程加锁
                node_->lock_remote_cnt++;
                brpc::Controller cntl;
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PXLockRequest request;
                page_table_service::PXLockResponse *response = new page_table_service::PXLockResponse();
                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                request.set_allocated_page_id(page_id_pb);
                request.set_node_id(node_->node_id);

                pagetable_stub.LRPXLock(&cntl, &request, response, NULL);
                if (cntl.Failed()) {
                    LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
                    exit(0);
                }
                node_id_t valid_node;
                if(response->wait_lock_release()){
                    // 等待加锁成功
                    valid_node = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess();
                } else{
                    // 加锁已经成功
                    valid_node = response->newest_node();
                }
                // 如果valid是false, 则需要去远程取这个数据页
                if (valid_node != -1) {
                    assert(valid_node != node_->node_id);
                    UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
                }
                //! lock remote ok and unlatch local
                node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
                // delete response;
                delete response;
            }
        }
        else assert(false);
    }
    return page;
}

Page* ComputeServer::rpc_phase_switch_coro_fetch_x_page_new(CoroutineScheduler* coro_sched, coro_yield_t &yield, coro_id_t coro_id, table_id_t table_id, page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    assert(SYSTEM_MODE == 7);

//    if(!is_partitioned_page_new(table_id, page_id)) assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL); // 非本地逻辑分区只在Global Phase访问
//    if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL) assert(is_partitioned_page_new(table_id, page_id)); // Partition Phase和Switch to Global Phase只访问本地逻辑分区
    
    // if(!is_partitioned_page_new(table_id, page_id)) assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL); // 非本地逻辑分区只在Global Phase访问
    // if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL) assert(is_partitioned_page_new(table_id, page_id)); // Partition Phase和Switch to Global Phase只访问本地逻辑分区
    // if(!is_partitioned_page_new(table_id, page_id) && table_id != 7) {// item表为只读表不需要进行判断
    //     if(node_->phase != Phase::SWITCH_TO_PAR && node_->phase != Phase::GLOBAL){
    //         LOG(ERROR) << "table_id: " << table_id << " page_id: " << page_id << " is not local page when fetch x page";
    //     }
    //     assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL); // 非本地逻辑分区只在Global Phase访问
    // }
    // 计数
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
#if UniformHot
    if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL){
#else
    if((node_->phase == Phase::PARTITION && is_partitioned_page_new(table_id, page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL){
#endif
        node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
        if(valid_node != -1){
            // 从远程更新数据页
            UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UpdateLocalOK();
        }
    }
    else{
        // 使用delay fetch的模式进行加锁
        // bool delay = true;
        bool delay = ((table_id) <= 1);
        double hot_num = HHH;
        int page_num = node_->meta_manager_->GetMaxPageNumPerTable(table_id);
        if(page_id % ( page_num / ComputeNodeCount) > hot_num){
            delay = false;
        }
        LockMode lockmode = node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive(coro_sched, yield, coro_id, delay);
        // 再在远程加锁
        switch (lockmode)
        {
        case LockMode::SHARED:{
            assert(false);
        }
        case LockMode::EXCLUSIVE:{
            // LOG(INFO) << "Lock Exclusive " << table_id << " " << page_id;
            node_->lock_remote_cnt++;
            page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
            page_table_service::PXLockRequest request;
            page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
            page_table_service::PageID *page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            request.set_allocated_page_id(page_id_pb);
            request.set_node_id(node_->node_id);

            brpc::Controller* cntl = new brpc::Controller();
            brpc::CallId call_id = cntl->call_id();
            std::atomic<bool>* finish = new std::atomic<bool>(false);
            // ! 注意, 如果这里写成同步, 当所有计算节点都要远程拿控制权时, 就会卡住, 因为不会切换到其他协程去释放锁
            // ! 因此这里需要异步, 等到远程拿到控制权后, 会调用LockRemoteOK()函数, 释放锁
            pagetable_stub.LRPXLock(cntl, &request, response, brpc::NewCallback(PXlockRPCDone, response, cntl, finish));

            while((*finish) == false){
                coro_sched->Yield(yield, coro_id);
            }
            brpc::Join(call_id);
            node_id_t valid_node;
            if(response->wait_lock_release()){
                // 等待加锁成功
                while(node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess(&valid_node)==false){
                    coro_sched->Yield(yield, coro_id);
                }
            } else{
                // 加锁已经成功
                valid_node = response->newest_node();
            }
            delete finish;
            delete response;

            // 如果valid是false, 则需要去远程取这个数据页
            if(valid_node != -1){
                assert(valid_node != node_->node_id);
                UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
            }
            node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
            node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->ReLockExclusive();
            // LOG(INFO) << "Lock Exclusive " << table_id << " " << page_id << " OK";
            break;
        }
        case LockMode::NONE:
            // get the page lock, do nothing
            break;
        default:
            assert(false);
            break;
        }
    }
    return page;
}

void ComputeServer::rpc_phase_switch_release_s_page_new(table_id_t table_id, page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    // if(!is_partitioned_page_new(table_id, page_id) && table_id != 7) assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL); // 非本地逻辑分区只在Global Phase访问
    // if((node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL) && table_id != 7) assert(is_partitioned_page_new(table_id, page_id)); // Partition Phase和Switch to Global Phase只访问本地逻辑分区
    // if(!is_partitioned_page_new(table_id, page_id) && table_id != 7) {// item表为只读表不需要进行判断
    //     if(node_->phase != Phase::SWITCH_TO_PAR && node_->phase != Phase::GLOBAL){
    //         LOG(ERROR) << "table_id: " << table_id << " page_id: " << page_id << " is not local page when release s page";
    //     }
    //     assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL); // 非本地逻辑分区只在Global Phase访问
    // }
#if UniformHot
    if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL || table_id == 7){
#else
    if((node_->phase == Phase::PARTITION && is_partitioned_page_new(table_id, page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL || table_id == 7){
#endif
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    }
    else{
        // Global Phase
        if(SYSTEM_MODE == 2) {
            // use baseline
            bool unlock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
            if (unlock_remote) {
                // UnLock Remote
                // 再在远程解锁
                brpc::Controller cntl;
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PSUnlockRequest request;
                page_table_service::PSUnlockResponse *response = new page_table_service::PSUnlockResponse();
                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                request.set_allocated_page_id(page_id_pb);
                request.set_node_id(node_->node_id);

                pagetable_stub.PSUnlock(&cntl, &request, response, NULL);
                if (cntl.Failed()) {
                    LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
                    exit(0);
                }
                node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
                delete response;
            }
        }
        else if(SYSTEM_MODE == 3) {
            // release page
            int unlock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
            if (unlock_remote > 0) {
                // rpc release page
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PAnyUnLockRequest request;
                page_table_service::PAnyUnLockResponse *response = new page_table_service::PAnyUnLockResponse();
                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                request.set_allocated_page_id(page_id_pb);
                request.set_node_id(node_->node_id);

                brpc::Controller cntl;
                pagetable_stub.LRPAnyUnLock(&cntl, &request, response, NULL);
                if (cntl.Failed()) {
                    LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
                }
                //! unlock remote ok and unlatch local
                node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
                // delete response;
                delete response;
            }
        }
        else if(SYSTEM_MODE == 7){
            // release page
            int unlock_remote = node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
            // LOG(INFO) << "Release Shared" << table_id << " " << page_id << " " << unlock_remote;
            if(unlock_remote > 0){
                // rpc release page
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PAnyUnLockRequest request;
                page_table_service::PAnyUnLockResponse* response = new page_table_service::PAnyUnLockResponse();
                page_table_service::PageID* page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                request.set_allocated_page_id(page_id_pb);
                request.set_node_id(node_->node_id);

                brpc::Controller cntl;
                pagetable_stub.LRPAnyUnLock(&cntl, &request, response, NULL);
                if(cntl.Failed()){
                    LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
                }
                //! unlock remote ok and unlatch local
                node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
                // delete response;
                delete response;
            }
        }
        else assert(false);
    }
}

void ComputeServer::rpc_phase_switch_release_x_page_new(table_id_t table_id, page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    // if(!is_partitioned_page_new(table_id, page_id)) assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL); // 非本地逻辑分区只在Global Phase访问
    // if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL) assert(is_partitioned_page_new(table_id, page_id)); // Partition Phase和Switch to Global Phase只访问本地逻辑分区
    // if(!is_partitioned_page_new(table_id, page_id) && table_id != 7) {// item表为只读表不需要进行判断
    //     if(node_->phase != Phase::SWITCH_TO_PAR && node_->phase != Phase::GLOBAL){
    //         LOG(ERROR) << "table_id: " << table_id << " page_id: " << page_id << " is not local page when release x page";
    //     }
    //     assert(node_->phase == Phase::SWITCH_TO_PAR || node_->phase == Phase::GLOBAL); // 非本地逻辑分区只在Global Phase访问
    // }
#if UniformHot
    if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL){
#else
    if((node_->phase == Phase::PARTITION && is_partitioned_page_new(table_id, page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL){
#endif
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    }
    else{
        // Global Phase
        if(SYSTEM_MODE == 2) {
            // use baseline
            bool unlock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
            if (unlock_remote) {
                // UnLock Remote
                // 再在远程解锁
                brpc::Controller cntl;
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PXUnlockRequest request;
                page_table_service::PXUnlockResponse *response = new page_table_service::PXUnlockResponse();
                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                request.set_allocated_page_id(page_id_pb);
                request.set_node_id(node_->node_id);

                pagetable_stub.PXUnlock(&cntl, &request, response, NULL);
                if (cntl.Failed()) {
                    LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
                    exit(0);
                }
                node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
                delete response;
            }
        }
        else if(SYSTEM_MODE == 3) {
            // release page
            int unlock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
            if (unlock_remote > 0) {
                assert(unlock_remote == 2);
                // 1. rpc release page
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PAnyUnLockRequest unlock_request;
                page_table_service::PAnyUnLockResponse *unlock_response = new page_table_service::PAnyUnLockResponse();
                page_table_service::PageID *page_id_pb3 = new page_table_service::PageID();
                page_id_pb3->set_page_no(page_id);
                page_id_pb3->set_table_id(table_id);
                unlock_request.set_allocated_page_id(page_id_pb3);
                unlock_request.set_node_id(node_->node_id);

                brpc::Controller cntl;
                pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
                if (cntl.Failed()) {
                    LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
                }
                //! unlock remote ok and unlatch local
                node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
                delete unlock_response;
            }
        }
        else if(SYSTEM_MODE == 7){
            // release page
            int unlock_remote = node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
            // LOG(INFO) << "Release Exclusive" << table_id << " " << page_id << " " << unlock_remote;
            if(unlock_remote > 0){
                // rpc release page
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PAnyUnLockRequest request;
                page_table_service::PAnyUnLockResponse* response = new page_table_service::PAnyUnLockResponse();
                page_table_service::PageID* page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                request.set_allocated_page_id(page_id_pb);
                request.set_node_id(node_->node_id);

                brpc::Controller cntl;
                pagetable_stub.LRPAnyUnLock(&cntl, &request, response, NULL);
                if(cntl.Failed()){
                    LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
                }
                //! unlock remote ok and unlatch local
                node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
                // delete response;
                delete response;
            }
        }
        else assert(false);
    }
}

void ComputeServer::switch_phase(){
    brpc::Controller cntl0;
    partition_table_service::SendCrossRatioRequest send_cross_request;
    partition_table_service::SendCrossRatioResponse send_cross_resqonse;
    send_cross_request.set_cross_ratio(CrossNodeAccessRatio);
    partition_table_service::PartitionTableService_Stub partable_stub0(get_pagetable_channel());
    partable_stub0.SendCorssRation(&cntl0, &send_cross_request, &send_cross_resqonse, NULL);
    if(cntl0.Failed()){
        assert(0);
    }
    
    if(CrossNodeAccessRatio == 1){
        // 特判: 不进入Partition Phase
        node_->phase = Phase::GLOBAL;
        if(node_->is_running){
            std::unique_lock<std::mutex> lck(node_->phase_switch_mutex);
            node_->phase_switch_cv.wait(lck, [&](){return !node_->is_running;});
            lck.unlock();
        }
        node_->is_phase_switch_finish = true;
        return;
    }
    while(node_->is_running){
        
        // ! 计时
        auto start = std::chrono::high_resolution_clock::now();

        // 1. 切换为Partition Phase
        // 1.1 远程对分片加X锁
        brpc::Controller cntl;
        partition_table_service::PartitionTableService_Stub partable_stub(get_pagetable_channel());
        partition_table_service::ParXLockRequest par_lock_request;
        partition_table_service::ParXLockResponse par_lock_response;
        partition_table_service::PartitionID* partition_id_pb = new partition_table_service::PartitionID();
        // node i lock partition i
        partition_id_pb->set_partition_no(node_->node_id);
        par_lock_request.set_allocated_partition_id(partition_id_pb);
        par_lock_request.set_node_id(node_->node_id);
        partable_stub.ParXLock(&cntl, &par_lock_request, &par_lock_response, NULL);

        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock partition " << node_->node_id << " in remote partition table";
        }
        
        node_->partition_ms = par_lock_response.partition_time();
        node_->global_ms = par_lock_response.global_time() * EarlyStopEpoch;
        LOG(INFO) << "Epoch: " << node_->epoch << " par_time: " << node_->partition_ms << " global_time: " << node_->global_ms; 
        
        // 1.1.5 获取分区所有数据的invalid pages
        if(ComputeNodeCount != 1 && SYSTEM_MODE != 12){
            rpc_phase_switch_get_invalid_pages_new();
        } else if(ComputeNodeCount != 1 && SYSTEM_MODE == 12){
            rpc_phase_switch_sync_invalid_pages_new(); // 由于star采用非对称的方式，因此这里同步所有页面以防止partitioned phase节点之间吞吐量显著差异
        }

        auto partition_phase_start = std::chrono::high_resolution_clock::now();

        // 1.2 设置node的phase为Partition
        // LOG(INFO) << "change phase to partition";
        node_->phase = Phase::PARTITION;
        node_->epoch++;
        
        // LOG(INFO) << "Node: " << node_->node_id << "Epoch " << epoch << " Partition Phase";
        // 1.3.(1) 持续远程分片锁一段时间, 
        // std::this_thread::sleep_for(std::chrono::microseconds(PartitionPhaseDuration));
        // 1.3.(2) 检测Partition阶段运行的事务数目是否达到了EpochOptCount*(1-CrossNodeAccessRatio)
        // while(node_->partition_cnt < EpochOptCount*(1-CrossNodeAccessRatio) && node_->is_running){
        //     // std::this_thread::sleep_for(std::chrono::microseconds(1)); // sleep 2us
        // }
        
        {
            // 使用条件变量cv检测到条件!(node_->partition_cnt < EpochOptCount*(1-CrossNodeAccessRatio) && node_->is_running) 时唤醒
            // if(node_->is_running){
            //     std::unique_lock<std::mutex> lck(node_->phase_switch_mutex);
            //     node_->phase_switch_cv.wait(lck, [&](){return node_->partition_cnt >= EpochOptCount*(1-CrossNodeAccessRatio) || !node_->is_running;});
            //     lck.unlock();
            // }
            auto run_partition_time = std::chrono::microseconds(node_->partition_ms);
            if(node_->is_running){
                std::unique_lock<std::mutex> lck(node_->phase_switch_mutex);
                node_->phase_switch_cv.wait_for(lck, run_partition_time, [&](){return !node_->is_running;});
                lck.unlock();
            }
        }

        // !计时
        auto switch_to_global = std::chrono::high_resolution_clock::now();

        // LOG(INFO) << "change phase to SWITCH_TO_GLOBAL";
        node_->phase = Phase::SWITCH_TO_GLOBAL;
        // !1.4 这里需要等到所有线程都完成了对分片的读写操作
        for(int i=0; i<thread_num_per_node; i++){
            while(node_->threads_switch[i] == false && node_->threads_finish[i] == false){}
            // LOG(INFO) << "Node: " << node_->node_id << " Thread: " << i << " finish switch";
            node_->threads_switch[i] = false;
        }
        LOG(INFO) << "Node: " << node_->node_id << " Epoch: " << node_->epoch << " Partition Phase exe txn: " << node_->partition_cnt;
        
        // !计时
        auto switch_to_global_wait_thread = std::chrono::high_resolution_clock::now();

        // !计算分区阶段吞吐量
        auto diff = std::chrono::duration_cast<std::chrono::microseconds>(switch_to_global_wait_thread - start).count();
        double epoch_par_tps;
        if(diff >= 0){
            epoch_par_tps = node_->partition_cnt * 10000 / diff * 100;
            LOG(INFO) << "Epoch: " << node_->epoch << "partition tps " << epoch_par_tps << "diff " << diff << " us";
            node_->partition_tps = ((node_->partition_tps) * ( (node_->epoch-1) / 2) + epoch_par_tps) / (node_->epoch / 2 + 1);
            assert(node_->partition_tps >= 0);
            LOG(INFO) << "Epoch: " << node_->epoch << "partition tps " << epoch_par_tps << " ave tps " << node_->partition_tps;
        }
        else{
            epoch_par_tps = node_->partition_tps;
        }


        node_->partition_cnt = 0;

        // 1.5 刷回partition phase所有修改过的数据页
        if(ComputeNodeCount != 1){
            rpc_phase_switch_invalid_pages_new();
        }

        // 1.6 释放远程分片锁
        partition_table_service::ParXUnlockRequest par_unlock_request;
        partition_table_service::ParXUnlockResponse par_unlock_response;
        partition_table_service::PartitionID* partition_id_pb2 = new partition_table_service::PartitionID();
        partition_id_pb2->set_partition_no(node_->node_id);
        par_unlock_request.set_allocated_partition_id(partition_id_pb2);
        par_unlock_request.set_node_id(node_->node_id);
        cntl.Reset();
        partable_stub.ParXUnlock(&cntl, &par_unlock_request, &par_unlock_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to unlock partition " << node_->node_id << " in remote partition table";
        }

        // !特例：如果只有一个计算节点，则不需要进行Global Phase
        // if(ComputeNodeCount == 1 || CrossNodeAccessRatio == 0) continue;

        // 2. 切换为Global Phase
        // 2.1 远程对全局数据加S锁
        // !强同步
        for(int i=0; i<ComputeNodeCount; i++){
            partition_table_service::ParSLockRequest global_lock_request;
            partition_table_service::ParSLockResponse global_lock_response;
            partition_table_service::PartitionID* partition_id_pb_s = new partition_table_service::PartitionID();
            partition_id_pb_s->set_partition_no(i);
            global_lock_request.set_allocated_partition_id(partition_id_pb_s);
            global_lock_request.set_node_id(node_->node_id);
            cntl.Reset();
            partable_stub.ParSLock(&cntl, &global_lock_request, &global_lock_response, NULL);
            if(cntl.Failed()){
                LOG(ERROR) << "Fail to lock partition " << i << " in remote partition table";
            }
        }

        // !计时
        auto swich_global_finish = std::chrono::high_resolution_clock::now();

        // 2.2 设置node的phase为Global
        // LOG(INFO) << "change phase to global";
        node_->phase = Phase::GLOBAL;
        node_->epoch++;
        // 2.3.(1) 持续远程全局锁一段时间
        // std::this_thread::sleep_for(std::chrono::microseconds(GlobalPhaseDuration));
        // 2.3.(2) 检测Global阶段运行的事务数目是否达到了EpochOptCount*CrossNodeAccessRatio
        // while(node_->global_cnt < EpochOptCount*CrossNodeAccessRatio && node_->is_running){
        //     // std::this_thread::sleep_for(std::chrono::microseconds(1)); // sleep 2us
        // }
        
        {
            // if(node_->is_running){
            //     // 使用条件变量cv检测到条件!(node_->global_cnt < EpochOptCount*CrossNodeAccessRatio && node_->is_running) 时唤醒
            //     std::unique_lock<std::mutex> lck2(node_->phase_switch_mutex);
            //     node_->phase_switch_cv.wait(lck2, [&](){return node_->global_cnt >= EpochOptCount*CrossNodeAccessRatio || !node_->is_running;});
            //     lck2.unlock();
            // }
            auto run_global_time = std::chrono::microseconds(node_->global_ms);
            if(node_->is_running){
                std::unique_lock<std::mutex> lck(node_->phase_switch_mutex);
                node_->phase_switch_cv.wait_for(lck, run_global_time, [&](){return !node_->is_running;});
                lck.unlock();
            }
        }

        // !计时
        auto switch_to_par = std::chrono::high_resolution_clock::now();
        
        // LOG(INFO) << "change phase to SWITCH_TO_PAR";
        node_->phase = Phase::SWITCH_TO_PAR;
        // !2.4 这里需要等到所有线程都完成了对全局数据的读写操作
        for(int i=0; i<thread_num_per_node; i++){
            while(node_->threads_switch[i] == false && node_->threads_finish[i] == false){}
            node_->threads_switch[i] = false;
        }
        LOG(INFO) << "Node: " << node_->node_id << " Epoch: " << node_->epoch << " Global Phase exe txn: " << node_->global_cnt;

        // !计时
        auto switch_to_par_wait_thread = std::chrono::high_resolution_clock::now();

        // ! lazy_release 模式下释放所有数据页的锁
        if(SYSTEM_MODE == 3)
            // rpc_lazy_release_all_page_async();
            // rpc_lazy_release_all_page_async_new();
            rpc_lazy_release_all_page_new();
        // ! delay_release 模式下释放所有数据页的锁
        else if(SYSTEM_MODE == 5) {
            // 等待delay release释放的锁全部释放
            {
                // 使用条件变量cv检测到条件 decided release lock 和 check release lock 队列都为空时唤醒
                std::unique_lock<std::mutex> lck3(node_->phase_switch_mutex);
                node_->phase_switch_cv.wait(lck3, [&]() {
                    return node_->delay_local_page_lock_table->delay_release_lock_list.empty()
                           && node_->delay_local_page_lock_table->decided_release_queue.empty();
                });
                lck3.unlock();
            }
        }
        // ! delay_fetch 模式下释放所有数据页的锁
        else if(SYSTEM_MODE == 7 && node_->is_running){
            // rpc_delay_fetch_all_page_async();
            // rpc_delay_fetch_release_all_page_async_new(); // !todo: need optimize
            rpc_delay_fetch_release_all_page_new();
        }

        // !计算全局阶段吞吐量
        auto diff1 = std::chrono::duration_cast<std::chrono::microseconds>(switch_to_par_wait_thread - swich_global_finish).count();
        double epoch_global_tps;
        if(diff1 >= 0){
            epoch_global_tps = node_->global_cnt * 10000 / diff1 * 100;
            node_->global_tps = ((node_->global_tps) * (node_->epoch / 2 -1) + epoch_global_tps) / (node_->epoch / 2);
            assert(node_->global_tps >= 0);
            LOG(INFO) << "Epoch: " << node_->epoch << "global tps " << epoch_global_tps << " ave tps " << node_->global_tps;
            node_->global_cnt = 0;
        }
        else{
            epoch_global_tps = node_->global_tps;
        }

        // 2.5 释放远程全局锁
        for(int i=0; i<ComputeNodeCount; i++){
            partition_table_service::ParSUnlockRequest global_unlock_request;
            partition_table_service::ParSUnlockResponse global_unlock_response;
            partition_table_service::PartitionID* partition_id_pb_s = new partition_table_service::PartitionID();
            partition_id_pb_s->set_partition_no(i);
            global_unlock_request.set_allocated_partition_id(partition_id_pb_s);
            global_unlock_request.set_node_id(node_->node_id);
            global_unlock_request.set_global_tps(epoch_global_tps);
            global_unlock_request.set_par_tps(epoch_par_tps);
            cntl.Reset();
            partable_stub.ParSUnlock(&cntl, &global_unlock_request, &global_unlock_response, NULL);
            if(cntl.Failed()){
                LOG(ERROR) << "Fail to unlock partition " << i << " in remote partition table";
            }
        }

        // 更新下一轮的执行时间
        // node_->partition_ms = (1 - CrossNodeAccessRatio) * node_->global_tps * EpochTime / (CrossNodeAccessRatio * node_->partition_tps + (1-CrossNodeAccessRatio) * node_->global_tps) ;
        // assert(node_->partition_ms > 0);
        // node_->global_ms = CrossNodeAccessRatio * node_->partition_tps  * EpochTime / (CrossNodeAccessRatio * node_->partition_tps + (1-CrossNodeAccessRatio) * node_->global_tps); 
        // assert(node_->global_ms > 0);
        // node_->max_par_txn_queue = node_->partition_ms /1000 * node_->partition_tps /10000;
        // node_->max_global_txn_queue = node_->global_ms /1000 * node_->global_tps /10000;
        node_->max_par_txn_queue = 0;
        node_->max_global_txn_queue = 0;
        // LOG(INFO) << "Epoch: " << node_->epoch + 1 << "partition_ms: "<< node_->partition_ms;
        // LOG(INFO) << "Epoch: " << node_->epoch + 2 << "global_ms: "<< node_->global_ms;

        LOG(INFO) << "Epoch: " << node_->epoch << "par queue remain: " << node_->partitioned_txn_queue.size();
        LOG(INFO) << "Epoch: " << node_->epoch << "global queue remain: " << node_->global_txn_queue.size();

        // !计时
        // std::lock_guard<std::mutex> lck(terminal_mutex);
        auto end = std::chrono::high_resolution_clock::now();
        // time1: 从开始到partition phase开始
        auto get_x_lock_time = std::chrono::duration_cast<std::chrono::microseconds>(partition_phase_start - start);
        // time2: partition phase时间
        auto partition_phase_time = std::chrono::duration_cast<std::chrono::microseconds>(switch_to_global - partition_phase_start);
        // time3: 线程停止时间
        auto wait_thread_time = std::chrono::duration_cast<std::chrono::microseconds>(switch_to_global_wait_thread - switch_to_global);
        // time4: 释放远程锁到global phase开始
        auto release_lock_to_global_time = std::chrono::duration_cast<std::chrono::microseconds>(swich_global_finish - switch_to_global_wait_thread);
        // time5: global phase时间
        auto global_phase_time = std::chrono::duration_cast<std::chrono::microseconds>(switch_to_par - swich_global_finish);
        // time6: 线程停止时间2
        auto wait_thread_time2 = std::chrono::duration_cast<std::chrono::microseconds>(switch_to_par_wait_thread - switch_to_par);
        // time7: 释放远程锁到partition phase开始, time7+time1=切换为partition phase的总时间
        auto release_lock_to_par_time = std::chrono::duration_cast<std::chrono::microseconds>(end - switch_to_par_wait_thread);
        // std::cout << "Node: " << node_->node_id << " partition phase time: " << partition_phase_time.count() << "us" << std::endl;
        // LOG(INFO) << "Node: " << node_->node_id << " get x lock time: " << get_x_lock_time.count() << "us";
        // LOG(INFO) << "Node: " << node_->node_id << " partition phase time: " << partition_phase_time.count() << "us";
        // LOG(INFO) << "Node: " << node_->node_id << " wait thread time: " << wait_thread_time.count() << "us";
        // LOG(INFO) << "Node: " << node_->node_id << " release lock to global time: " << release_lock_to_global_time.count() << "us";
        // LOG(INFO) << "Node: " << node_->node_id << " global phase time: " << global_phase_time.count() << "us";
        // LOG(INFO) << "Node: " << node_->node_id << " wait thread time2: " << wait_thread_time2.count() << "us";
        // LOG(INFO) << "Node: " << node_->node_id << " release lock to par time: " << release_lock_to_par_time.count() << "us";
        // std::cout << "get x lock time: " << get_x_lock_time.count() << "us" << std::endl;
        // std::cout << "wait thread time: " << wait_thread_time.count() << "us" << std::endl;
        // std::cout << "release lock to global time: " << release_lock_to_global_time.count() << "us" << std::endl;
        // std::cout << "global_phase_time: " << global_phase_time.count() << "us" << std::endl;
        // std::cout << "wait thread time2: " << wait_thread_time2.count() << "us" << std::endl;
        // std::cout << "release lock to par time: " << release_lock_to_par_time.count() << "us" << std::endl;
    }
    node_->is_phase_switch_finish = true;
    // send compute node finish
    brpc::Controller cntl;
    partition_table_service::SendFinishRequest compute_node_finish_request;
    partition_table_service::SendFinishResponse compute_node_finish_response;
    compute_node_finish_request.set_node_id(node_->node_id);
    partition_table_service::PartitionTableService_Stub partable_stub(get_pagetable_channel());
    partable_stub.SendFinish(&cntl, &compute_node_finish_request, &compute_node_finish_response, NULL);
    if(!cntl.Failed()){
        LOG(INFO) << "Send Node: " << node_->node_id << " finish";
    }
    return;
}