#include "server.h"

// maybe unused
void ComputeServer::PSUnlockRPCDone(page_table_service::PSUnlockResponse* response, brpc::Controller* cntl, page_id_t page_id) {
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<page_table_service::PSUnlockResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "PSUnlockRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        auto page_lock = node_->delay_local_page_lock_table->GetLock(page_id);
        page_lock->is_decided_release = false;
        page_lock->remote_mode = LockMode::NONE;
        page_lock->mutex.unlock();
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::PXUnlockRPCDone(page_table_service::PXUnlockResponse* response, brpc::Controller* cntl, page_id_t page_id) {
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<page_table_service::PXUnlockResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "PXUnlockRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        auto page_lock = node_->delay_local_page_lock_table->GetLock(page_id);
        page_lock->is_decided_release = false;
        page_lock->remote_mode = LockMode::NONE;
        page_lock->mutex.unlock();
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

// 持续释放decided_release_queue的锁，这个队列是已经决定释放的锁
void ComputeServer::release_decided_lock(){
    auto p = node_->delay_local_page_lock_table;
    // 线程退出顺序：
    // 1. 工作线程完成, node_->is_running = false
    // 2. check_if_release_lock线程完成, check_delay_release_finish = true
    // 3. release_decided_lock线程完成
    while(node_->is_running || !node_->check_delay_release_finish || !p->decided_release_queue.empty()){
        std::unique_lock<std::mutex> l(p->decided_release_queue_mutex);
        if(p->decided_release_queue.empty()){
            p->decided_release_queue_cv.wait(l, [p, this] { 
                return !p->decided_release_queue.empty() || !node_->is_running;
            });
            if(!node_->is_running) continue;
        }
        // 获取decided release page_id
        page_id_t page_id = p->decided_release_queue.front();
        DYLocalPageLock* page_lock = p->page_table[page_id];
        page_lock->mutex.lock();
        assert(page_lock->is_decided_release == true);
        assert(page_lock->is_granting == false);
        assert(page_lock->remote_mode == LockMode::SHARED || page_lock->remote_mode == LockMode::EXCLUSIVE);
        assert(page_lock->lock == 0);

        // 释放锁
        page_table_service::PageTableService_Stub page_table_stub(&node_->page_table_channel);
        brpc::Controller cntl;
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        if(page_lock->remote_mode == LockMode::SHARED){
            page_table_service::PSUnlockRequest request;
            page_table_service::PSUnlockResponse* response = new page_table_service::PSUnlockResponse();
            request.set_allocated_page_id(page_id_pb);
            request.set_node_id(node_->node_id);
            page_table_stub.PSUnlock(&cntl, &request, response, nullptr);
        }
        else if(page_lock->remote_mode == LockMode::EXCLUSIVE){
            page_table_service::PXUnlockRequest request;
            page_table_service::PXUnlockResponse* response = new page_table_service::PXUnlockResponse();
            request.set_allocated_page_id(page_id_pb);
            request.set_node_id(node_->node_id);
            page_table_stub.PXUnlock(&cntl, &request, response, nullptr);
        }
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
        page_lock->is_decided_release = false;
        page_lock->remote_mode = LockMode::NONE;
        page_lock->mutex.unlock();
        // 释放完成
        p->decided_release_queue.pop();
  if(SYSTEM_MODE == 5) {
      // 在Global阶段结束之后, 如果是SWITCH_TO_PAR阶段, 如果decided_release_queue为空, 则通知switch线程切换为PAR
      if (node_->phase == Phase::SWITCH_TO_PAR) {
          if (p->decided_release_queue.empty() && p->delay_release_lock_list.empty()) {
              // std::unique_lock<std::mutex> l(node_->phase_switch_mutex);
              node_->phase_switch_cv.notify_one();
          }
      }
  }
    }
    node_->release_delay_lock_finish = true;
    return;
}

// 持续检查delay_release_lock_list的锁是否到期，到期则加入decided_release_queue
// 注意：这个列表是可以反悔的，即可能有线程在期限内请求了这个锁，就会从这个列表中移除
void ComputeServer::check_if_release_lock(){
    auto p = node_->delay_local_page_lock_table;
    while(node_->is_running || !p->delay_release_lock_list.empty()){
        std::unique_lock<std::mutex> l(p->delay_release_list_mutex);
        if(p->delay_release_lock_list.empty()){
            p->delay_release_list_cv.wait(l, [p, this] { 
                return !(p->delay_release_lock_list.empty()) || !node_->is_running;
            });
            if(!node_->is_running) continue;
        }
        // delay_release_list_mutex is locked now
        DYLocalPageLock* page_lock = *p->delay_release_lock_list.begin();
        std::unique_lock<std::mutex> l1(page_lock->mutex, std::try_to_lock);  // 加锁, 判断时间是否到期
        if(!l1.owns_lock()){
            // 未获取到锁，说明有线程正在请求这个锁，因此需要重新检查
            continue;
        }
        if(page_lock->release_time > std::chrono::steady_clock::now()){
            l1.unlock();
            // delay_release_list 中的锁按照释放时间排序，如果第一个锁还没到期，那么后面的锁也不会到期
            // 因此至少需要等待第一个锁到期
            p->delay_release_list_cv.wait_until(l, page_lock->release_time);
            // 等到第一个锁到期, 但是此时可能有线程请求了这个锁，导致该锁现在不在队列当中, 所以需要重新检查
            continue;
        }
        page_lock->is_decided_release = true;
        page_lock->is_in_delay_release_list = false;
        // 锁已到期, 从delay_release_lock_list中移除
        p->delay_release_lock_list.pop_front();
        // 加入decided_release_queue
        std::unique_lock<std::mutex> l2(p->decided_release_queue_mutex);
        p->decided_release_queue.push(page_lock->page_id);
        p->decided_release_queue_cv.notify_one();
    }
    node_->check_delay_release_finish = true;
}

Page* ComputeServer::rpc_delay_fetch_s_page(page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    Page* page = node_->local_buffer_pool->pages_ + page_id;
    // 先在本地进行加锁
    bool lock_remote = node_->delay_local_page_lock_table->GetLock(page_id)->LockShared();
    // 再在远程加锁
    if(lock_remote){
        node_->lock_remote_cnt++;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PSLockRequest request;
        page_table_service::PSLockResponse* response = new page_table_service::PSLockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.PSLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
        }
        node_id_t valid_node = response->newest_node();
        // 如果valid是false, 则需要去远程取这个数据页
        if(valid_node != -1){
            assert(valid_node != node_->node_id);
            UpdatePageFromRemoteCompute(page, page_id, valid_node);
        }
        node_->delay_local_page_lock_table->GetLock(page_id)->LockRemoteOK();
        delete response;
    }
    return page;
}

Page* ComputeServer::rpc_delay_fetch_x_page(page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    Page* page = node_->local_buffer_pool->pages_ + page_id;
    // 先在本地进行加锁
    bool lock_remote = node_->delay_local_page_lock_table->GetLock(page_id)->LockExclusive();
    // 再在远程加锁
    if(lock_remote){
        node_->lock_remote_cnt++;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PXLockRequest request;
        page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.PXLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
        }
        node_id_t valid_node = response->newest_node();
        // 如果valid是false, 则需要去远程取这个数据页
        if(valid_node != -1){
            assert(valid_node != node_->node_id);
            UpdatePageFromRemoteCompute(page, page_id, valid_node);
        }
        node_->delay_local_page_lock_table->GetLock(page_id)->LockRemoteOK();
        delete response;
    }
    return page;
}

void ComputeServer::rpc_delay_release_s_page(page_id_t page_id){
    // release page
    node_->delay_local_page_lock_table->GetLock(page_id)->UnlockShared();
}

void ComputeServer::rpc_delay_release_x_page(page_id_t page_id){
    // release page
    node_->delay_local_page_lock_table->GetLock(page_id)->UnlockExclusive();
}

