#include "server.h"

Page* ComputeServer::rpc_leap_fetch_all_page(std::vector<table_id_t> table_ids, std::vector<page_id_t> page_ids, std::vector<bool> is_x){
    assert(table_ids.size() == page_ids.size());
    assert(table_ids.size() == is_x.size());
    for(size_t i=0; i<table_ids.size(); i++){
        auto page_id = page_ids[i];
        auto table_id = table_ids[i];
        auto isx = is_x[i];
        assert(page_id < ComputeNodeBufferPageSize);
        this->node_->fetch_allpage_cnt++;
        Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
        bool lock_remote;
        if(isx){
            lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
            if(lock_remote){
                node_->lock_remote_cnt++;
                brpc::Controller cntl;
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PXLockRequest request;
                page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
                page_table_service::PageID* page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                request.set_allocated_page_id(page_id_pb);
                request.set_node_id(node_->node_id);

                pagetable_stub.LRPXLock(&cntl, &request, response, NULL);
                if(cntl.Failed()){
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
                if(valid_node != -1){
                    assert(valid_node != node_->node_id);
                    UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
                }
                //! lock remote ok and unlatch local
                node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
                delete response;
            }
        } else{
            lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
            if(lock_remote){
                node_->lock_remote_cnt++;
                brpc::Controller cntl;
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                page_table_service::PSLockRequest request;
                page_table_service::PSLockResponse* response = new page_table_service::PSLockResponse();
                page_table_service::PageID* page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                request.set_allocated_page_id(page_id_pb);
                request.set_node_id(node_->node_id);

                pagetable_stub.LRPSLock(&cntl, &request, response, NULL);
                if(cntl.Failed()){
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
                if(valid_node != -1){
                    assert(valid_node != node_->node_id);
                    UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
                }
                //! lock remote ok and unlatch local
                node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
                delete response;
            }
        }
    }
}

Page* ComputeServer::rpc_leap_fetch_s_page(table_id_t table_id, page_id_t page_id) {
    // LEAP 的ownership不采用共享-排他模式, LEAP移动数据当某个计算节点没有掌握数据的完整控制权
    assert(page_id < ComputeNodeBufferPageSize);
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
    // 先在本地进行加锁
    //  LOG(INFO) << "node id: " << node_->node_id << " LockExclusive table id: " << table_id << "page_id" << page_id;
    bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    if(lock_remote){
        // 再在远程加锁
        //  LOG(INFO) << "node id: " << node_->node_id << " remote Exclusive table id: " << table_id << "page_id" << page_id;
        node_->lock_remote_cnt++;
        brpc::Controller cntl;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PXLockRequest request;
        page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        pagetable_stub.LRPXLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
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
        if(valid_node != -1){
            assert(valid_node != node_->node_id);
            UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
        }
        //! lock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
        delete response;
    }

    return page;
}

Page* ComputeServer::rpc_leap_fetch_x_page(table_id_t table_id, page_id_t page_id) {
    assert(page_id < ComputeNodeBufferPageSize);
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
    // 先在本地进行加锁
    //  LOG(INFO) << "node id: " << node_->node_id << " LockExclusive table id: " << table_id << "page_id" << page_id;
    bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    if(lock_remote){
        // 再在远程加锁
        //  LOG(INFO) << "node id: " << node_->node_id << " remote Exclusive table id: " << table_id << "page_id" << page_id;
        node_->lock_remote_cnt++;
        brpc::Controller cntl;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PXLockRequest request;
        page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        pagetable_stub.LRPXLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
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
        if(valid_node != -1){
            assert(valid_node != node_->node_id);
            UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
        }
        //! lock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
        delete response;
    }

    return page;
}

void ComputeServer::rpc_leap_release_s_page(table_id_t table_id, page_id_t page_id) {
    // release page
    // LOG(INFO) << "node id: " << node_->node_id << " Release x table id: " << table_id << "page_id" << page_id;
    int unlock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    if(unlock_remote > 0){
        // 1. rpc release page
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PAnyUnLockRequest unlock_request;
        page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        unlock_request.set_allocated_page_id(page_id_pb);
        unlock_request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
        //! unlock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
        // delete response;
        delete unlock_response;
    }
    return;
}

void ComputeServer::rpc_leap_release_x_page(table_id_t table_id, page_id_t page_id) {
    // release page
    // LOG(INFO) << "node id: " << node_->node_id << " Release x table id: " << table_id << "page_id" << page_id;
    int unlock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    if(unlock_remote > 0){
        // 1. rpc release page
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PAnyUnLockRequest unlock_request;
        page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        unlock_request.set_allocated_page_id(page_id_pb);
        unlock_request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
        //! unlock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
        // delete response;
        delete unlock_response;
    }
    return;
}
