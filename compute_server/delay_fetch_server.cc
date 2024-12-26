#include "server.h"

Page* ComputeServer::rpc_delay2_fetch_s_page(page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    Page* page = node_->local_buffer_pool->pages_ + page_id;
    // 先在本地进行加锁
    LockMode lockmode = node_->delay_fetch_local_page_lock_table->GetLock(page_id)->LockShared();
    // 再在远程加锁
    switch (lockmode)
    {
    case LockMode::SHARED:{
        node_->lock_remote_cnt++;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PSLockRequest request;
        page_table_service::PSLockResponse* response = new page_table_service::PSLockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.LRPSLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
        }
        node_id_t valid_node = response->newest_node();
        // 如果valid是false, 则需要去远程取这个数据页
        if(valid_node != -1){
            assert(valid_node != node_->node_id);
            UpdatePageFromRemoteCompute(page, page_id, valid_node);
        }
        node_->delay_fetch_local_page_lock_table->GetLock(page_id)->LockRemoteOK(node_->node_id);
        node_->delay_fetch_local_page_lock_table->GetLock(page_id)->ReLockShared();
        delete response;
        break;
    }
    case LockMode::EXCLUSIVE:{
        node_->lock_remote_cnt++;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PXLockRequest request;
        page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.LRPXLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
        }
        node_id_t valid_node = response->newest_node();
        // 如果valid是false, 则需要去远程取这个数据页
        if(valid_node != -1){
            assert(valid_node != node_->node_id);
            UpdatePageFromRemoteCompute(page, page_id, valid_node);
        }
        node_->delay_fetch_local_page_lock_table->GetLock(page_id)->LockRemoteOK(node_->node_id);
        node_->delay_fetch_local_page_lock_table->GetLock(page_id)->ReLockShared();
        delete response;
        break;
    }
    case LockMode::NONE:
        // get the page lock, do nothing
        break;
    default:
        break;
    }
    return page;
}

Page* ComputeServer::rpc_delay2_fetch_x_page(page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    Page* page = node_->local_buffer_pool->pages_ + page_id;
    // 先在本地进行加锁
    LockMode lockmode = node_->delay_fetch_local_page_lock_table->GetLock(page_id)->LockExclusive();
    switch (lockmode)
    {
    case LockMode::SHARED:{
        node_->lock_remote_cnt++;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PXLockRequest request;
        page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.LRPXLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
        }
        node_id_t valid_node = response->newest_node();
        // 如果valid是false, 则需要去远程取这个数据页
        if(valid_node != -1){
            assert(valid_node != node_->node_id);
            UpdatePageFromRemoteCompute(page, page_id, valid_node);
        }
        node_->delay_fetch_local_page_lock_table->GetLock(page_id)->LockRemoteOK(node_->node_id);
        node_->delay_fetch_local_page_lock_table->GetLock(page_id)->ReLockExclusive();
        delete response;
        break;
    }
    case LockMode::EXCLUSIVE:{
        node_->lock_remote_cnt++;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PXLockRequest request;
        page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.LRPXLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
        }
        node_id_t valid_node = response->newest_node();
        // 如果valid是false, 则需要去远程取这个数据页
        if(valid_node != -1){
            assert(valid_node != node_->node_id);
            UpdatePageFromRemoteCompute(page, page_id, valid_node);
        }
        node_->delay_fetch_local_page_lock_table->GetLock(page_id)->LockRemoteOK(node_->node_id);
        node_->delay_fetch_local_page_lock_table->GetLock(page_id)->ReLockExclusive();
        delete response;
        break;
    }
    case LockMode::NONE:
        // get the page lock, do nothing
        break;
    default:
        break;
    }
    return page;
}

// void ComputeServer::rpc_delay2_release_s_page(page_id_t page_id){
//     // release page
//     int unlock_remote = node_->delay_fetch_local_page_lock_table->GetLock(page_id)->UnlockShared();
//     if(unlock_remote > 0){
//         // rpc release page
//         page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
//         page_table_service::PAnyUnLockRequest request;
//         page_table_service::PAnyUnLockResponse* response = new page_table_service::PAnyUnLockResponse();
//         page_table_service::PageID* page_id_pb = new page_table_service::PageID();
//         page_id_pb->set_page_no(page_id);
//         request.set_allocated_page_id(page_id_pb);
//         request.set_node_id(node_->node_id);

//         brpc::Controller cntl;
//         pagetable_stub.LRPAnyUnLock(&cntl, &request, response, NULL);
//         if(cntl.Failed()){
//             LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
//         }
//         //! unlock remote ok and unlatch local
//         node_->delay_fetch_local_page_lock_table->GetLock(page_id)->UnlockRemoteOK();
//         // delete response;
//         delete response;
//     }
// }

// void ComputeServer::rpc_delay2_release_x_page(page_id_t page_id){
//     // release page
//     int unlock_remote = node_->delay_fetch_local_page_lock_table->GetLock(page_id)->UnlockExclusive();
    
//     if(unlock_remote > 0){
//         assert(unlock_remote == 2);
//         // 1. rpc release page
//         page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
//         page_table_service::PAnyUnLockRequest unlock_request;
//         page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
//         page_table_service::PageID* page_id_pb3 = new page_table_service::PageID();
//         page_id_pb3->set_page_no(page_id);
//         unlock_request.set_allocated_page_id(page_id_pb3);
//         unlock_request.set_node_id(node_->node_id);

//         brpc::Controller cntl;
//         pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
//         if(cntl.Failed()){
//             LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
//         }
//         //! unlock remote ok and unlatch local
//         node_->delay_fetch_local_page_lock_table->GetLock(page_id)->UnlockRemoteOK();
//         // delete response;
//         delete unlock_response;
//     }
//     return;
// }

Page* ComputeServer::rpc_delay2_fetch_s_page_new(CoroutineScheduler* coro_sched, coro_yield_t &yield, coro_id_t coro_id, table_id_t table_id, page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;

    // 先在本地进行加锁
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
        // LOG(INFO) << "Fetch S Page " << table_id << " "<< page_id;
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
        // while((*finish) == false){
        //     coro_sched->Yield(yield, coro_id);
        // }
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
        // LOG(INFO) << "Fetch S Page " << table_id << " "<< page_id << " OK";
        // delete response;
        break;
    }
    case LockMode::EXCLUSIVE:{
        // LOG(INFO) << "*Fetch X Page " << table_id << " "<< page_id;
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
        // LOG(INFO) << "Fetch X Page " << table_id << " "<< page_id << " OK";
        // delete response;
        break;
    }
    case LockMode::NONE:
        // get the page lock, do nothing
        break;
    default:
        break;
    }
    return page;
}

Page* ComputeServer::rpc_delay2_fetch_x_page_new(CoroutineScheduler* coro_sched, coro_yield_t &yield, coro_id_t coro_id, table_id_t table_id, page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    node_->fetch_allpage_cnt++;
    // 先在本地进行加锁
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
    bool delay = ((table_id) <= 1);
    double hot_num = HHH;
    int page_num = node_->meta_manager_->GetMaxPageNumPerTable(table_id);
    if(page_id % ( page_num / ComputeNodeCount) > hot_num){
        delay = false;
    }
    LockMode lockmode = node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive(coro_sched, yield, coro_id, delay);
    switch (lockmode)
    {
    case LockMode::SHARED:{
        assert(false);
    }
    case LockMode::EXCLUSIVE:{
        // LOG(INFO) << "Fetch X Page " << table_id << " "<< page_id;
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
        // LOG(INFO) << "Fetch X Page " << table_id << " "<< page_id << " OK";
        // delete response;
        break;
    }
    case LockMode::NONE:
        // get the page lock, do nothing
        break;
    default:
        break;
    }
    return page;
}

void ComputeServer::rpc_delay2_release_s_page_new(table_id_t table_id, page_id_t page_id){
    // release page
    int unlock_remote = node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    if(unlock_remote > 0){
        // rpc release page
        // LOG(INFO) << "Release S Page " << table_id << " "<< page_id;
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

void ComputeServer::rpc_delay2_release_x_page_new(table_id_t table_id, page_id_t page_id){
    // release page
    int unlock_remote = node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    if(unlock_remote > 0){
        assert(unlock_remote == 2);
        // LOG(INFO) << "Release X Page " << table_id << " "<< page_id;
        // 1. rpc release page
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PAnyUnLockRequest unlock_request;
        page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
        page_table_service::PageID* page_id_pb3 = new page_table_service::PageID();
        page_id_pb3->set_page_no(page_id);
        page_id_pb3->set_table_id(table_id);
        unlock_request.set_allocated_page_id(page_id_pb3);
        unlock_request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
        //! unlock remote ok and unlatch local
        node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
        // delete response;
        delete unlock_response;
    }
    return;
}

// void ComputeServer::rpc_delay_fetch_release_all_page() {
//     // LOG(INFO) << "node id: " << node_->getNodeID() <<"Release all pages";
//     for(int page_id=0; page_id<ComputeNodeBufferPageSize; page_id++){
//         int unlock_remote = node_->delay_fetch_local_page_lock_table->GetLock(page_id)->UnlockAny();
//         // if(unlock_remote == 1){
//         //     LOG(INFO) << "node id: " << node_->getNodeID() << " UnlockAny: " << page_id << " remote_mode == SHARED";
//         // }else if(unlock_remote == 2){
//         //     LOG(INFO) << "node id: " << node_->getNodeID() << " UnlockAny: " << page_id << " remote_mode == EXCLUSIVE";
//         // }
//         if(unlock_remote == 0) continue;
//         // 3. rpc release page
//         page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
//         page_table_service::PAnyUnLockRequest unlock_request;
//         page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
//         page_table_service::PageID* page_id_pb = new page_table_service::PageID();
//         page_id_pb->set_page_no(page_id);
//         unlock_request.set_allocated_page_id(page_id_pb);
//         unlock_request.set_node_id(node_->node_id);

//         brpc::Controller cntl;
//         pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
//         if(cntl.Failed()){
//             LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
//         }
//         //! unlock remote ok and unlatch local
//         node_->delay_fetch_local_page_lock_table->GetLock(page_id)->UnlockRemoteOK();
//         // delete response;
//         delete unlock_response;
//     }
//     return;
// }

void ComputeServer::rpc_delay_fetch_release_all_page_new(){
    page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
    page_table_service::PAnyUnLocksRequest unlock_request;
    page_table_service::PAnyUnLockResponse *unlock_response = new page_table_service::PAnyUnLockResponse();
    brpc::Controller cntl;
    // LOG(INFO) << "node id: " << node_->getNodeID() <<"Release all pages";
    for(size_t i=0; i < node_->delay_fetch_local_page_lock_tables.size(); i++){
        auto max_page_id = node_->meta_manager_->GetMaxPageNumPerTable(i);
        for(int page_id=0; page_id<=max_page_id; page_id++){
            int unlock_remote = node_->delay_fetch_local_page_lock_tables[i]->GetLock(page_id)->UnlockAny();
            if(unlock_remote == 0) continue;
            // 3. rpc release page
            auto p = unlock_request.add_pages_id();
            p->set_page_no(page_id);
            p->set_table_id(i);
        }
    }
    unlock_request.set_node_id(node_->node_id);
    pagetable_stub.LRPAnyUnLocks(&cntl, &unlock_request, unlock_response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to unlock pages " << " in remote page table";
    }
    //! unlock remote ok and unlatch local
    for(int i=0; i<unlock_request.pages_id_size(); i++){
        table_id_t table_id = unlock_request.pages_id(i).table_id();
        int page_no = unlock_request.pages_id(i).page_no();
        node_->delay_fetch_local_page_lock_tables[table_id]->GetLock(page_no)->UnlockRemoteOK();
    }
    // delete response;
    delete unlock_response;
    return;
}

// // 这里用异步的方法实现释放所有数据页
// void ComputeServer::rpc_delay_fetch_release_all_page_async() {
//     std::vector<std::pair<brpc::CallId, page_id_t>> unlock_cids;
//     for(int page_id=0; page_id<ComputeNodeBufferPageSize; page_id++){
//         int unlock_remote = node_->delay_fetch_local_page_lock_table->GetLock(page_id)->UnlockAny();
//         if(unlock_remote == 0) continue;        
//         // 这里可以直接释放远程锁
//         page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
//         page_table_service::PAnyUnLockRequest unlock_request;
//         page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
//         page_table_service::PageID* page_id_pb = new page_table_service::PageID();
//         page_id_pb->set_page_no(page_id);
//         unlock_request.set_allocated_page_id(page_id_pb);
//         unlock_request.set_node_id(node_->node_id);
//         brpc::Controller* unlock_cntl = new brpc::Controller();
//         unlock_cids.push_back(std::make_pair(unlock_cntl->call_id(), page_id));
//         pagetable_stub.LRPAnyUnLock(unlock_cntl, &unlock_request, unlock_response, 
//             brpc::NewCallback(LazyReleaseRPCDone, unlock_response, unlock_cntl));
//     }
//     for(auto cids : unlock_cids){
//         brpc::Join(cids.first);
//         //! unlock remote ok and unlatch local
//         node_->delay_fetch_local_page_lock_table->GetLock(cids.second)->UnlockRemoteOK();
//     }
//     return;
// }

void ComputeServer::rpc_delay_fetch_release_all_page_async_new(){
    std::vector<std::vector<std::pair<brpc::CallId, page_id_t>>> unlock_cids(node_->delay_fetch_local_page_lock_tables.size());
    for(size_t i = 0; i < node_->delay_fetch_local_page_lock_tables.size(); i++) {
        auto max_page_id = node_->meta_manager_->GetMaxPageNumPerTable(i);
        for(int page_id=0; page_id<=max_page_id; page_id++){
            int unlock_remote = node_->delay_fetch_local_page_lock_tables[i]->GetLock(page_id)->UnlockAny();
            if(unlock_remote == 0) continue;
            // 这里可以直接释放远程锁
            // LOG(INFO) << "Unlock any table: " << i << "page: "<< page_id;
            page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
            page_table_service::PAnyUnLockRequest unlock_request;
            page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
            page_table_service::PageID* page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(i);
            unlock_request.set_allocated_page_id(page_id_pb);
            unlock_request.set_node_id(node_->node_id);
            brpc::Controller* unlock_cntl = new brpc::Controller();
            unlock_cids[i].push_back(std::make_pair(unlock_cntl->call_id(), page_id));
            pagetable_stub.LRPAnyUnLock(unlock_cntl, &unlock_request, unlock_response, 
                brpc::NewCallback(LazyReleaseRPCDone, unlock_response, unlock_cntl));
        }
    }
    for(size_t i = 0; i < node_->delay_fetch_local_page_lock_tables.size(); i++) {
        for (auto cids: unlock_cids[i]) {
            brpc::Join(cids.first);
            //! unlock remote ok and unlatch local
            // LOG(INFO) << "Unlock ok: " << i << "page: "<< cids.second;
            node_->delay_fetch_local_page_lock_tables[i]->GetLock(cids.second)->UnlockRemoteOK();
        }
    }
    return;
}