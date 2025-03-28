#include "server.h"

bool ComputeServer::fetch_page_from_storage(table_id_t table_id, page_id_t page_id){
    std::unique_lock<std::mutex> lock(node_->lru_latch_);
    if(node_->local_page_set.find(table_id) == node_->local_page_set.end()){
        node_->local_page_set[table_id] = new std::unordered_map<page_id_t, std::list<page_id_t>::iterator>();
    }
    if(node_->lru_page_list.find(table_id) == node_->lru_page_list.end()){
        node_->lru_page_list[table_id] = new std::list<page_id_t>();
    }

    if(node_->local_page_set[table_id]->find(page_id) != node_->local_page_set[table_id]->end()){
        node_->lru_page_list[table_id]->erase(node_->local_page_set[table_id]->at(page_id)); // 删除原来的位置
        node_->lru_page_list[table_id]->push_back(page_id); // 插入到链表尾部
        node_->local_page_set[table_id]->at(page_id) = --node_->lru_page_list[table_id]->end();
        // LOG(INFO) << "hit page :" << table_id << " " << page_id;
        return true;
    }
    node_->lru_page_list[table_id]->push_back(page_id);
    node_->local_page_set[table_id]->insert(std::make_pair(page_id, --node_->lru_page_list[table_id]->end()));
    
    if(node_->lru_page_list[table_id]->size() >= (size_t)node_->meta_manager_->GetMaxPageNumPerTable(table_id) / ComputeNodeCount){
        page_id_t old_page_id = node_->lru_page_list[table_id]->front();
        node_->lru_page_list[table_id]->pop_front();
        node_->local_page_set[table_id]->erase(old_page_id);
    }
    // LOG(INFO) << "fetch page from storage: " << table_id << " " << page_id;
    return false;
}

Page* ComputeServer::rpc_star_fetch_s_page(table_id_t table_id, page_id_t page_id){
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
#if UniformHot
    if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL || table_id == 7)
#else
    if((node_->phase == Phase::PARTITION && is_partitioned_page_new(table_id, page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL || table_id == 7)
#endif
    {
        node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
        if(valid_node != -1){
            // 从远程更新数据页
            UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UpdateLocalOK();
        }
    }
    else{
        assert(node_->node_id == 0); // 只有节点号为0的节点才会进行事务处理
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
        std::random_device rd;
        static std::mt19937 gen(rd()); 
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        // if (dist(gen) <= SINGLE_MISS_CACHE_RATE){
        //     rpc_fetch_page_from_storage(table_id, page_id);
        // }
        if(!fetch_page_from_storage(table_id, page_id)){
            node_->stat_miss++;
            rpc_fetch_page_from_storage(table_id, page_id);
        }
        else{
            node_->stat_hit++;
        }
    }
    return page;
}

Page* ComputeServer::rpc_star_fetch_x_page(table_id_t table_id, page_id_t page_id){
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
#if UniformHot
    if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL)
#else
    if((node_->phase == Phase::PARTITION && is_partitioned_page_new(table_id, page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL)
#endif
    {
        node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
        if(valid_node != -1){
            // 从远程更新数据页
            UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UpdateLocalOK();
        }
    }
    else{
        assert(node_->node_id == 0); // 只有节点号为0的节点才会进行事务处理
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
        std::random_device rd;
        static std::mt19937 gen(rd()); 
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        // if (dist(gen) <= SINGLE_MISS_CACHE_RATE){
        //     rpc_fetch_page_from_storage(table_id, page_id);
        // }
        if(!fetch_page_from_storage(table_id, page_id)){
            node_->stat_miss++;
            rpc_fetch_page_from_storage(table_id, page_id);
        }
        else{
            node_->stat_hit++;
        }
    }
    return page;
}

void ComputeServer::rpc_star_release_s_page(table_id_t table_id, page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
#if UniformHot
    if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL || table_id == 7)
#else
    if((node_->phase == Phase::PARTITION && is_partitioned_page_new(table_id, page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL || table_id == 7)
#endif
    {
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    }
    else{
        assert(node_->node_id == 0); // 只有节点号为0的节点才会进行事务处理
        assert(SYSTEM_MODE == 12);
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
}

void ComputeServer::rpc_star_release_x_page(table_id_t table_id, page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
#if UniformHot
    if(node_->phase == Phase::PARTITION || node_->phase == Phase::SWITCH_TO_GLOBAL)
#else
    if((node_->phase == Phase::PARTITION && is_partitioned_page_new(table_id, page_id)) || node_->phase == Phase::SWITCH_TO_GLOBAL)
#endif
    {
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    }
    else{
        assert(node_->node_id == 0); // 只有节点号为0的节点才会进行事务处理
        assert(SYSTEM_MODE == 12);
        // release page
        int unlock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
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
}