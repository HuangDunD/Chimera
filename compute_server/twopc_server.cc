#include "server.h"

namespace twopc_service{
    void TwoPCServiceImpl::GetDataItem(::google::protobuf::RpcController* controller,
                        const ::twopc_service::GetDataItemRequest* request,
                        ::twopc_service::GetDataItemResponse* response,
                        ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        // 从本地取数据
        table_id_t table_id = request->item_id().table_id();
        page_id_t page_id = request->item_id().page_no();
        int slot_id = request->item_id().slot_id();
        bool lock = request->item_id().lock_data();

        if(!lock){
            Page* page = server->local_fetch_s_page(table_id, page_id);
            char* data = page->get_data();
            response->set_data(data, PAGE_SIZE);
            // char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            // char *slots = bitmap + server->get_node()->getMetaManager()->GetTableMeta(table_id).bitmap_size_;
            // char* tuple = slots + slot_id * (sizeof(DataItem) + sizeof(itemkey_t));
            // char* ret = tuple + sizeof(itemkey_t);
            // response->set_data(ret, sizeof(DataItem));
            server->local_release_s_page(table_id, page_id);
        }else{
            // LOG(INFO) << "Node " << server->get_node()->getNodeID() << " lock data item " << table_id << " " << page_id << " " << slot_id;
            Page* page = server->local_fetch_x_page(table_id, page_id);
            char* data = page->get_data();
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            char *slots = bitmap + server->get_node()->getMetaManager()->GetTableMeta(table_id).bitmap_size_;
            char* tuple = slots + slot_id * (sizeof(DataItem) + sizeof(itemkey_t));
            // lock the data
            DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            if(item->lock == UNLOCKED){
                item->lock = EXCLUSIVE_LOCKED;
                char* ret = tuple + sizeof(itemkey_t);
                response->set_data(data, PAGE_SIZE);
                // response->set_data(ret, sizeof(DataItem));
            }
            else {
                // abort
                response->set_abort(true);
            }
            server->local_release_x_page(table_id, page_id);
        }
        return;
    };

    // maybe unused
    void TwoPCServiceImpl::WriteDataItem(::google::protobuf::RpcController* controller,
                        const ::twopc_service::WriteDataItemRequest* request,
                        ::twopc_service::WriteDataItemResponse* response,
                        ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        // 从本地取数据
        table_id_t table_id = request->item_id().table_id();
        page_id_t page_id = request->item_id().page_no();
        int slot_id = request->item_id().slot_id();
        assert(request->data().size() == MAX_ITEM_SIZE);
        char* write_remote_data = (char*)request->data().c_str();

        Page* page = server->local_fetch_x_page(table_id, page_id);
        char* data = page->get_data();
        char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        char *slots = bitmap + server->get_node()->getMetaManager()->GetTableMeta(table_id).bitmap_size_;
        char* tuple = slots + slot_id * (sizeof(DataItem) + sizeof(itemkey_t));
        DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
        assert(item->lock == EXCLUSIVE_LOCKED);
        memcpy(item->value, write_remote_data, MAX_ITEM_SIZE);
        item->lock = UNLOCKED;
        server->local_release_x_page(table_id, page_id);
        return;
    };

    void TwoPCServiceImpl::Prepare(::google::protobuf::RpcController* controller,
                       const ::twopc_service::PrepareRequest* request,
                       ::twopc_service::PrepareResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        storage_service::StorageService_Stub stub(server->get_storage_channel());
        brpc::Controller cntl;
        storage_service::LogWriteRequest log_request;
        storage_service::LogWriteResponse log_response;

        uint64_t tx_id = request->transaction_id();
        TxnLog txn_log;
        BatchEndLogRecord* prepare_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        txn_log.logs.push_back(prepare_log);
        txn_log.batch_id_ = tx_id;
        log_request.set_log(txn_log.get_log_string());

        stub.LogWrite(&cntl, &log_request, &log_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to write log";
        }
        response->set_ok(true);
    };

    void add_milliseconds(struct timespec& ts, long long ms) {
        ts.tv_sec += ms / 1000;
        ts.tv_nsec += (ms % 1000) * 1000000;

        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec += ts.tv_nsec / 1000000000;
            ts.tv_nsec %= 1000000000;
        }
    }

    void TwoPCServiceImpl::Commit(::google::protobuf::RpcController* controller,
                        const ::twopc_service::CommitRequest* request,
                        ::twopc_service::CommitResponse* response,
                        ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        storage_service::StorageService_Stub stub(server->get_storage_channel());
        brpc::Controller cntl;
        storage_service::LogWriteRequest log_request;
        storage_service::LogWriteResponse log_response;
        uint64_t tx_id = request->transaction_id();

        int item_size = request->item_id_size();
        assert(item_size == request->data_size());
        for(int i=0; i<item_size; i++){
            table_id_t table_id = request->item_id(i).table_id();
            page_id_t page_id = request->item_id(i).page_no();
            int slot_id = request->item_id(i).slot_id();
            char* write_remote_data = (char*)request->data(i).c_str();

            // LOG(INFO) << "Node " << server->get_node()->getNodeID() << " release data item " << table_id << " " << page_id << " " << slot_id;
            
            Page* page = server->local_fetch_x_page(table_id, page_id);
            char* data = page->get_data();
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            char *slots = bitmap + server->get_node()->getMetaManager()->GetTableMeta(table_id).bitmap_size_;
            char* tuple = slots + slot_id * (sizeof(DataItem) + sizeof(itemkey_t));
            DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            assert(item->lock == EXCLUSIVE_LOCKED);
            memcpy(item->value, write_remote_data, MAX_ITEM_SIZE);
            item->lock = UNLOCKED;
            server->local_release_x_page(table_id, page_id);
        }

#if !GroupCommit
        TxnLog txn_log;
        BatchEndLogRecord* commit_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        txn_log.logs.push_back(commit_log);
        txn_log.batch_id_ = tx_id;
        log_request.set_log(txn_log.get_log_string());
        stub.LogWrite(&cntl, &log_request, &log_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to write log";
        }
        response->set_latency_commit(0);
#else
        BatchEndLogRecord* commit_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        commit_log_mutex.lock();
        txn_log.logs.push_back(commit_log);
        txn_log.batch_id_ = tx_id;
        commit_log_mutex.unlock();
        struct timespec now_time;
        clock_gettime(CLOCK_REALTIME, &now_time);
        // auto diff_ms = (next_commit_time.tv_sec - now_time.tv_sec)* 1000LL + (double)(next_commit_time.tv_nsec - now_time.tv_nsec) / 1000000;
        long long diff_sec = next_commit_time.tv_sec - now_time.tv_sec;
        long diff_nsec = next_commit_time.tv_nsec - now_time.tv_nsec;

        if (diff_sec > 0 || (diff_sec == 0 && diff_nsec > 0)) {
            long long diff_ms = diff_sec * 1000LL + (double)diff_nsec / 1000000;
            // std::cout << "Time difference in milliseconds: " << diff_ms << std::endl;
            response->set_latency_commit(diff_ms);
        } else {
            // 如果 now_time 大于等于 next_commit_time，将 diff_ms 设为负数
            long long diff_ms = diff_sec * 1000LL + (double)diff_nsec / 1000000;
            // std::cout << "Time difference in milliseconds: " << diff_ms << std::endl;
            commit_log_mutex.lock();
            response->set_latency_commit(0);
            if(txn_log.logs.size()!=0){
                log_request.set_log(txn_log.get_log_string());
                stub.LogWrite(&cntl, &log_request, &log_response, NULL);
                if(cntl.Failed()){
                    LOG(ERROR) << "Fail to write log";
                }
                txn_log.logs.clear();
            }
            commit_log_mutex.unlock();
            add_milliseconds(now_time, EpochTime);
            next_commit_time = now_time;
            // LOG(INFO) << "2pc group commit...";
        }
#endif
    };

    void TwoPCServiceImpl::Abort(::google::protobuf::RpcController* controller,
                        const ::twopc_service::AbortRequest* request,
                        ::twopc_service::AbortResponse* response,
                        ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        storage_service::StorageService_Stub stub(server->get_storage_channel());
        brpc::Controller cntl;
        storage_service::LogWriteRequest log_request;
        storage_service::LogWriteResponse log_response;
        uint64_t tx_id = request->transaction_id();
        TxnLog txn_log;
        BatchEndLogRecord* prepare_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        txn_log.logs.push_back(prepare_log);
        txn_log.batch_id_ = tx_id;
        log_request.set_log(txn_log.get_log_string());

        stub.LogWrite(&cntl, &log_request, &log_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to write log";
        }

        int item_size = request->item_id_size();
        
        for(int i=0; i<item_size; i++){
            table_id_t table_id = request->item_id(i).table_id();
            page_id_t page_id = request->item_id(i).page_no();
            int slot_id = request->item_id(i).slot_id();

            // LOG(INFO) << "Node " << server->get_node()->getNodeID() << " release data item " << table_id << " " << page_id << " " << slot_id;
            
            Page* page = server->local_fetch_x_page(table_id, page_id);
            char* data = page->get_data();
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            char *slots = bitmap + server->get_node()->getMetaManager()->GetTableMeta(table_id).bitmap_size_;
            char* tuple = slots + slot_id * (sizeof(DataItem) + sizeof(itemkey_t));
            DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            assert(item->lock == EXCLUSIVE_LOCKED);
            item->lock = UNLOCKED;
            server->local_release_x_page(table_id, page_id);
        }
    };
};

Page* ComputeServer::local_fetch_s_page(table_id_t table_id, page_id_t page_id){
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    return page;
}

Page* ComputeServer::local_fetch_x_page(table_id_t table_id, page_id_t page_id){
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    return page;
}

void ComputeServer::local_release_s_page(table_id_t table_id, page_id_t page_id){
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    return;
}

void ComputeServer::local_release_x_page(table_id_t table_id, page_id_t page_id){
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    return;
}