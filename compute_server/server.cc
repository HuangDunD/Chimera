#include "server.h"
#include "worker/global.h"
#include "base/queue.h"
#include <vector>
#include <utility>

int socket_start_client(std::string ip, int port){
    // 创建套接字
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

    // 设置服务器地址和端口
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);

    if(inet_pton(AF_INET, ip.c_str(), &(serverAddress.sin_addr)) <= 0){
        std::cerr << "Invalid address" << std::endl;
        return -1;
    }

    if(clientSocket < 0){
        std::cerr << "Socket creation failed" << std::endl;
        close(clientSocket);
        return -1;
    }

    // 连接到服务器
    if(connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0){
        std::cerr << "Connection failed" << std::endl;
        close(clientSocket);
        return -1;
    }
    // 发送 节点数目 到服务器
    send(clientSocket, &ComputeNodeCount, sizeof(ComputeNodeCount), 0);

    // 接收服务器发送的 SYN 消息
    char buffer[10];
    recv(clientSocket, buffer, 9, 0);
    buffer[9] = '\0';
    assert(strcmp(buffer, "SYN-BEGIN") == 0);

    std::cout << "Remote server has build brpc channel with compute nodes" << std::endl;

    // 关闭套接字
    close(clientSocket);

    return 0;
}

int socket_finish_client(std::string ip, int port){
    // 创建套接字
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

    // 设置服务器地址和端口
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);

    if(inet_pton(AF_INET, ip.c_str(), &(serverAddress.sin_addr)) <= 0){
        std::cerr << "Invalid address" << std::endl;
        return -1;
    }

    if(clientSocket < 0){
        std::cerr << "Socket creation failed" << std::endl;
        close(clientSocket);
        return -1;
    }

    // 连接到服务器
    if(connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0){
        std::cerr << "Connection failed" << std::endl;
        close(clientSocket);
        return -1;
    }
    // 发送 节点数目 到服务器
    send(clientSocket, &ComputeNodeCount, sizeof(ComputeNodeCount), 0);

    // 接收服务器发送的 SYN 消息
    char buffer[11];
    recv(clientSocket, buffer, 10, 0);
    buffer[10] = '\0';
    assert(strcmp(buffer, "SYN-FINISH") == 0);

    std::cout << "Remote server has build brpc channel with compute nodes" << std::endl;


    // 关闭套接字
    close(clientSocket);

    return 0;
}

namespace compute_node_service{
void ComputeNodeServiceImpl::Pending(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::PendingRequest* request,
                       ::compute_node_service::PendingResponse* response,
                       ::google::protobuf::Closure* done){

            brpc::ClosureGuard done_guard(done);

            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            bool xpending = (request->pending_type() == PendingType::XPending);

            // LOG(INFO) << "Panding node id: " << server->get_node()->getNodeID() << " page id: " << page_id;
            int unlock_remote = server->get_node()->PendingPage(page_id, xpending, table_id);
            if(unlock_remote > 0){
                // 3. rpc release page
                page_table_service::PageTableService_Stub pagetable_stub(server->get_pagetable_channel());
                page_table_service::PAnyUnLockRequest unlock_request;
                page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
                page_table_service::PageID* page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                page_id_pb->set_table_id(table_id);
                unlock_request.set_allocated_page_id(page_id_pb);
                unlock_request.set_node_id(server->get_node()->getNodeID());

                brpc::Controller cntl;
                pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
                if(cntl.Failed()){
                    LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
                }
                //! unlock remote ok and unlatch local
                if(SYSTEM_MODE == 1 || SYSTEM_MODE == 3 || SYSTEM_MODE == 11){
                    server->get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->UnlockRemoteOK();
                }else if(SYSTEM_MODE == 6 || SYSTEM_MODE == 7){
                    server->get_node()->getDelayFetchPageLockTable(table_id)->GetLock(page_id)->UnlockRemoteOK();
                }else{
                    assert(false);
                }
                // delete response;
                delete unlock_response;
            }

            // 添加模拟延迟
            // usleep(NetworkLatency); // 100us
            return;
        }

void ComputeNodeServiceImpl::GetPage(::google::protobuf::RpcController* controller,
                    const ::compute_node_service::GetPageRequest* request,
                    ::compute_node_service::GetPageResponse* response,
                    ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        // Page* page = server->get_node()->getBufferPool()->GetPage(page_id);
        Page* page = server->get_node()->getBufferPoolByIndex(request->page_id().table_id())->GetPage(page_id);
        response->set_page_data(page->get_data(), PAGE_SIZE);

        // 添加模拟延迟
        // usleep(NetworkLatency); // 100us
        return;
    }

void ComputeNodeServiceImpl::LockSuccess(::google::protobuf::RpcController* controller,
                    const ::compute_node_service::LockSuccessRequest* request,
                    ::compute_node_service::LockSuccessResponse* response,
                    ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        bool xlock = request->xlock_succeess();
        node_id_t node_id = request->newest_node();
        server->get_node()->NotifyLockPageSuccess(table_id, page_id, xlock, node_id);
        return;
    }

void ComputeNodeServiceImpl::TransferDTX(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::TransferDTXRequest* request,
                       ::compute_node_service::TransferDTXResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        std::vector<dtx_entry> txns;
        for (auto d : request->dtxs()) {
            txns.push_back(dtx_entry(d.seed(),d.tx_type(),d.dtx_id(),d.is_partitioned()));
        }
        queue->seq_enqueue(txns, request->batch_id(),request->src_node_id());
        // 添加模拟延迟
        // usleep(NetworkLatency); // 100us
        return;
    }
}

void ComputeServer::UpdatePageFromRemoteCompute(Page* page, page_id_t page_id, node_id_t node_id){
    // 从远程取数据页
    brpc::Controller cntl;
    node_->fetch_remote_cnt++;

    // 使用远程compute node进行更新
    compute_node_service::ComputeNodeService_Stub compute_node_stub(&nodes_channel[node_id]);
    compute_node_service::GetPageRequest request;
    compute_node_service::GetPageResponse* response = new compute_node_service::GetPageResponse();
    compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_id(0);
    request.set_allocated_page_id(page_id_pb);
    compute_node_stub.GetPage(&cntl, &request, response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to fetch page " << page_id << " from remote compute node";
    }
    assert(response->page_data().size() == PAGE_SIZE);
    memcpy(page->get_data(), response->page_data().c_str(), response->page_data().size());
    // delete response;
    delete response;
}

void ComputeServer::UpdatePageFromRemoteComputeNew(Page* page, table_id_t table_id, page_id_t page_id, node_id_t node_id){
    // LOG(INFO) << "need to update page from node " << node_id << " table id: " << table_id << "page_id" << page_id;
    // 从远程取数据页
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
    brpc::Controller cntl;
    node_->fetch_remote_cnt++;

    // 使用远程compute node进行更新
    compute_node_service::ComputeNodeService_Stub compute_node_stub(&nodes_channel[node_id]);
    compute_node_service::GetPageRequest request;
    compute_node_service::GetPageResponse* response = new compute_node_service::GetPageResponse();
    compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_id(table_id);
    request.set_allocated_page_id(page_id_pb);
    compute_node_stub.GetPage(&cntl, &request, response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to fetch page " << page_id << " from remote compute node";
    }
    assert(response->page_data().size() == PAGE_SIZE);
    memcpy(page->get_data(), response->page_data().c_str(), response->page_data().size());
    // delete response;
    delete response;
    clock_gettime(CLOCK_REALTIME, &end_time);
    update_m.lock();
    this->tx_update_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
    update_m.unlock();
}

void ComputeServer::SendBatch(std::vector<dtx_entry> txns, batch_id_t bid, node_id_t node_id, node_id_t s_nid){
    // 从远程取数据页
    brpc::Controller cntl;
    node_->fetch_remote_cnt++;

    // 使用远程compute node进行更新
    compute_node_service::ComputeNodeService_Stub compute_node_stub(&nodes_channel[node_id]);
    compute_node_service::TransferDTXRequest request;
    compute_node_service::TransferDTXResponse* response = new compute_node_service::TransferDTXResponse();
    request.set_batch_id(bid);
    request.set_dst_node_id(node_id);
    request.set_src_node_id(s_nid);
    for (int i = 0; i < txns.size(); i++) {
        compute_node_service::DTX *dtx = request.add_dtxs();
        dtx->set_dtx_id(txns[i].tid);
        dtx->set_seed(txns[i].seed);
        dtx->set_is_partitioned(txns[i].is_partitioned);
        dtx->set_tx_type(txns[i].type);
        
    }
    // 如果失败了就重新发送，直到成功
    int count = 0;
    while (true) {
        compute_node_stub.TransferDTX(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            count++;
            if (count % 100 == 0) {
                LOG(ERROR) << "Fail to send batch " << bid << " to remote compute node";
            }
            // LOG(ERROR) << "Fail to send batch " << bid << " to remote compute node";
        } else {
            break;
        }
    }
    // assert(response->page_data().size() == PAGE_SIZE);
    // memcpy(page->get_data(), response->page_data().c_str(), response->page_data().size());
    // delete response;
    delete response;
}

// 用于一次性释放指定范围内的所有数据页, 抽象为一个函数的原因是需要更好地异步处理
void ComputeServer::rpc_phase_switch_invalid_pages(int start_pageid, int end_pageid){
    std::vector<brpc::CallId> cids;
    for(int page_id=start_pageid; page_id<end_pageid; page_id++){
        if(node_->local_page_lock_table->GetLock(page_id)->GetDirty() == true){
            partition_table_service::InvalidRequest invalid_request;
            partition_table_service::PageID* page_id_pb = new partition_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            invalid_request.set_allocated_page_id(page_id_pb);
            invalid_request.set_node_id(this->get_node()->getNodeID());
            partition_table_service::PartitionTableService_Stub partition_table_stub(get_pagetable_channel());
            partition_table_service::InvalidResponse* invalid_response = new partition_table_service::InvalidResponse();
            brpc::Controller* cntl = new brpc::Controller();
            cids.push_back(cntl->call_id());
            partition_table_stub.Invalid(cntl, &invalid_request, invalid_response, 
                brpc::NewCallback(InvalidRPCDone, invalid_response, cntl));
                
            // 释放脏标记
            node_->local_page_lock_table->GetLock(page_id)->SetDirty(false);
        }
    }
    // 等待所有的请求完成
    for(uint32_t i = 0; i < cids.size(); i++){
        brpc::Join(cids[i]);
    }
}

void ComputeServer::rpc_phase_switch_get_invalid_pages_new(){
    auto table_size = node_->meta_manager_->GetTableNum();
    partition_table_service::GetInvalidRequest get_invalid_request;
    partition_table_service::GetInvalidResponse get_invalid_response;
    partition_table_service::PartitionTableService_Stub partition_table_stub(get_pagetable_channel());
    get_invalid_request.set_node_id(this->get_node()->getNodeID());
    for(int table_id = 0; table_id < table_size; table_id++){
        auto page_size = get_partitioned_size(table_id);
        int start_pageid = node_->node_id * page_size;
        int end_pageid = (node_->node_id + 1) * page_size;
        get_invalid_request.add_table_id(table_id);
        get_invalid_request.add_start_page_no(start_pageid);
        get_invalid_request.add_end_page_no(end_pageid);
    }
    brpc::Controller cntl;
    partition_table_stub.GetInvalid(&cntl, &get_invalid_request, &get_invalid_response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to get invalid pages from remote partition table";
    }
    assert(get_invalid_response.invalid_page_no_size() == get_invalid_response.newest_node_id_size());
    assert(get_invalid_response.invalid_page_no_size() == get_invalid_response.table_id_size());
    for(int i = 0; i < get_invalid_response.invalid_page_no_size(); i++){
        int table_id = get_invalid_response.table_id(i);
        int page_id = get_invalid_response.invalid_page_no(i);
        int node_id = get_invalid_response.newest_node_id(i);
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->SetNewestNode(node_id);
    }
    return;
}

void ComputeServer::rpc_phase_switch_sync_invalid_pages_new(){
    auto table_size = node_->meta_manager_->GetTableNum();
    partition_table_service::GetInvalidRequest get_invalid_request;
    partition_table_service::GetInvalidResponse get_invalid_response;
    partition_table_service::PartitionTableService_Stub partition_table_stub(get_pagetable_channel());
    get_invalid_request.set_node_id(this->get_node()->getNodeID());
    for(int table_id = 0; table_id < table_size; table_id++){
        auto page_size = get_partitioned_size(table_id);
        int start_pageid = node_->node_id * page_size;
        int end_pageid = (node_->node_id + 1) * page_size;
        get_invalid_request.add_table_id(table_id);
        get_invalid_request.add_start_page_no(start_pageid);
        get_invalid_request.add_end_page_no(end_pageid);
    }
    brpc::Controller cntl;
    partition_table_stub.GetInvalid(&cntl, &get_invalid_request, &get_invalid_response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to get invalid pages from remote partition table";
    }
    assert(get_invalid_response.invalid_page_no_size() == get_invalid_response.newest_node_id_size());
    assert(get_invalid_response.invalid_page_no_size() == get_invalid_response.table_id_size());
    for(int i = 0; i < get_invalid_response.invalid_page_no_size(); i++){
        int table_id = get_invalid_response.table_id(i);
        int page_id = get_invalid_response.invalid_page_no(i);
        int node_id = get_invalid_response.newest_node_id(i);
        assert(node_id != -1);
        // sync invalid page
        Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
        if(node_id != -1){
            // 从远程更新数据页
            UpdatePageFromRemoteComputeNew(page, table_id, page_id, node_id);
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UpdateLocalOK();
        }
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->SetNewestNode(-1); // update finish
    }
    return;
}

void ComputeServer::rpc_phase_switch_invalid_pages_new(){
    partition_table_service::InvalidPagesRequest invalid_request;
    partition_table_service::PartitionTableService_Stub partition_table_stub(get_pagetable_channel());
    partition_table_service::InvalidResponse* invalid_response = new partition_table_service::InvalidResponse();
    brpc::Controller cntl;
    auto table_size = node_->meta_manager_->GetTableNum();
    for(int table_id = 0; table_id < table_size; table_id++){
        uint64_t page_size = get_partitioned_size(table_id);
        int start_pageid = node_->node_id * page_size;
        int end_pageid = (node_->node_id + 1) * page_size;
        for(int page_id = start_pageid; page_id < end_pageid; page_id++){
            if(node_->local_page_lock_tables[table_id]->GetLock(page_id)->GetDirty() == true){
                auto p = invalid_request.add_page_id();
                p->set_table_id(table_id);
                p->set_page_no(page_id);                
            }
        }
    }
    invalid_request.set_node_id(node_->getNodeID());
    partition_table_stub.InvalidPages(&cntl, &invalid_request, invalid_response, NULL);
    for(int i=0; i<invalid_request.page_id_size(); i++){
        table_id_t table_id = invalid_request.page_id(i).table_id();
        page_id_t page_no = invalid_request.page_id(i).page_no();
        // 释放脏标记
        node_->local_page_lock_tables[table_id]->GetLock(page_no)->SetDirty(false);
    }
    delete invalid_response;
    return;
}

void ComputeServer::InitTableNameMeta(){
    if(WORKLOAD_MODE == 0){
        table_name_meta.resize(2);
        table_name_meta[0] = "../storage_server/smallbank_savings";
        table_name_meta[1] = "../storage_server/smallbank_checking";
    }
    else if(WORKLOAD_MODE == 1){
        table_name_meta.resize(11);
        table_name_meta[0] = "../storage_server/TPCC_warehouse";
        table_name_meta[1] = "../storage_server/TPCC_district";
        table_name_meta[2] = "../storage_server/TPCC_customer";
        table_name_meta[3] = "../storage_server/TPCC_customerhistory";
        table_name_meta[4] = "../storage_server/TPCC_ordernew";
        table_name_meta[5] = "../storage_server/TPCC_order";
        table_name_meta[6] = "../storage_server/TPCC_orderline";
        table_name_meta[7] = "../storage_server/TPCC_item";
        table_name_meta[8] = "../storage_server/TPCC_stock";
        table_name_meta[9] = "../storage_server/TPCC_customerindex";
        table_name_meta[10] = "../storage_server/TPCC_orderindex";
    }
}

Page* ComputeServer::rpc_fetch_page_from_storage(table_id_t table_id, page_id_t page_id){    
    storage_service::StorageService_Stub storage_stub(get_storage_channel());
    storage_service::GetPageRequest request;
    storage_service::GetPageResponse response;
    auto page_id_pb = request.add_page_id();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_name(table_name_meta[table_id]);
    brpc::Controller cntl;
    storage_stub.GetPage(&cntl, &request, &response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to fetch page " << page_id << " from remote storage server";
    }
    assert(response.data().size() == PAGE_SIZE);
    return node_->getBufferPoolByIndex(table_id)->GetPage(page_id);
}

void ComputeServer::FlushRPCDone(bufferpool_service::FlushPageResponse* response, brpc::Controller* cntl) {
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<bufferpool_service::FlushPageResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "FlushRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::InvalidRPCDone(partition_table_service::InvalidResponse* response, brpc::Controller* cntl) {
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<partition_table_service::InvalidResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::LazyReleaseRPCDone(page_table_service::PAnyUnLockResponse* response, brpc::Controller* cntl){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<page_table_service::PAnyUnLockResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::PSlockRPCDone(page_table_service::PSLockResponse* response, brpc::Controller* cntl, std::atomic<bool>* finish){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    // std::unique_ptr<page_table_service::PSLockResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        *finish = true;
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::PXlockRPCDone(page_table_service::PXLockResponse* response, brpc::Controller* cntl, std::atomic<bool>* finish){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    // std::unique_ptr<page_table_service::PXLockResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    // std::unique_ptr<bool> finish_guard(finish);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        *finish = true;
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}
