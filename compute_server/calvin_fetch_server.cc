// Author: Chunyue Huang
// Copyright (c) 2024

#include "server.h"
#include "dtx/dtx.h"
#include "base/queue.h"
// #include "compute_server/worker/global.h"

// static void CalvinReadRPCDone(calvin_service::GetCalvinDataItemResponse* response, brpc::Controller* cntl, std::vector<std::pair<int, char*>>* res_item, BenchDTX* dtx){
//     // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
//     // std::unique_ptr<page_table_service::PSLockResponse> response_guard(response);
//     std::unique_ptr<brpc::Controller> cntl_guard(cntl);
//     if (cntl->Failed()) {
//         LOG(ERROR) << "CalvinReadRPC failed";
//         // RPC失败了. response里的值是未定义的，勿用。
//     } else {
//         // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
//         for (auto item : response->data()) {
//             int idx = item.set_index();
//             char* data = new char[sizeof(DataItem)];
//             memcpy(data, item.data().c_str(), sizeof(DataItem));
//             dtx->dtx->FillRemoteReadWriteSet(idx, data);
//             // res_item->push_back(std::make_pair(idx,data));
//             // res_item->at(response->node_id()) = std::make_pair(idx,data);
//         }
//         // 将response的节点从事务的wait_ids中去除
//         dtx->dtx->wait_ids.erase(response->node_id());
//         if (dtx->dtx->wait_ids.empty()) {
//             dtx->stage = CalvinStages::WRITE;
//             queue->enqueue(dtx, dtx->bid, false);
//         }
//     }
//     // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
// }

// void ComputeServer::GetCalvinRemotedata(batch_id_t bid, tx_id_t txn_id, std::vector<std::vector<int>> idx, std::vector<std::pair<int, char*>>& res_item, BenchDTX* dtx) {
//     assert(SYSTEM_MODE == 8);
//     std::vector<brpc::CallId> cids;
//     // std::vector<calvin_service::GetCalvinDataItemResponse*> responses;
//     for(int i=0; i < g_node_cnt; i++) {
//         node_id_t node_id = i;
//         if (idx[i].empty()) continue;
//         calvin_service::GetCalvinDataItemRequest request;
//         calvin_service::GetCalvinDataItemResponse* response = new calvin_service::GetCalvinDataItemResponse();
//         calvin_service::CalvinService_Stub stub(&nodes_channel[node_id]);
//         request.set_txn_id(txn_id);
//         request.set_batch_id(bid);
//         for(auto item: idx[i]){
//             request.add_set_index(item);
//         }
//         brpc::Controller *cntl = new brpc::Controller();
//         bool tmp;
//         stub.GetCalvinDataItem(cntl, &request, response, 
//             brpc::NewCallback(CalvinReadRPCDone, response, cntl, &res_item, dtx));
//     }
//     return;
// }

void ComputeServer::SendCalvinRemotedata(batch_id_t bid, tx_id_t txn_id, std::vector<std::pair<int, char*>> data_map, BenchDTX* dtx) {
    assert(SYSTEM_MODE == 8);
    std::vector<brpc::CallId> cids;
    // std::vector<calvin_service::GetCalvinDataItemResponse*> responses;
    for(int i=0; i < g_node_cnt; i++) {
        node_id_t node_id = i;
        if (node_id == machine_id) continue;
        if (dtx->dtx->all_ids.find(i) == dtx->dtx->all_ids.end()) continue;
        calvin_service::SendCalvinDataItemRequest request;
        calvin_service::SendCalvinDataItemResponse response;
        calvin_service::CalvinService_Stub stub(&nodes_channel[node_id]);
        request.set_txn_id(txn_id);
        request.set_batch_id(bid);
        request.set_node_id(machine_id);
        request.set_seed(dtx->seed);
        for(auto item: data_map){
            calvin_service::ResItem* it = request.add_data();
            it->set_data(item.second, sizeof(DataItem));
            it->set_set_index(item.first);
        }
        brpc::Controller *cntl = new brpc::Controller();
        stub.SendCalvinDataItem(cntl, &request, &response, nullptr);
        // responses.push_back(response);
    }
    return;
}
