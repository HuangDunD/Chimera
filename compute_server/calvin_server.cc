// Author: Chunyue Huang
// Copyright (c) 2024

#include "server.h"
#include "compute_server/worker/global.h"
#include "dtx/dtx.h"
#include "base/queue.h"
#include <vector>
#include <utility>

namespace calvin_service{
    // void CalvinServiceImpl::GetCalvinDataItem(::google::protobuf::RpcController* controller,
    //                    const ::calvin_service::GetCalvinDataItemRequest* request,
    //                    ::calvin_service::GetCalvinDataItemResponse* response,
    //                    ::google::protobuf::Closure* done){
    //     brpc::ClosureGuard done_guard(done);

    //     batch_id_t bid = request->batch_id();
    //     tx_id_t txn_id = request->txn_id();
    //     BenchDTX* dtx = nullptr;
    //     // 打印一段进来rpc的日志
    //     // LOG(FATAL) << "[FATAL] txn:" << txn_id << " get data item";
    //     while(true){
    //         calvin_txn_mutex->lock();
    //         if(calvin_txn_status->find({bid,txn_id})==calvin_txn_status->end()){
    //             // not find
    //             calvin_txn_mutex->unlock();
    //             usleep(1);
    //             continue;
    //         }else{
    //             dtx = calvin_txn_status->at({bid,txn_id});
    //             calvin_txn_mutex->unlock();
    //             break;
    //         }
    //     }
    //     assert(dtx != nullptr);
    //     response->set_node_id(machine_id);
    //     for (auto i : request->set_index()) {
    //         uint64_t key_id = i;
    //         if (key_id < dtx->dtx->read_only_set.size()) {
    //             while (!dtx->dtx->read_only_set[key_id].is_fetched){
    //                 usleep(1);
    //             }
    //             DataItem* item = dtx->dtx->read_only_set[key_id].item_ptr.get();
    //             ResItem* res = response->add_data();
    //             res->set_data(item, sizeof(DataItem));
    //             res->set_set_index(i);
    //         } else {
    //             key_id -= dtx->dtx->read_only_set.size();
    //             while (!dtx->dtx->read_write_set[key_id].is_fetched){
    //                 usleep(1);
    //             }
    //             DataItem* item = dtx->dtx->read_write_set[key_id].item_ptr.get();
    //             ResItem* res = response->add_data();
    //             res->set_data(item, sizeof(DataItem));
    //             res->set_set_index(i);
    //         }
    //     }
    //     return;
    // };

    void CalvinServiceImpl::SendCalvinDataItem(::google::protobuf::RpcController* controller,
                       const ::calvin_service::SendCalvinDataItemRequest* request,
                       ::calvin_service::SendCalvinDataItemResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);

        batch_id_t bid = request->batch_id();
        tx_id_t txn_id = request->txn_id();
        node_id_t node_id = request->node_id();
        uint64_t seed = request->seed();
        std::vector<std::pair<int, char*>> data_map;
        for (auto i : request->data()) {
            char* data = new char[sizeof(DataItem)];
            memcpy(data, i.data().c_str(), sizeof(DataItem));
            data_map.push_back({i.set_index(), data});
        }
        // 打印接收到哪个节点的哪个事务的信息
        CALVIN_LOG(INFO) << "Receive data from " << node_id << " for txn " << bid << "-"<< txn_id;
        queue->msg_enqueue(new rw_set_entry(node_id, bid, txn_id, data_map, seed));
        
        return;
    };
};