#include <brpc/channel.h>
#include <brpc/controller.h>
#include "transfer.pb.h"

int transfer_cnt = 1000;
int transfer_time = 1000;

int main(int argc, char* argv[]) {
    if(argc > 2) {
        transfer_cnt = atoi(argv[1]);
        transfer_time = atoi(argv[2]);
    }
    brpc::Channel channel;
    std::string remote_node = "10.77.110.146:34000";
    brpc::ChannelOptions options;
    if(channel.Init(remote_node.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to init channel"; 
    }

    page_transfer_service::PageTransferService_Stub stub(&channel);

    // 计时
    auto startTime = std::chrono::high_resolution_clock::now();
    // GetPage
    page_transfer_service::GetPageRequest request;
    page_transfer_service::GetPageResponse response;
    for(int i=0; i<transfer_time; i++){
        request.set_page_cnt(transfer_cnt);
        brpc::Controller cntl;
        stub.GetPage(&cntl, &request, &response, nullptr);
        if(cntl.Failed()) {
            LOG(ERROR) << "Fail to get page"; 
        }
    }

    // 计时
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime); // 毫秒

    std::cout << "Transfer " << transfer_cnt * transfer_time 
        << " pages, duration: " << duration.count() << "ms" << std::endl;
    return 0;
}