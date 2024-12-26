#include <brpc/server.h>
#include "transfer.pb.h"
#include <gflags/gflags.h>
#include <butil/logging.h> 

namespace page_transfer_service {
class PageTransferServiceImpl : public PageTransferService {
public:
    virtual void GetPage(::google::protobuf::RpcController* controller,
                        const ::page_transfer_service::GetPageRequest* request,
                        ::page_transfer_service::GetPageResponse* response,
                        ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        for(int i=0; i<request->page_cnt(); i++){
            char* page = new char[4096];
            response->add_page_data(page, 4096);
        }
        return;
    }
};
};

int main(){
    brpc::Server server;
    page_transfer_service::PageTransferServiceImpl service;
    if(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0){
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    brpc::ServerOptions options;
    if(server.Start(34000, &options) != 0){
        LOG(ERROR) << "Fail to start server";
        return -1;
    }
    server.RunUntilAskedToQuit();
}