add_executable(transfer_server server.cc transfer.pb.cc)
target_link_libraries(transfer_server ${DYNAMIC_LIB} ${BRPC_LIB})

add_executable(transfer_client client.cc transfer.pb.cc)
target_link_libraries(transfer_client ${DYNAMIC_LIB} ${BRPC_LIB})