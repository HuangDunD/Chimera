add_subdirectory(worker)

#add_executable(compute_node rpc_workload.cc rpc_run.cc server.cc gernerate_pageid.cc baseline_server.cc lazyrelease_server.cc phase_switch_server.cc delay_release_server.cc delay_fetch_server.cc)

add_executable(compute_server run.cc server.cc baseline_server.cc lazyrelease_server.cc phase_switch_server.cc delay_release_server.cc delay_fetch_server.cc twopc_server.cc twopc_fetch_server.cc calvin_server.cc calvin_fetch_server.cc single_server.cc leap_server.cc star_server.cc)

#target_link_libraries(compute_node global core ${DYNAMIC_LIB} ${BRPC_LIB})

target_link_libraries(compute_server worker global core ${DYNAMIC_LIB} ${BRPC_LIB})
