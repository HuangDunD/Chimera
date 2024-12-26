add_subdirectory(page_transfer_test)

add_executable(coro_test coro_test.cc)
target_link_libraries(coro_test core ${DYNAMIC_LIB} ${BRPC_LIB})