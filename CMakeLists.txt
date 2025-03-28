cmake_minimum_required(VERSION 3.10)

project(Chimera)

option(LINK_SO "Whether are linked dynamically" ON)

execute_process(
    COMMAND bash -c "find ${PROJECT_SOURCE_DIR}/ -type d -regex \".*output/include$\" | head -n1 | xargs dirname | tr -d '\n'"
    OUTPUT_VARIABLE OUTPUT_PATH
)

set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result -fPIC")

set(CMAKE_CXX_STANDARD 17)

# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")

include(FindThreads)
include(FindProtobuf)

# set protobuf path
set(PROTO_DIR "${PROJECT_SOURCE_DIR}/core")
# find all proto files
file(GLOB_RECURSE PROTO_FILES "${PROTO_DIR}/**/*.proto")
# compile proto files
foreach(PROTO_FILE ${PROTO_FILES})
    message("-- Find proto file: ${PROTO_FILE}")
    get_filename_component(PROTO_PATH "${PROTO_FILE}" PATH)
    get_filename_component(PROTO_NAME "${PROTO_FILE}" NAME_WE)
    set(PROTO_OUTPUT "${PROTO_PATH}/${PROTO_NAME}.pb.cc")
    # message("protoc --cpp_out=${PROTO_PATH} --proto_path=${PROTO_PATH} ${PROTO_FILE}")
    execute_process(
        COMMAND "protoc" "--cpp_out=${PROTO_PATH}" "--proto_path=${PROTO_PATH}" "${PROTO_FILE}"
    )
endforeach()

# Search for libthrift* by best effort. If it is not found and brpc is
# compiled with thrift protocol enabled, a link error would be reported.
find_library(THRIFT_LIB NAMES thrift)
if (NOT THRIFT_LIB)
    set(THRIFT_LIB "")
endif()

find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h PATHS ${CMAKE_CURRENT_SOURCE_DIR})
message(STATUS "BRPC_INCLUDE_PATH: ${BRPC_INCLUDE_PATH}")

if(LINK_SO)
    find_library(BRPC_LIB NAMES brpc)
else()
    find_library(BRPC_LIB NAMES libbrpc.a brpc)
endif()
if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()
include_directories(${BRPC_INCLUDE_PATH})

find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
include_directories(${GFLAGS_INCLUDE_PATH})

execute_process(
    COMMAND bash -c "grep \"namespace [_A-Za-z0-9]\\+ {\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $2}' | tr -d '\n'"
    OUTPUT_VARIABLE GFLAGS_NS
)
if(${GFLAGS_NS} STREQUAL "GFLAGS_NAMESPACE")
    execute_process(
        COMMAND bash -c "grep \"#define GFLAGS_NAMESPACE [_A-Za-z0-9]\\+\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $3}' | tr -d '\n'"
        OUTPUT_VARIABLE GFLAGS_NS
    )
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    include(CheckFunctionExists)
    CHECK_FUNCTION_EXISTS(clock_gettime HAVE_CLOCK_GETTIME)
    if(NOT HAVE_CLOCK_GETTIME)
        set(DEFINE_CLOCK_GETTIME "-DNO_CLOCK_GETTIME_IN_MAC")
    endif()
endif()

set(CMAKE_CPP_FLAGS "${DEFINE_CLOCK_GETTIME} -DGFLAGS_NS=${GFLAGS_NS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} -DDEBUG -O0 -D__const__=__unused__ -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")
set(CMAKE_BUILD_TYPE Debug)

add_compile_options(-Wno-deprecated-copy -Wno-class-memaccess)

find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()
include_directories(${LEVELDB_INCLUDE_PATH})

find_package(OpenSSL)
include_directories(${OPENSSL_INCLUDE_DIR})

set(DYNAMIC_LIB
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${PROTOBUF_LIBRARIES}
    ${LEVELDB_LIB}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${OPENSSL_SSL_LIBRARY}
    ${THRIFT_LIB}
    dl
    )

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -DDEBUG -g")

include_directories(./
                    core
                    thirdparty
                    workload
)

set(CONFIG_SRC config.cc)
add_library(global STATIC ${CONFIG_SRC})

add_subdirectory(core)

add_subdirectory(workload)

add_subdirectory(remote_server)

add_subdirectory(compute_server)

add_subdirectory(storage_server)