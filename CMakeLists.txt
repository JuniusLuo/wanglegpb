#  Copyright (c) 2014, Facebook, Inc.
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree. An additional grant
#  of patent rights can be found in the PATENTS file in the same directory.

cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_FLAGS "-std=c++0x -fPIC")
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

find_package(Wangle REQUIRED)
find_package(Protobuf REQUIRED)
find_package(Folly REQUIRED)
find_package(Boost REQUIRED COMPONENTS system thread)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${PROTOBUF_INCLUDE_DIR}
  ${WANGLE_INCLUDE_DIR}
  ${FOLLY_INCLUDE_DIR}
  ${INCLUDE_DIR}
)

# generated protobuf code files first
execute_process(COMMAND protoc protocols/test.proto --cpp_out=.)

# set c++ header file dirs
set(HEADER_DIRS
  protocols
  rpc
)

foreach(dir ${HEADER_DIRS})
  file(GLOB_RECURSE headers ${dir}/*.h)
  set(HEADERS
    ${HEADERS}
    ${headers})
endforeach()

# CMake recommends to list source files explicitly, 
# see discussion: http://stackoverflow.com/questions/1027247/best-way-to-specify-sourcefiles-in-cmake
set(SOURCES
  protocols/test.pb.cc
  #  rpc/RpcClient.cpp
  #  rpc/RpcServer.cpp
)

add_library(gpbRpc SHARED
  ${HEADERS}
  ${SOURCES}
)

target_link_libraries(gpbRpc
  ${WANGLE_LIBRARIES}
  ${PROTOBUF_LIBRARIES}
  ${FOLLY_LIBRARIES}
  ${Boost_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  -lglog
  -lgflags
  -latomic)

#install(TARGETS gpbRpc DESTINATION lib)
#foreach(dir ${HEADER_DIRS})
#  install(DIRECTORY ${dir} DESTINATION include/gpbRpc
#          FILES_MATCHING PATTERN "*.h")
#endforeach()


add_executable(RpcClient rpc/RpcClient.cpp)
target_link_libraries(RpcClient gpbRpc)

add_executable(RpcServer rpc/RpcServer.cpp)
target_link_libraries(RpcServer gpbRpc)

