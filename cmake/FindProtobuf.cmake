#  Copyright (c) 2014, Facebook, Inc.
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree. An additional grant
#  of patent rights can be found in the PATENTS file in the same directory.
#
# - Try to find Protobuf
# This will define
# PROTOBUF_FOUND
# PROTOBUF_INCLUDE_DIR
# PROTOBUF_LIBRARIES

CMAKE_MINIMUM_REQUIRED(VERSION 2.8.7 FATAL_ERROR)

INCLUDE(FindPackageHandleStandardArgs)

FIND_LIBRARY(PROTOBUF_LIBRARY protobuf PATHS ${PROTOBUF_LIBRARYDIR})
FIND_PATH(PROTOBUF_INCLUDE_DIR "google/protobuf/any.h" PATHS ${PROTOBUF_INCLUDEDIR})

SET(PROTOBUF_LIBRARIES ${PROTOBUF_LIBRARY})

FIND_PACKAGE_HANDLE_STANDARD_ARGS(Protobuf
  REQUIRED_ARGS PROTOBUF_INCLUDE_DIR PROTOBUF_LIBRARIES)
