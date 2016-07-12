/*
 *  Copyright (c) 2016, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#pragma once

#include <wangle/channel/Handler.h>

#include <protocols/test.pb.h>

// Do some serialization / deserialization using protobuf.
class ServerSerializeHandler : public wangle::Handler<
  std::unique_ptr<folly::IOBuf>, test::BonkRequest,
  test::BonkResponse, std::unique_ptr<folly::IOBuf>> {
 public:
  virtual void read(Context* ctx, std::unique_ptr<folly::IOBuf> msg) override {
    test::BonkRequest received;
    if (!received.ParseFromString(msg->moveToFbString().toStdString())) {
      // Looks not necessary to have special error handling here. 
      // The next handler, ServerDispatcher, could have a default function
      // for the empty BonkRequest, which will return error response.
      std::cerr << "failed to parse BonkRequest" << std::endl;
    }
    ctx->fireRead(received);
  }

  virtual folly::Future<folly::Unit> write(
    Context* ctx, test::BonkResponse b) override {

    std::string out;
    if (!b.SerializeToString(&out)) {
      // Ideally better to directly return if serialization fails.
      // While, this just sends a noisy response to client and should 
      // only happen at the corner bug.
      std::cerr << "failed to serialize BonkResponse" << std::endl;
    }
    return ctx->fireWrite(folly::IOBuf::copyBuffer(out));
  }

};
