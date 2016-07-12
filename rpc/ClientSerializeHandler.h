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

#include "protocols/test.pb.h"

// Do some serialization / deserialization using thrift.
// A real rpc server would probably use generated client/server stubs
class ClientSerializeHandler : public wangle::Handler<
  std::unique_ptr<folly::IOBuf>, test::BonkResponse,
  test::BonkRequest, std::unique_ptr<folly::IOBuf>> {
 public:
  virtual void read(Context* ctx, std::unique_ptr<folly::IOBuf> msg) override {
    test::BonkResponse received;
    if (!received.ParseFromString(msg->moveToFbString().toStdString())) {
      // Looks not necessary to have special error handling here. 
      // The caller, service in RpcClient, could have a default function
      // for the empty BonkResponse.
      // TODO how to throw exception?
      std::cerr << "failed to parse response" << std::endl;
    }
    // TODO add a sleep to simulate read at server down.
    ctx->fireRead(received);
  }

  virtual void readException(Context* ctx, folly::exception_wrapper e) override {
      std::cout << "readException " << exceptionStr(e) << std::endl;
      close(ctx);
  }

  virtual void readEOF(Context* ctx) override {
      std::cout << "EOF received :(" << std::endl;
      close(ctx);
  }

  virtual folly::Future<folly::Unit> write(
    Context* ctx, test::BonkRequest b) override {

    std::string out;
    if (!b.SerializeToString(&out)) {
      return folly::makeFuture<folly::Unit>(
              folly::make_exception_wrapper<std::runtime_error>(
                  "failed to serialize request"));
    }

    return ctx->fireWrite(folly::IOBuf::copyBuffer(out));
  }

  // TODO how does writeException() in Handler.h get triggered?
  virtual folly::Future<folly::Unit> writeException(
          Context* ctx, folly::exception_wrapper e) override {
      std::cout << "writeException " << exceptionStr(e) << std::endl;
  }

 // NOTE: could not declare the atomic variable, can't pass compile, 
 //       don't know why yet.
 //private:
  //std::atomic<bool> closed;
};
