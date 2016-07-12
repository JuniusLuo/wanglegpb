/*
 *  Copyright (c) 2016, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include <gflags/gflags.h>

#include <wangle/service/Service.h>
#include <wangle/service/ExpiringFilter.h>
#include <wangle/service/ExecutorFilter.h>
#include <wangle/service/ServerDispatcher.h>
#include <wangle/bootstrap/ServerBootstrap.h>
#include <wangle/channel/AsyncSocketHandler.h>
#include <wangle/codec/LengthFieldBasedFrameDecoder.h>
#include <wangle/codec/LengthFieldPrepender.h>
#include <wangle/channel/EventBaseHandler.h>
#include <wangle/concurrent/CPUThreadPoolExecutor.h>

#include "rpc/ServerSerializeHandler.h"

using namespace folly;
using namespace wangle;

using SerializePipeline = wangle::Pipeline<IOBufQueue&, test::BonkResponse>;

DEFINE_int32(port, 8080, "test server port");
DEFINE_bool(sleep, false, "sleep before response");
DEFINE_int32(threads, 2, "number of threads handle response");

class RpcService : public Service<test::BonkRequest, test::BonkResponse> {
 public:
  virtual Future<test::BonkResponse> operator()(test::BonkRequest request) override {
    // Oh no, we got test::BonkRequested!  Quick, Bonk back
    std::cout << " thread " << std::this_thread::get_id()
              << " get BonkRequest " << request.id()
              << ", " << request.name() << std::endl;

    test::BonkResponse response;
    response.set_id(request.id());
    response.set_name("Stop saying " + request.name() + "!");
    if (!FLAGS_sleep) {
        return response;
    } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        return response;

        /* sleep override: ignore lint
         * useful for testing dispatcher behavior by hand
         */
        // Wait for a bit
        /*
        //return futures::sleep(std::chrono::seconds(request.id()))
        return futures::sleep(std::chrono::seconds(1))
            .then([request]() {
                    test::BonkResponse response;
                    response.set_id(request.id());
                    response.set_name("Stop saying " + request.name() + "!");
                    return response;
                    });
        */
    }
 
  }
};

class RpcPipelineFactory : public PipelineFactory<SerializePipeline> {
 public:
  SerializePipeline::Ptr newPipeline(
      std::shared_ptr<AsyncTransportWrapper> sock) {
    auto pipeline = SerializePipeline::create();
    pipeline->addBack(AsyncSocketHandler(sock));
    // ensure we can write from any thread
    pipeline->addBack(EventBaseHandler());
    pipeline->addBack(LengthFieldBasedFrameDecoder());
    pipeline->addBack(LengthFieldPrepender());
    pipeline->addBack(ServerSerializeHandler());
    // We could use a serial dispatcher instead easily
    // pipeline->addBack(SerialServerDispatcher<test::BonkRequest>(&service_));
    // Or a Pipelined Dispatcher
    // pipeline->addBack(PipelinedServerDispatcher<test::BonkRequest>(&service_));
    // TODO how about data transfer? like writing 2MB to storage server? check wangle file example
    pipeline->addBack(MultiplexServerDispatcher<test::BonkRequest, test::BonkResponse>(&service_));
    pipeline->finalize();

    return pipeline;
  }

 private:
  ExecutorFilter<test::BonkRequest, test::BonkResponse> service_{
      // thread pool to handle the coming requests
      std::make_shared<CPUThreadPoolExecutor>(FLAGS_threads),
      std::make_shared<RpcService>()};
};

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::cout << " port " << FLAGS_port 
            << " sleep " << FLAGS_sleep 
            << " threads " << FLAGS_threads 
            << std::endl;

  ServerBootstrap<SerializePipeline> server;
  server.childPipeline(std::make_shared<RpcPipelineFactory>());
  server.bind(FLAGS_port);
  server.waitForStop();

  return 0;
}
