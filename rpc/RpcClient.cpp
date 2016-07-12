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
#include <wangle/service/ClientDispatcher.h>
#include <wangle/bootstrap/ClientBootstrap.h>
#include <wangle/channel/AsyncSocketHandler.h>
#include <wangle/codec/LengthFieldBasedFrameDecoder.h>
#include <wangle/codec/LengthFieldPrepender.h>
#include <wangle/channel/EventBaseHandler.h>
#include <wangle/concurrent/CPUThreadPoolExecutor.h>
#include <folly/AtomicUnorderedMap.h>
#include <folly/AtomicHashMap.h>

#include <glog/logging.h>

#include "rpc/ClientSerializeHandler.h"
#include "protocols/test.pb.h"

using namespace folly;
using namespace wangle;
using namespace std;
using namespace test;

using SerializePipeline = wangle::Pipeline<IOBufQueue&, BonkRequest>;

DEFINE_int32(port, 8080, "test server port");
DEFINE_string(host, "::1", "test server address");
DEFINE_bool(sleep, false, "sleep for request and response");
DEFINE_int32(outthreads, 2, "number threads send the request");
DEFINE_int32(inthreads, 1, "number threads handle the response");

class RpcPipelineFactory : public PipelineFactory<SerializePipeline> {
 public:
  SerializePipeline::Ptr newPipeline(
      shared_ptr<AsyncTransportWrapper> sock) override {
    auto pipeline = SerializePipeline::create();
    pipeline->addBack(AsyncSocketHandler(sock));
    // ensure we can write from any thread
    pipeline->addBack(EventBaseHandler());
    pipeline->addBack(LengthFieldBasedFrameDecoder());
    pipeline->addBack(LengthFieldPrepender());
    pipeline->addBack(ClientSerializeHandler());
    pipeline->finalize();

    return pipeline;
  }
};

// Client multiplex dispatcher.  Uses Bonk.id as request ID
class BonkMultiplexClientDispatcher
    : public ClientDispatcherBase<SerializePipeline, BonkRequest, BonkResponse> {
 public:
  void read(Context* ctx, BonkResponse in) override {
    VLOG(3) << " thread " << std::this_thread::get_id()
            << ", handle resp " << in.id();
    auto search = requests_.find(in.id());
    CHECK(search != requests_.end());
    auto p = move(search->second);
    requests_.erase(in.id());
    p.setValue(in);

    if (FLAGS_sleep) {
        // add sleep to expirement which thread handles the response
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  Future<BonkResponse> operator()(BonkRequest arg) override {
    VLOG(3) << "BonkMultiplexClientDispatcher thread " << std::this_thread::get_id()
            << ", handle req " << arg.id() 
            << ", request map size " << requests_.size();

    auto& p = requests_[arg.id()];
    auto f = p.getFuture();
    p.setInterruptHandler([arg, this](const folly::exception_wrapper& e) {
            this->requests_.erase(arg.id());
            });
    
    this->pipeline_->write(arg)
      .onError([arg, this](std::exception const& e) {
          LOG(ERROR) << "BonkMultiplexClientDispatcher thread " << std::this_thread::get_id()
                     << " write fail, done " << arg.id() << std::endl
                     << "\tex: " << exceptionStr(e) 
                     << std::endl;

          const folly::AsyncSocketException* fe = 
            dynamic_cast<const folly::AsyncSocketException*>(&e);
          if (fe != NULL) {
            this->requests_[arg.id()].setException(*fe);
            if (isConnectionIssue(fe)) {
              VLOG(3) << "set available to false";
              available.exchange(false);
            }
          } else {
            this->requests_[arg.id()].setException(e);
          }

          this->requests_.erase(arg.id());
      });

    VLOG(3) << "BonkMultiplexClientDispatcher request map size " << requests_.size();

    if (FLAGS_sleep) {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    return f;
  }

  virtual bool isAvailable() override {
    VLOG(3) << " call isAvailable " << available;
    return available;
  }

  // Print some nice messages for close
  virtual Future<Unit> close() override {
    VLOG(3) << "Channel closed";
    return ClientDispatcherBase::close();
  }

  virtual Future<Unit> close(Context* ctx) override {
    VLOG(3) << "Channel closed";
    return ClientDispatcherBase::close(ctx);
  }

 private:
  bool isConnectionIssue(const folly::AsyncSocketException* fe) {
    switch (fe->getType()) {
        case folly::AsyncSocketException::NOT_OPEN:
        case folly::AsyncSocketException::END_OF_FILE:
        //case folly::AsyncSocketException::TIMED_OUT:
        case folly::AsyncSocketException::INVALID_STATE:
        case folly::AsyncSocketException::COULD_NOT_BIND:
            return true;
        default:
            return false;
    }
  }

 private:
  // TODO is unordered_map safe for concurrent put/erase?
  unordered_map<int32_t, Promise<BonkResponse>> requests_;
  std::atomic<bool> available{true};
};


class ClientServiceManager {
  private:
    class ClientService {
      public:
        typedef ExecutorFilter<BonkRequest, BonkResponse> BonkService;

        ClientBootstrap<SerializePipeline> client_;
        shared_ptr<BonkService> bonkSvc_;

        ClientService()
          : ClientService(make_shared<IOThreadPoolExecutor>(1)) {
          // TODO one IOTP per instance, could not set connPool_ to group.
          //      checked Netty, netty could easily use the same TP to create 
          //      2 connections to the same server.
        }

        // @throws AsyncSocketException
        ClientService(shared_ptr<IOThreadPoolExecutor> connPool) {
          client_.group(connPool);
          client_.pipelineFactory(make_shared<RpcPipelineFactory>());
        }

        // @throws AsyncSocketException
        void connect(string host, int32_t port,
            shared_ptr<CPUThreadPoolExecutor> reqPool) {
          // connect().get() may throw AsyncSocketException
          // TODO make connect async
          SerializePipeline* pipeline = client_.connect(SocketAddress(host, port)).get();
          VLOG(5) << "connected to " << host << " " << port;
          auto dispatcher = std::make_shared<BonkMultiplexClientDispatcher>();
          dispatcher->setPipeline(pipeline);

          bonkSvc_ = std::make_shared<BonkService>(reqPool, dispatcher);
        }

        bool isAvailable() {
          return bonkSvc_->isAvailable();
        }
    };

  public:
    ClientServiceManager() {
    }

    // write the request to server
    Future<BonkResponse> write(string host, int32_t port, 
                               shared_ptr<BonkRequest> request) {
      string skey = host + std::to_string(port);

      // put the request to requests_ map
      shared_ptr<Promise<BonkResponse>> p = 
        std::make_shared<Promise<BonkResponse>>();
      shared_ptr<ReqItem> reqItem = 
        std::make_shared<ReqItem>(request, p, host, port, skey);
      requests_->emplace(request->id(), reqItem);

      try {
        // get the communication service
        std::shared_ptr<ClientService> service;
        auto iter = services_->find(skey);
        if (iter == services_->cend()) {
          // create service to server
          service = createService(host, port, skey);
        } else {
          service = iter->second;
        }

        // send request to server
        send2Service(service, request);
      } catch (const folly::AsyncSocketException& e) {
        p->setException(e);
        requests_->erase(request->id());
      }

      VLOG(3) << " request map size " << requests_->size();

      return p->getFuture();
    }

  private:
    void send2Service(shared_ptr<ClientService> service,
                      shared_ptr<BonkRequest> request) {
      // send the request to service
      (*(service->bonkSvc_))(*request)
        .then([request, this](BonkResponse response) {
            VLOG(3) << "ClientServiceManager thread " << std::this_thread::get_id()
                    << " success write, done " << request->id();

            shared_ptr<ReqItem> reqItem = 
              this->requests_->find(request->id())->second;
            reqItem->p_->setValue(response);

            this->requests_->erase(request->id());
        })
        .onError([request, this](std::exception const& e) {
            LOG(ERROR) << "ClientServiceManager thread " << std::this_thread::get_id()
                       << " write fail, done " << request->id()
                       << "\tex: " << exceptionStr(e);
            
            shared_ptr<ReqItem> reqItem =
              this->requests_->find(request->id())->second;

            if (reqItem->retryCnt_ < MAX_REQUEST_RETRY_COUNT) {
              // reconnect and retry

              try {
                // TODO move to async sleep
                std::this_thread::sleep_for(std::chrono::seconds(1));
                reqItem->retryCnt_++;

                shared_ptr<ClientService> service = 
                  recreateService(reqItem->host_, reqItem->port_, reqItem->skey_);

                send2Service(service, request);

              } catch (const folly::AsyncSocketException& e) {
                LOG(ERROR) << "reconnects failed " << exceptionStr(e);
                reqItem->p_->setException(e);
                this->requests_->erase(request->id());
              }
            } else {
              // throw exception to caller
              const folly::AsyncSocketException* fe = 
                dynamic_cast<const folly::AsyncSocketException*>(&e);
              if (fe != NULL) {
                reqItem->p_->setException(*fe);
              } else {
                reqItem->p_->setException(e);
              }

              this->requests_->erase(request->id());
            }

        });
    }

    shared_ptr<ClientService> recreateService(string host, 
        int32_t port, string skey) {
      // lock to avoid potential concurrent connections to the same host
      std::lock_guard<std::mutex> guard(serviceMutex_);

      // get the latest service and check if someone else already reconnects
      shared_ptr<ClientService> service = services_->find(skey)->second;
      if (!service->isAvailable()) {
        // setup new connection
        VLOG(3) << " try to reconnect to " << host << " " << port;

        shared_ptr<ClientService> newService = make_shared<ClientService>();
        newService->connect(host, port, reqPool_);
        
        VLOG(3) << " reconnected to " << host << " " << port;
        //service = newService;
        //VLOG(3) << " reset service to the new one";
        services_->erase(skey);
        services_->emplace(skey, newService);
        return newService;
      }

      return service;
    }

    shared_ptr<ClientService> createService(string host, 
        int32_t port, string skey) {
      // lock to avoid potential concurrent connections to the same host
      std::lock_guard<std::mutex> guard(serviceMutex_);

      // check if someone else creates the service at the same time
      auto iter = services_->find(skey);
      if (iter != services_->cend()) {
        return iter->second;
      }

      shared_ptr<ClientService> service = make_shared<ClientService>();
      service->connect(host, port, reqPool_);

      VLOG(3) << "connected to " << host << " " << port;

      // should be just an insert, as we have lock to protect concurrent 
      // connection creations.
      return services_->emplace(skey, service).first->second;
    }

  private:
    static const int32_t MAX_REQUEST_RETRY_COUNT = 3;

    // The thread in ClientBootstrap.group is used as the EventBase of socket.
    // The responses of the same socket will be handled by a single thread 
    // in group, that the socket is assigned to. So the responses of 
    // different sockets could be handled by different threads.
    // TODO could not set ClientBootstrap.group with connPool_, failed with:
    //      RpcClient: io/async/AsyncSocket.cpp:558: virtual void folly::AsyncSocket::setReadCB(folly::AsyncTransportWrapper::ReadCallback*): Assertion `eventBase_->isInEventBaseThread()' failed.
    shared_ptr<IOThreadPoolExecutor> connPool_ = 
      make_shared<IOThreadPoolExecutor>(FLAGS_inthreads);

    // The shared executor for all coming requests
    std::shared_ptr<CPUThreadPoolExecutor> reqPool_ = 
      std::make_shared<CPUThreadPoolExecutor>(FLAGS_outthreads);

    // The service instances to different processes.
    // NOTE: further check how folly uses maxSize and maxLoadFactor 
    //typedef AtomicUnorderedInsertMap<string, 
    //          shared_ptr<ClientService>> ClientServiceMap;
    //std::unique_ptr<ClientServiceMap> services_{new ClientServiceMap(200, 0.8f)};
    typedef std::map<string, shared_ptr<ClientService>> ClientServiceMap;
    std::unique_ptr<ClientServiceMap> services_{new ClientServiceMap()};
    // lock to serialize the connection creations
    // TODO try folly::SharedMutex
    std::mutex serviceMutex_;

    // The outgoing requests map
    struct ReqItem {
      shared_ptr<BonkRequest> req_;
      shared_ptr<Promise<BonkResponse>> p_;
      int32_t retryCnt_;
      string host_;
      int32_t port_;
      string skey_;

      ReqItem(shared_ptr<BonkRequest> req, 
              shared_ptr<Promise<BonkResponse>> p,
              string host,
              int32_t port,
              string skey)
        : req_(req), p_(p), retryCnt_(0),
          host_(host), port_(port), skey_(skey) {
      }
    };
    // TODO use Intel TBB.
    // could not use folly::AtomicHashMap, the space of erased elements are
    // tombstoned forever.
    typedef std::map<uint64_t, shared_ptr<ReqItem>> ReqMap;
    // NOTE: further check folly AtomicHashMap finalSizeEst
    std::unique_ptr<ReqMap> requests_{new ReqMap()};
      
};

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  ClientServiceManager service;

  try {
    while (true) {
      cout << "Input int and string" << endl;

      shared_ptr<BonkRequest> request = std::make_shared<BonkRequest>();
      
      int32_t id;
      cin >> id;
      request->set_id(id);


      string name;
      cin >> name;
      request->set_name(name);

      service.write(FLAGS_host, FLAGS_port, request)
          .then([request](BonkResponse response) {
              CHECK(request->id() == response.id());
              cout << "Caller thread " << std::this_thread::get_id()
                   << " get BonkResponse " << response.id() 
                   << " " << response.name() << std::endl;
          })
          .onError([request](std::exception const& e) {
              LOG(ERROR) << "Caller thread " << std::this_thread::get_id()
                         << " write fail " << request->id()
                         << "\tex: " << exceptionStr(e);
          });

      if (FLAGS_sleep) {
          shared_ptr<BonkRequest> r2 = std::make_shared<BonkRequest>();
          r2->set_id(id + 1);
          r2->set_name(name);

          service.write(FLAGS_host, FLAGS_port, r2)
              .then([r2](BonkResponse response) {
                  CHECK(r2->id() == response.id());
                  VLOG(3) << "Caller thread " << std::this_thread::get_id()
                          << " get BonkResponse " << response.id() 
                          << " " << response.name();
              })
              .onError([r2](std::exception const& e) {
                  LOG(ERROR) << "Caller thread " << std::this_thread::get_id()
                             << " write fail " << r2->id()
                             << "\tex: " << exceptionStr(e);
              });


      }
    }
  } catch (const exception& e) {
    LOG(ERROR) << exceptionStr(e);
  }

  return 0;
}
