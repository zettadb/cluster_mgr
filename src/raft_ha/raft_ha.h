/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

// raft_ha contains the HA infrastructure of the cluster_mgr.
// All the infras is Implemented by the using of Braft which is compatiable with
// BRPC inherently.
//
// By default, the port which listen by Braft service can be same with the
// existing brpc::Server, but we don't agree with it.
//
// Thus, we seperate the port listen by normal server and Braft Server
// respectively. Braft instance comunicate each other by the port which is
// specified in the cluster_mgr.cnf
//
// In the HA mode, cluster_mgr work flow is under the arrangement described
// below:
//    1. Start braft group
//    2. Each cluster_mgr instance just loop and wait until the leader role is
//    confirmed
//    3. If current address is same with the leader's address, then go and
//    execute the rest of the procedure,otherwise, cluster_mgr is just loop and
//    wait because current cluster_mgr is not the leader

#include "zettalib/errorcup.h"
#include "zettalib/zthread.h"
#include <braft/protobuf_file.h> // braft::ProtoBufFile
#include <braft/raft.h>          // braft::Node braft::StateMachine
#include <braft/storage.h>       // braft::SnapshotWriter
#include <braft/util.h>          // braft::AsyncClosureGuard
#include <brpc/controller.h>     // brpc::Controller
#include <brpc/server.h>         // brpc::Server
#include <mutex>

class NodeHaClosure : public braft::Closure {
public:
  NodeHaClosure(time_t &timestamp) : timestamp_(timestamp){};
  ~NodeHaClosure(){};
  void Run() override;
  time_t timestamp_;
};
class NodeHa : public braft::StateMachine,
               public kunlun::ZThread,
               public kunlun::ErrorCup {
public:
  NodeHa() : server_(nullptr), node_(nullptr), leader_term_(-1){};
  ~NodeHa() { delete node_; };
  bool Launch();

  // braft::StateMachine
  void on_apply(braft::Iterator &iter) override;
  // kunlun::Zthread
  int run() override;

  void on_leader_start(int64_t) override;
  void on_leader_stop(const butil::Status &) override;
  void on_shutdown() override;
  void on_error(const ::braft::Error &) override;
  void on_configuration_committed(const ::braft::Configuration &) override;
  void on_stop_following(const ::braft::LeaderChangeContext &ctx) override;
  void on_start_following(const ::braft::LeaderChangeContext &ctx) override;
  // end of @braft::StateMachine
  bool is_leader() const;
  // Shut this node down.
  void shutdown();
  // Blocking this thread until the node is eventually down.
  void join();

private:
  brpc::Server *server_;
  // facility of the raft group
  braft::Node *volatile node_;
  // Identify the commited request_id
  time_t timestamp_repl_;
  // To avoid the ABA problem
  butil::atomic<int64_t> leader_term_;
  std::mutex mutex_;
};
