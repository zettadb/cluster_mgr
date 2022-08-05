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

#include <condition_variable>
#include "zettalib/errorcup.h"
#include <braft/protobuf_file.h> // braft::ProtoBufFile
#include <braft/raft.h>          // braft::Node braft::StateMachine
#include <braft/storage.h>       // braft::SnapshotWriter
#include <braft/util.h>          // braft::AsyncClosureGuard
#include <brpc/controller.h>     // brpc::Controller
#include <brpc/server.h>         // brpc::Server
#include <mutex>
#include "leveldb/db.h"
#include "proto/raft_msg.pb.h"
#include "generate_id.h"

typedef enum raft_state {
  RAFT_INIT,
  RAFT_MASTER,
  RAFT_SLAVE
} RaftState;

using namespace raft_ha;

typedef enum msg_operator {
  MSG_INSERT,
  MSG_DELETE,
} Msg_Operator;

class NodeHa;

class NodeHaClosure : public braft::Closure {
public:
  NodeHaClosure(NodeHa* nha, const raft_msg& msgpb) 
        : nha_(nha), msgpb_(msgpb), cb_(nullptr), cb_ctx_(nullptr) {
  }

  virtual ~NodeHaClosure(){}
  void set_cb(void (*cb)(bool, void *)) {
    cb_ = cb;
  }

  void set_cb_ctx(void *ctx) {
    cb_ctx_ = ctx;
  }

  const raft_msg& GetRaftMsg() const {
    return msgpb_;
  }
  void Run() override;

  void (*cb_)(bool, void *);
  void *cb_ctx_;

private:
  NodeHa* nha_;
  raft_msg msgpb_;
};

class SharedFD : public butil::RefCountedThreadSafe<SharedFD> {
public:
  explicit SharedFD(int fd) : fd_(fd) {}
  int fd() const {
    return fd_;
  }

private:
  friend class butil::RefCountedThreadSafe<SharedFD>;
  ~SharedFD() {
    if(fd_ >= 0) {
      while(1) {
        const int rc = close(fd_);
        if(rc == 0 || errno != EINTR)
          break;
      }
      fd_ = -1;
    }
  }
  int fd_;
};

typedef scoped_refptr<SharedFD> scoped_fd;

class NodeHa : public braft::StateMachine,
               public kunlun::ErrorCup {
public:
  NodeHa() : server_(nullptr), node_(nullptr), leader_term_(-1), cur_state_(RAFT_INIT) {}
  ~NodeHa() { delete node_; }
  bool Launch();

  // braft::StateMachine
  void on_apply(braft::Iterator &iter) override;
  void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) override;
  int on_snapshot_load(braft::SnapshotReader* reader) override;
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

public:
  void InsertMsg(const std::string& key, const std::string& msg);
  void DeleteMsg(const std::string& key);
  bool DealHaMsg(const raft_msg& msgpb);

  CGenerateId* GetGenerateId() {
    return gen_id_;
  }

  void DispatchJobs();

  void wait_state_change();
  void notify_state_change();
  void UpdateMetaClusterMgrState();
private:
  bool SyncMsgLeveldb(const std::string& key, const std::string& msg);
  void ReadMsgLeveldb();
  bool DeleteMsgLeveldb(const std::string& key);

  scoped_fd get_fd() const {
    BAIDU_SCOPED_LOCK(fd_mutex_);
    return fd_;
  }
private:
  brpc::Server *server_;
  // facility of the raft group
  braft::Node *volatile node_;
  // Identify the commited request_id
  //time_t timestamp_repl_;
  // To avoid the ABA problem
  butil::atomic<int64_t> leader_term_;
  mutable butil::Mutex fd_mutex_;
  scoped_fd fd_;
  
  std::mutex mutex_;                   
	std::condition_variable state_cv_; 
  RaftState cur_state_;
  leveldb::DB *leveldb_;
  CGenerateId* gen_id_;
};
