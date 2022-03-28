#include "raft_ha.h"
#include "kl_mentain/log.h"
#include "stdio.h"
#include "sys/time.h"
#include "zettalib/tool_func.h"
#include <string>

std::string raft_group_member_init_config;
std::string cluster_mgr_tmp_data_path;
int64_t raft_brpc_port;
extern std::string local_ip;

void NodeHaClosure::Run() {
  if (status().ok()) {
    return;
  }
  syslog(Logger::ERROR, "NodeHaCloosure: %s", status().error_cstr());
}

bool NodeHa::Launch() {

  server_ = new brpc::Server();

  if (braft::add_service(server_, raft_brpc_port) != 0) {
    setErr("add braft server failed");
    return false;
  }

  brpc::ServerOptions *server_options = new brpc::ServerOptions();
  server_options->idle_timeout_sec = -1;
  if (server_->Start(raft_brpc_port, server_options) != 0) {
    setErr("start raft server on port %d faild", raft_brpc_port);
    return false;
  }

  butil::ip_t ip_;
  butil::str2ip(local_ip.c_str(), &ip_);
  butil::EndPoint ep(ip_, raft_brpc_port);
  braft::NodeOptions options;
  if (options.initial_conf.parse_from(raft_group_member_init_config) != 0) {
    setErr("Faild to parse raft group initial configuration: %s",
           raft_group_member_init_config.c_str());
    return false;
  }
  options.election_timeout_ms = 5000;
  options.fsm = this;
  options.node_owns_fsm = false;
  options.snapshot_interval_s = 60;
  char buffer[1024];
  sprintf(buffer, "local://%s/cluster_mgr_raft",
          cluster_mgr_tmp_data_path.c_str());
  std::string prefix(buffer);
  options.log_uri = prefix + "/log";
  options.raft_meta_uri = prefix + "/raft_meta";
  options.snapshot_uri = prefix + "/snapshot";
  options.disable_cli = true;
  node_ = new braft::Node(raft_group_member_init_config, braft::PeerId(ep));
  if (node_->init(options) != 0) {
    setErr("Fail to init raft node");
    delete node_;
    return false;
  }
  return true;
}

int NodeHa::run() {
  while (m_state) {
    if (is_leader()) {
      struct timeval tv;
      gettimeofday(&tv, nullptr);

      butil::IOBuf data;
      data.append(&tv.tv_sec, sizeof(tv.tv_sec));
      braft::Task task;
      task.data = &data;
      task.done = new NodeHaClosure(tv.tv_sec);
      node_->apply(task);
    }
    sleep(1);
  }
  return 0;
}

void NodeHa::on_apply(braft::Iterator &iter) {

  for (; iter.valid(); iter.next()) {

    braft::AsyncClosureGuard closure(iter.done());
    //  std::lock_guard<std::mutex> guard(mutex_);
    if (iter.done()) {
      // if iter.done() is not nullptr means task was proposed locally
      NodeHaClosure *c = dynamic_cast<NodeHaClosure *>(iter.done());
      timestamp_repl_ = c->timestamp_;
    } else {
      time_t va;
      iter.data().fetch((void *)(&va), sizeof(va));
      timestamp_repl_ = va;
    }
  }
  return;
}
void NodeHa::on_leader_start(int64_t term) {
  leader_term_.store(term, butil::memory_order_release);
  LOG(INFO) << "Node becomes leader";
}
void NodeHa::on_leader_stop(const butil::Status &status) {
  leader_term_.store(-1, butil::memory_order_release);
  LOG(INFO) << "Node stepped down : " << status;
}
void NodeHa::on_shutdown() { LOG(INFO) << "This node is down"; }

void NodeHa::on_error(const ::braft::Error &e) {
  LOG(ERROR) << "Met raft error " << e;
}
void NodeHa::on_configuration_committed(const ::braft::Configuration &conf) {
  LOG(INFO) << "Configuration of this group is " << conf;
}
void NodeHa::on_stop_following(const ::braft::LeaderChangeContext &ctx) {
  LOG(INFO) << "Node stops following " << ctx;
}
void NodeHa::on_start_following(const ::braft::LeaderChangeContext &ctx) {
  LOG(INFO) << "Node start following " << ctx;
}
bool NodeHa::is_leader() const {
  return leader_term_.load(butil::memory_order_acquire) > 0;
}
// Shut this node down.
void NodeHa::shutdown() {
  if (node_) {
    node_->shutdown(NULL);
  }
}
// Blocking this thread until the node is eventually down.
void NodeHa::join() {
  if (node_) {
    node_->join();
  }
}
