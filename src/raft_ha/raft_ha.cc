#include "raft_ha.h"
#include "zettalib/op_log.h"
#include "stdio.h"
#include "sys/time.h"
#include "zettalib/tool_func.h"
#include <string>
#include "butil/sys_byteorder.h"
#include "kl_mentain/shard.h"
#include "kl_mentain/sys.h"
#include "util_func/object_ptr.h"

using namespace kunlun;

std::string raft_group_member_init_config;
std::string cluster_mgr_tmp_data_path;
int64_t raft_brpc_port;
std::string raft_groupid = "kunlun_nodeha";
extern std::string local_ip;
//extern int64_t cluster_mgr_collection_port;
extern int64_t cluster_mgr_brpc_http_port;
extern int64_t prometheus_port_start;

void NodeHaClosure::Run() {
  std::unique_ptr<NodeHaClosure> self_guard(this);
  if (status().ok()) {
    return;
  }

  KLOG_ERROR("NodeHaClosure: {}", status().error_cstr());
  if(cb_)
    cb_(false, cb_ctx_);
}

bool NodeHa::Launch() {
  gen_id_ = new CGenerateId;

  server_ = new brpc::Server();

  if (braft::add_service(server_, raft_brpc_port) != 0) {
    setErr("add braft server failed");
    return false;
  }

  brpc::ServerOptions *server_options = new brpc::ServerOptions();
  server_options->idle_timeout_sec = -1;
  if (server_->Start(raft_brpc_port, server_options) != 0) {
    setErr("start raft server on port %ld faild", raft_brpc_port);
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
  options.disable_cli = false;
  char buffer[1024];
  sprintf(buffer, "local://%s/cluster_mgr_raft",
          cluster_mgr_tmp_data_path.c_str());
  std::string prefix(buffer);
  //snapshot save
  std::string ss_data = cluster_mgr_tmp_data_path+"/cluster_mgr_raft/ss_data";
  int fd = open(ss_data.c_str(), O_CREAT | O_RDWR, 0644);
  if(fd < 0) {
    setErr("open snapshot data: %s failed: %d", ss_data.c_str(), errno);
    return false;
  }
  fd_ = new SharedFD(fd);

  //leveldb init
  leveldb::Options oOptions;
  oOptions.create_if_missing = true;
  leveldb::Status oStatus = leveldb::DB::Open(oOptions, cluster_mgr_tmp_data_path+"/cluster_mgr_raft/leveldb_data", &leveldb_);
  if(!oStatus.ok()) {
    setErr("Fail to init leveldb, %s", oStatus.ToString().c_str());
    return false;
  }

  options.log_uri = prefix + "/log";
  options.raft_meta_uri = prefix + "/raft_meta";
  options.snapshot_uri = prefix + "/snapshot";

  node_ = new braft::Node(raft_groupid, braft::PeerId(ep));
  if (node_->init(options) != 0) {
    setErr("Fail to init raft node");
    delete node_;
    return false;
  }

  //read raft data
  ReadMsgLeveldb();

  return true;
}

void NodeHa::on_apply(braft::Iterator &iter) {

  for (; iter.valid(); iter.next()) {

    braft::AsyncClosureGuard closure(iter.done());
    raft_msg msgpb;

    if (iter.done()) {
      // if iter.done() is not nullptr means task was proposed locally
      NodeHaClosure *c = dynamic_cast<NodeHaClosure *>(iter.done());
      msgpb = c->GetRaftMsg();
      if(c->cb_) 
        c->cb_(true, c->cb_ctx_);
    } else {
      butil::IOBuf save_log = iter.data();
      uint32_t msg_size = 0;
      save_log.cutn(&msg_size, sizeof(uint32_t));
      msg_size = butil::NetToHost32(msg_size);
      
      butil::IOBuf msg;
      save_log.cutn(&msg, msg_size);
      butil::IOBufAsZeroCopyInputStream wrapper(msg);
      if(!msgpb.ParseFromZeroCopyStream(&wrapper)) {
        KLOG_ERROR("parse raft msg to pb failed");
        return;
      }
    }
    DealHaMsg(msgpb);
  }
  return;
}

struct SnapshotArg {
  scoped_fd fd;
  braft::SnapshotWriter* writer;
  braft::Closure* done;
};

static int link_overwrite(const char* old_path, const char* new_path) {
  if (::unlink(new_path) < 0 && errno != ENOENT) {
    //LOG(ERROR) << "Fail to unlink " << new_path;
    KLOG_ERROR("Fail to unlink {}", new_path);
    return -1;
  }
  return ::link(old_path, new_path);
}

static void *save_snapshot(void* arg) {
  SnapshotArg* sa = (SnapshotArg*)arg;
  std::unique_ptr<SnapshotArg> arg_guard(sa);

  brpc::ClosureGuard done_guard(sa->done);
  std::string snapshot_path = sa->writer->get_path() + "/data";
  // Sync buffered data before
  int rc = 0;
  //LOG(INFO) << "Saving snapshot to " << snapshot_path;
  KLOG_DEBUG("Saving snapshot to {}", snapshot_path);
  for (; (rc = ::fdatasync(sa->fd->fd())) < 0 && errno == EINTR;) {}
  if (rc < 0) {
    sa->done->status().set_error(EIO, "Fail to sync fd=%d : %m",
                                       sa->fd->fd());
    return NULL;
  }
  std::string data_path = cluster_mgr_tmp_data_path+"/cluster_mgr_raft/ss_data";
  if (link_overwrite(data_path.c_str(), snapshot_path.c_str()) != 0) {
    sa->done->status().set_error(EIO, "Fail to link data : %m");
    return NULL;
  }
        
  // Snapshot is a set of files in raft. Add the only file into the
  // writer here.
  if (sa->writer->add_file("data") != 0) {
    sa->done->status().set_error(EIO, "Fail to add file to writer");
    return NULL;
  }
  return NULL;
}

void NodeHa::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
  SnapshotArg* arg = new SnapshotArg;
  arg->fd = fd_;
  arg->writer = writer;
  arg->done = done;
  bthread_t tid;
  bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}

int NodeHa::on_snapshot_load(braft::SnapshotReader* reader) {
  // Load snasphot from reader, replacing the running StateMachine
  if(!is_leader())  {
    KLOG_ERROR("Leader is not supposed to load snapshot");
    return 0;
  }
        
  if (reader->get_file_meta("data", NULL) != 0) {
    KLOG_ERROR("Fail to find `data` on {}", reader->get_path());
    return -1;
  }
  
  // reset fd
  fd_ = NULL;
  std::string snapshot_path = reader->get_path() + "/data";
  std::string data_path = cluster_mgr_tmp_data_path+"/cluster_mgr_raft/ss_data";
  if (link_overwrite(snapshot_path.c_str(), data_path.c_str()) != 0) {
    KLOG_ERROR("Fail to link data");
    return -1;
  }
  
  // Reopen this file
  int fd = ::open(data_path.c_str(), O_RDWR, 0644);
  if (fd < 0) {
    KLOG_ERROR("Fail to open {}", data_path);
    return -1;
  }
  fd_ = new SharedFD(fd);
  return 0;
}

void NodeHa::on_leader_start(int64_t term) {
  leader_term_.store(term, butil::memory_order_release);
  notify_state_change();
  LOG(INFO) << "Node becomes leader";
}
void NodeHa::on_leader_stop(const butil::Status &status) {
  leader_term_.store(-1, butil::memory_order_release);
  notify_state_change();
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

bool NodeHa::DealHaMsg(const raft_msg& msgpb) {
  bool ret = false;

  if(static_cast<Msg_Operator>(msgpb.op()) == MSG_INSERT)
    ret = SyncMsgLeveldb(msgpb.key(), msgpb.msg());
  else if(static_cast<Msg_Operator>(msgpb.op()) == MSG_DELETE)
    ret = DeleteMsgLeveldb(msgpb.key());
  
  return ret;
}

//Send msg to other cluster_msg by raft
//message format:
// ha_msg_type + len(uuid) + uuid + len(msg) + msg
void NodeHa::InsertMsg(const std::string& key, const std::string& msg) {
  butil::IOBuf data;
  //HAMsg_Type msg_type = HA_MSG;
  raft_msg msgpb;
  msgpb.set_op(MSG_INSERT);
  msgpb.set_key(key);
  msgpb.set_msg(msg);

  uint32_t msg_size = butil::HostToNet32(msgpb.ByteSize());
  data.append(&msg_size, sizeof(uint32_t));
  butil::IOBufAsZeroCopyOutputStream wrapper(&data);
  if(!msgpb.SerializeToZeroCopyStream(&wrapper)) {
    KLOG_ERROR("Fail to serialize raft msg");
    return;
  }
  
  braft::Task task;
  task.data = &data;
  task.done = new NodeHaClosure(this, msgpb);
  node_->apply(task);
}

void NodeHa::DeleteMsg(const std::string& key) {
  butil::IOBuf data;
  raft_msg msgpb;
  msgpb.set_op(MSG_DELETE);
  msgpb.set_key(key);

  uint32_t msg_size = butil::HostToNet32(msgpb.ByteSize());
  data.append(&msg_size, sizeof(uint32_t));
  butil::IOBufAsZeroCopyOutputStream wrapper(&data);
  if(!msgpb.SerializeToZeroCopyStream(&wrapper)) {
    KLOG_ERROR("Fail to serialize raft msg");
    return;
  }
  
  braft::Task task;
  task.data = &data;
  task.done = new NodeHaClosure(this, msgpb);
  node_->apply(task);
}

bool NodeHa::SyncMsgLeveldb(const std::string& key, const std::string& msg) {
  //syslog(Logger::INFO, "Sync msg key: %s, msg: %s", key.to_string().c_str(), msg.to_string().c_str());
  if(!is_leader()) {
    if(strncmp(key.c_str(), "KUNLUN_GLOBAL_ID", 16) == 0) {
      uint64_t id = atoll(msg.c_str());
      gen_id_->SlaveClusterMgrUpdateId(id);
    } 
  }
  
  leveldb::Status oStatus = leveldb_->Put(leveldb::WriteOptions(), key, msg);
  if(!oStatus.ok()) {
    //LOG(INFO) << "leveldb.Put failed, key " << key;
    KLOG_ERROR("leveldb.Put key: {} failed: {}", 
            key, oStatus.ToString());
    return false;
  }
  
  KLOG_INFO("leveldb.Put key: {} successfull", key);
  return true;
}

bool NodeHa::DeleteMsgLeveldb(const std::string& key) {
  //syslog(Logger::INFO, "Delete msg key: %s", key.to_string().c_str());

  leveldb::Status oStatus = leveldb_->Delete(leveldb::WriteOptions(), key);
  if(!oStatus.ok()) {
    //LOG(INFO) << "leveldb.Delete failed, key " << key;
    KLOG_ERROR("leveldb.Delete key: {} failed: {}", 
            key, oStatus.ToString()); 
    return false;
  }
  KLOG_INFO("leveldb.Delete key: {} successfull", key);
  return true;
}

void NodeHa::ReadMsgLeveldb() {
  leveldb::Iterator *it = leveldb_->NewIterator(leveldb::ReadOptions());
  it->SeekToFirst();
  std::vector<std::string> del_keys;
  while(it->Valid()) {
    std::string key = it->key().ToString();
  
    if(strncmp(key.c_str(), "KUNLUN_GLOBAL_ID", 16) == 0) {
      uint64_t id = atoll(it->value().ToString().c_str());
      gen_id_->Init(id);
    }

    it->Next();
  }

  for(auto it : del_keys) 
    DeleteMsg(it);
}

void NodeHa::DispatchJobs() {
  
}

void NodeHa::wait_state_change() {
  std::unique_lock<std::mutex> lock{ mutex_ };
	state_cv_.wait(lock, [this]{
    if(cur_state_ == RAFT_MASTER) {// master
      if(!this->is_leader()) {
        cur_state_ = RAFT_SLAVE;
        return true;
      }
    } else if(cur_state_ == RAFT_SLAVE){ //slave
      if(this->is_leader()) {
        cur_state_ = RAFT_MASTER;
        return true;
      }
    } else {
      if(this->is_leader())
        cur_state_ = RAFT_MASTER;
      else
        cur_state_ = RAFT_SLAVE;
      return true;
    }
    return false;
	});
}

void NodeHa::notify_state_change() {
  state_cv_.notify_one();
}

void NodeHa::UpdateMetaClusterMgrState() {
  ObjectPtr<MetadataShard> metashard = System::get_instance()->get_MetadataShard();
  if(!metashard.Invalid())
    return;
  
  ObjectPtr<Shard_node> master = metashard->get_master();
  if(!master.Invalid())
    return;

  std::string sel_sql = "select * from cluster_mgr_nodes";
  MysqlResult res;
  int ret = master->send_stmt(sel_sql.c_str(), &res, stmt_retries);
  if(ret) {
    KLOG_ERROR("select cluster_mgr_nodes failed: {}",
              master->get_mysql_err());
    return;
  }

  std::vector<std::string> raft_lists = kunlun::StringTokenize(raft_group_member_init_config, ",");
  std::vector<std::string> raft_ips;
  for(auto it : raft_lists) {
    std::vector<std::string> ipports = kunlun::StringTokenize(it, ":");
    if(ipports.size() == 3) {
      raft_ips.emplace_back(ipports[0]);
    }
  }

  std::vector<std::string> exist_host;
  std::vector<std::string> sql;
  int num_rows = res.GetResultLinesNum();
  for(int i=0; i<num_rows; i++) {
    std::string hostaddr = res[i]["hostaddr"];
    std::string sport = res[i]["port"];
    std::string state = res[i]["member_state"];
    exist_host.push_back(hostaddr+"_"+sport);

    int exist_flag = 0;
    for(auto it : raft_ips) {
      if(it == hostaddr && sport == std::to_string(cluster_mgr_brpc_http_port)) {
        exist_flag = 1;
        if(it == local_ip) {
          if(state != "source") {
              std::string tmp = kunlun::string_sprintf("update cluster_mgr_nodes set member_state='source', prometheus_port=%lu where hostaddr='%s' and port=%s",
                  prometheus_port_start, it.c_str(), sport.c_str());
              sql.emplace_back(tmp);
            } else {
              std::string tmp = kunlun::string_sprintf("update cluster_mgr_nodes set prometheus_port=%lu where hostaddr='%s' and port=%s",
                  prometheus_port_start, it.c_str(), sport.c_str());
              sql.emplace_back(tmp);
            }
        } else {
          if(state != "replica") {
            std::string tmp = kunlun::string_sprintf("update cluster_mgr_nodes set member_state='replica', prometheus_port=%lu where hostaddr='%s' and port=%s",
                prometheus_port_start, it.c_str(), sport.c_str());
            sql.emplace_back(tmp);
          } else {
            std::string tmp = kunlun::string_sprintf("update cluster_mgr_nodes set prometheus_port=%lu where hostaddr='%s' and port=%s",
                prometheus_port_start, it.c_str(), sport.c_str());
            sql.emplace_back(tmp);
          }
        }
      } 
    }

    if(!exist_flag) {
      std::string tmp = kunlun::string_sprintf("delete from cluster_mgr_nodes where hostaddr='%s' and port=%s",
              hostaddr.c_str(), sport.c_str());
      sql.emplace_back(tmp);
    }
  }

  for(auto it : raft_ips) {
    std::string host = it+"_"+std::to_string(cluster_mgr_brpc_http_port);
    if(std::find(exist_host.begin(), exist_host.end(), host) == exist_host.end()) {
      std::string tmp;
      if(it == local_ip) {
        tmp = kunlun::string_sprintf("insert into cluster_mgr_nodes(hostaddr, port, member_state, prometheus_port)"
              " values('%s', %ld, 'source', %lu)", it.c_str(), cluster_mgr_brpc_http_port, prometheus_port_start);
      } else {
        tmp = kunlun::string_sprintf("insert into cluster_mgr_nodes(hostaddr, port, member_state, prometheus_port)"
              " values('%s', %ld, 'replica', %lu)", it.c_str(), cluster_mgr_brpc_http_port, prometheus_port_start);
      }
      sql.emplace_back(tmp);
    }
  }

  for(auto it : sql) {
    KLOG_INFO("update cluster_mgr_nodes sql: {}", it);
    ret = master->send_stmt(it.c_str(), &res, stmt_retries);
    if(ret) {
      KLOG_ERROR("execute sql: {} failed: {}", it,
                master->get_mysql_err());
      return;
    }
  }
}
