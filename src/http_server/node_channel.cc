/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "node_channel.h"
//#include "kl_mentain/log.h"
#include "http_server/proto/clustermng.pb.h"
#include "kl_mentain/global.h"
#include "kl_mentain/sys.h"
#include "stdio.h"
#include "string.h"
#include "util_func/meta_info.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"

extern std::string meta_svr_ip;
extern int64_t meta_svr_port;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

using namespace kunlun;

GlobalNodeChannelManager::~GlobalNodeChannelManager() {
  auto iter = nodes_channel_map_.begin();
  for (; iter != nodes_channel_map_.end(); iter++) {
    delete (iter->second);
  }
}

const std::map<std::string, brpc::Channel *> &
GlobalNodeChannelManager::get_nodes_channel_map() const {
  return nodes_channel_map_;
}

void GlobalNodeChannelManager::removeNodeChannel(const char * addr) {
    auto iter = nodes_channel_map_.find(addr);
    if(iter != nodes_channel_map_.end()) {
      delete (iter->second);
      nodes_channel_map_.erase(iter);
    }
}

brpc::Channel *GlobalNodeChannelManager::getNodeChannel(const char *addr) {
  auto iter = nodes_channel_map_.find(addr);
  if (iter == nodes_channel_map_.end()) {
    return nullptr;
  }
  return nodes_channel_map_[addr];
}

bool GlobalNodeChannelManager::initNodeChannelMap() {
  ObjectPtr<MetadataShard> metashard = System::get_instance()->get_MetadataShard();
  if(!metashard.Invalid()) {
    setErr("get meta shard failed, please check");
    return false;
  }

  ObjectPtr<Shard_node> node;
  for (auto it : metashard->get_nodes()) {
    if(it.Invalid()) {
      node = it;
      break;
    }
  }
  
  if(!node.Invalid()) {
    setErr("get meta shard node failed");
    return false;
  }

  char sql_stmt[1024] = {'\0'};
  sprintf(sql_stmt, "SELECT distinct hostaddr,nodemgr_port FROM `%s`.`server_nodes`",
          KUNLUN_METADATA_DB_NAME);
  kunlun::MysqlResult result;
  bool ret = node->send_stmt(sql_stmt, &result, stmt_retries);
  if(ret) {
    setErr("get server_nodes failed");
    return false;
  }

  for (int i = 0; i < result.GetResultLinesNum(); i++) {
    char node_hostaddr_str[1024] = {'\0'};
    std::string hostaddr = result[i]["hostaddr"];
    std::string nodemgr_port = result[i]["nodemgr_port"];
    sprintf(node_hostaddr_str, "%s:%s", result[i]["hostaddr"],
            result[i]["nodemgr_port"]);
    if (!kunlun::ValidNetWorkAddr(result[i]["hostaddr"])) {
      KLOG_INFO("Invalid Network address: {}. Will ignore", node_hostaddr_str);
      continue;
    }
    if(nodemgr_port != "NULL")
      host_nodemgr_port_[hostaddr] = nodemgr_port;

    char url[2048] = {'\0'};
    sprintf(url, "http://%s/HttpService/Emit", node_hostaddr_str);

    brpc::Channel *channel = new brpc::Channel();
    brpc::ChannelOptions channel_options;
    channel_options.max_retry = 0;
    channel_options.protocol = "http";
    channel_options.connection_type = "short";
    channel_options.timeout_ms = 3600000;

    int ret = channel->Init(url, "", &channel_options);
    if (ret != 0) {
      set_err_num(ENODE_UNREACHEABLE);
      KLOG_ERROR("Channel for {} init faild,Ignored", node_hostaddr_str);
      setErr("Channel for %s init faild", node_hostaddr_str);
      delete channel;
      continue;
      // return false;
    }
    KLOG_INFO("Channel {} for Node Manager init successfully",
           node_hostaddr_str);
    nodes_channel_map_[result[i]["hostaddr"]] = channel;
  }
  return true;
}

bool GlobalNodeChannelManager::Init() {

  /*if (!mysql_conn_) {
    kunlun::MysqlConnectionOption option;
    option.ip = meta_svr_ip;
    option.port_num = meta_svr_port;
    option.user = meta_svr_user;
    option.password = meta_svr_pwd;
    option.database = "kunlun_metadata_db";

    mysql_conn_ = new kunlun::MysqlConnection(option);
    if (!mysql_conn_->Connect()) {
      setErr("%s", mysql_conn_->getErr());
      return false;
    }
  }
  mysql_conn_->set_reconnect_support(true);*/
  return initNodeChannelMap();
}

bool GlobalNodeChannelManager::send_stmt(const std::string &sql_stmt,
                                         MysqlResult *result, int nretries) {
  ObjectPtr<MetadataShard> metashard = System::get_instance()->get_MetadataShard();
  if (!metashard.Invalid()) {
    setErr("get meta shard failed, please check");
    return true;
  }

  ObjectPtr<Shard_node> node = metashard->get_master();
  if(!node.Invalid()) {
    setErr("get meta shard node failed");
    return true;
  }

  return node->send_stmt(sql_stmt, result, nretries);
}

void GlobalNodeChannelManager::AddChannel(std::string key,
                                          brpc::Channel *channel) {
  if (channel != nullptr) {
    this->nodes_channel_map_[key] = channel;
  }
}

bool SyncNodeChannel::Init() {
  KlWrapGuard<KlWrapMutex> guard(mux_);

  char url[2048] = {'\0'};
  sprintf(url, "http://%s:%s/HttpService/Emit", ip_.c_str(), port_.c_str());

  node_channel_ = g_node_channel_manager->getNodeChannel(ip_.c_str());
  if (node_channel_ != nullptr) {
    KLOG_INFO("Got catched channel {}",ip_);
    return true;
  }


  KLOG_INFO("Can't Got catched channel {}, new one",ip_);
  node_channel_ = new brpc::Channel();
  brpc::ChannelOptions channel_options;
  channel_options.max_retry = 0;
  channel_options.protocol = "http";
  channel_options.connection_type = "short";

  int ret = node_channel_->Init(url, "", &channel_options);
  if (ret != 0) {
    set_err_num(ENODE_UNREACHEABLE);
    KLOG_ERROR("Channel for {}:{} init faild,Ignored", ip_.c_str(),
               port_.c_str());
    setErr("Channel for %s:%s init faild", ip_.c_str(), port_.c_str());
    delete node_channel_;
    return false;
  }
  g_node_channel_manager->AddChannel(ip_, node_channel_);
  return true;
}

int SyncNodeChannel::ExecuteCmd(Json::Value& root) {
  KlWrapGuard<KlWrapMutex> guard(mux_);

  kunlunrpc::HttpService_Stub stub(node_channel_);
  brpc::Controller cntl;

  Json::FastWriter writer;
  std::string body = writer.write(root);
  KLOG_DEBUG("Sync node channel execute cmd body: {}", body);
  cntl.request_attachment().append(body);
  cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
  cntl.set_timeout_ms(timeout_ * 1000);

  stub.Emit(&cntl, nullptr, nullptr, nullptr);

  if(cntl.Failed()) {
    setErr("Failed send request to node_mgr, %s", cntl.ErrorText().c_str());
    return ENODE_UNREACHEABLE;
  }

  str_resp_ = cntl.response_attachment().to_string();
  Json::Value json_resp;
  Json::Reader reader;
  bool ret = reader.parse(str_resp_, json_resp);
  if (!ret) {
    setErr("JSON parse error: %s, JSON string: %s",
           reader.getFormattedErrorMessages().c_str(), str_resp_.c_str());
    return EIVALID_RESPONSE_PROTOCAL;
  }

  if (!json_resp.isMember("status")) {
    setErr("Missing `status` key/value pairs");
    return EIVALID_RESPONSE_PROTOCAL;
  }
  if (json_resp["status"].asString() == "failed") {
    if(json_resp.isMember("info"))
      setErr("%s", json_resp["info"].asString().c_str());
    return 1; //execute failed;
  }
  return 0;
  //syslog(Logger::INFO, "get response: %s", respone.c_str());
}