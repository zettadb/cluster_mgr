/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "node_channel.h"
#include "kl_mentain/log.h"
#include "stdio.h"
#include "string.h"
#include "util_func/meta_info.h"

extern std::string meta_svr_ip;
extern int64_t meta_svr_port;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

GlobalNodeChannelManager::~GlobalNodeChannelManager() {
  auto iter = nodes_channel_map_.begin();
  for (; iter != nodes_channel_map_.end(); iter++) {
    delete (iter->second);
  }
}

kunlun::MysqlConnection *GlobalNodeChannelManager::get_meta_conn() {
  return mysql_conn_;
}

int GlobalNodeChannelManager::run() {
  while (1) {
    reloadFromMeta();
    sleep(1);
  }
  return 0;
}

void GlobalNodeChannelManager::reloadFromMeta() { return; }

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
  char sql_stmt[1024] = {'\0'};
  sprintf(sql_stmt, "SELECT * FROM `%s`.`server_nodes`",
          KUNLUN_METADATA_DB_NAME);
  kunlun::MysqlResult result;
  int ret = mysql_conn_->ExcuteQuery(sql_stmt, &result);
  if (ret < 0) {
    setErr("%s", mysql_conn_->getErr());
    return false;
  }
  for (int i = 0; i < result.GetResultLinesNum(); i++) {
    char node_hostaddr_str[1024] = {'\0'};
    sprintf(node_hostaddr_str, "%s:%s", result[i]["hostaddr"],
            result[i]["nodemgr_port"]);
    if (!kunlun::ValidNetWorkAddr(result[i]["hostaddr"])) {
      syslog(Logger::INFO, "Invalid Network address: %s. Will ignore",
             node_hostaddr_str);
      continue;
    }

    auto iter = nodes_channel_map_.find(result[i]["hostaddr"]);
    if(iter != nodes_channel_map_.end())
      continue;

    char url[2048] = {'\0'};
    sprintf(url, "http://%s/HttpService/Emit", node_hostaddr_str);

    brpc::Channel *channel = new brpc::Channel();
    brpc::ChannelOptions channel_options;
    channel_options.max_retry = 10;
    channel_options.protocol = "http";
    channel_options.timeout_ms = 5000000;

    int ret = channel->Init(url, "", &channel_options);
    if (ret != 0) {
      set_err_num(ENODE_UNREACHEABLE);
      syslog(Logger::ERROR, "Channel for %s init faild,Ignored",
             node_hostaddr_str);
      setErr("Channel for %s init faild", node_hostaddr_str);
      delete channel;
      continue;
      // return false;
    }
    syslog(Logger::INFO, "Channel %s for Node Manager init successfully",
           node_hostaddr_str);
    nodes_channel_map_[result[i]["hostaddr"]] = channel;
  }
  return true;
}

bool GlobalNodeChannelManager::Init() {

  if (!mysql_conn_) {
    kunlun::MysqlConnectionOption option;
    option.ip = meta_svr_ip;
    option.port_num = meta_svr_port;
    option.user = meta_svr_user;
    option.password = meta_svr_pwd;

    mysql_conn_ = new kunlun::MysqlConnection(option);
    if (!mysql_conn_->Connect()) {
      setErr("%s", mysql_conn_->getErr());
      return false;
    }
  }
  mysql_conn_->set_reconnect_support(true);
  return initNodeChannelMap();
}
