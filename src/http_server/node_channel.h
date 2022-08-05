/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _NODE_CHANNEL_H_
#define _NODE_CHANNEL_H_

#include "brpc/channel.h"
#include "request_framework/requestValueDefine.h"
#include "util_func/error_code.h"
#include "util_func/object_ptr.h"
#include "zettalib/errorcup.h"
#include "zettalib/op_mysql.h"
#include "zettalib/zthread.h"
#include "json/json.h"
#include <map>
#include <string>
#include <vector>
#include "util_func/kl_mutex.h"

typedef struct NodeSpecSt_ {
  std::string ip = "";
  unsigned int port = 0;
} NodeSpecSt;

using namespace kunlun;

// Not thread-safe yet
class GlobalNodeChannelManager : public kunlun::ErrorCup,
                                 public kunlun::GlobalErrorNum {
public:
  GlobalNodeChannelManager() {};
  ~GlobalNodeChannelManager();

  bool Init();
  void removeNodeChannel(const char *);
  brpc::Channel *getNodeChannel(const char *);
  //int run();
  //kunlun::MysqlConnection *get_meta_conn();
  /*
  * mysql execute sql success -- false,
  * failed --- true
  */
  bool send_stmt(const std::string& sql_stmt, MysqlResult* result, int nretries);
  const std::map<std::string,brpc::Channel *> &get_nodes_channel_map() const;
  const std::string& GetNodeMgrPort(const std::string& host) {
    return host_nodemgr_port_[host];
  }
  void AddChannel(std::string,brpc::Channel *channel);

private:
  bool initNodeChannelMap();
  //void reloadFromMeta();

private:
  //kunlun::MysqlConnection *mysql_conn_;
  std::map<std::string, std::string> host_nodemgr_port_;
  std::map<std::string /* hostaddr */, brpc::Channel *> nodes_channel_map_;
};
extern GlobalNodeChannelManager* g_node_channel_manager;

/*
* sync brpc channel 
* support timeout
*/
class SyncNodeChannel : public kunlun::ErrorCup, public ObjectRef, 
                        public kunlun::GlobalErrorNum {
public:
  SyncNodeChannel(uint32_t timeout, const std::string ip, const std::string port) : 
        node_channel_(nullptr), timeout_(timeout), ip_(ip), port_(port) {}
  virtual ~SyncNodeChannel() {
    //if(!node_channel_)
    //  delete node_channel_;
  }
  bool Init();

  int ExecuteCmd(Json::Value& root);
  const std::string GetRespStr() {
    return str_resp_;
  }
private:
  brpc::Channel* node_channel_;
  uint32_t timeout_;
  std::string ip_;
  std::string port_;
  std::string str_resp_;
  KlWrapMutex mux_;
};

#endif /*_NODE_CHANNEL_H_*/
