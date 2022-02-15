/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _NODE_CHANNEL_H_
#define _NODE_CHANNEL_H_

#include "brpc/channel.h"
#include "zettalib/op_mysql.h"
#include "zettalib/errorcup.h"
#include "zettalib/zthread.h"
#include "request_framework/requestValueDefine.h"
#include "util_func/error_code.h"
#include <map>
#include <string>
#include <vector>

typedef struct NodeSpecSt_
{
  std::string ip = "";
  unsigned int port = 0;
} NodeSpecSt;

// Not thread-safe yet
class GlobalNodeChannelManager : public kunlun::ZThread, public kunlun::ErrorCup, public kunlun::GlobalErrorNum
{
public:
  GlobalNodeChannelManager()
  {
    mysql_conn_ = nullptr;
  };
  ~GlobalNodeChannelManager();

  bool Init();
  brpc::Channel *getNodeChannel(const char *);
  int run();

private:
  bool initNodeChannelMap();
  void reloadFromMeta();

private:
  kunlun::MysqlConnection *mysql_conn_;
  std::map<std::string /* hostaddr */, brpc::Channel *> nodes_channel_map_;
};
extern GlobalNodeChannelManager g_node_channel_manager;

#endif /*_NODE_CHANNEL_H_*/
