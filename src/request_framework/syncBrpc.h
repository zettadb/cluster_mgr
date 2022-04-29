/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _SYNC_BRPC_H_
#define _SYNC_BRPC_H_

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/parallel_channel.h"
#include "brpc/server.h"
#include "bthread/bthread.h"
#include "butil/logging.h"
#include "butil/macros.h"
#include "butil/string_printf.h"
#include "butil/time.h"
#include "http_server/proto/clustermng.pb.h"
#include "request_framework/requestValueDefine.h"
#include "util_func/error_code.h"
#include "zettalib/errorcup.h"
#include "zettalib/op_mysql.h"
#include "zettalib/zthread.h"
#include "json/json.h"
#include <map>
#include <string>

// Not thread-safe yet
class SyncBrpc {
public:
  SyncBrpc(){};
  ~SyncBrpc(){};

  bool syncBrpcToNode(std::string &hostaddr, Json::Value &para);
  std::string response;
  bool result;
};

#endif /*_SYNC_BRPC_H_*/
