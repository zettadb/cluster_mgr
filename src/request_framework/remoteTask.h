/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _CLUSTER_MNG_REMOTE_TASK_H_
#define _CLUSTER_MNG_REMOTE_TASK_H_

#include "brpc/controller.h"
#include "brpc/parallel_channel.h"
#include "brpc/server.h"
#include "bthread/bthread.h"
#include "butil/logging.h"
#include "butil/macros.h"
#include "butil/string_printf.h"
#include "butil/time.h"
#include "http_server/proto/clustermng.pb.h"
#include "requestBase.h"
#include "zettalib/errorcup.h"
#include "json/json.h"
#include <mutex>
#include <string>

using namespace kunlunrpc;
enum REMOTE_TASK_STATUS {
  // R_* means remote
  R_NOT_STARTED = 0,
  R_WATIING_RESPONSE,
  R_SUCCESS_DONE,
  R_FAILED_DONE
};
class TaskManager;
// RemoteTask is converted from the POST method's body
class RemoteTask : public kunlun::ErrorCup, public kunlun::GlobalErrorNum {
public:
  explicit RemoteTask(const char *task_name)
      : task_spec_info_(task_name), call_back_(nullptr){};
  virtual ~RemoteTask(){};

private:
  RemoteTask(const RemoteTask &) = delete;
  RemoteTask &operator=(const RemoteTask &) = delete;

public:
  // Info report
  bool TaskReport();
  // if task execution info need to be report , dirived class
  // can override the TaskReportImpl,defalut action is logging
  bool virtual TaskReportImpl();

public:
  void AddNodeSubChannel(const char *, brpc::Channel *);
  void AddChannelParas(const char *, Json::Value);
  // sync run in bthread
  bool RunTask();
  bool RunSingleTask();
  bool Success();
  Json::Value GetExcuteErrorInfo();
  const char *get_task_spec_info() const;
  const std::map<std::string, Json::Value> &get_response_map() const;
  Json::Value getNodeResponse(const char *);
  void SetPara(const char *, Json::Value);
  void Set_call_back(void (*function)(brpc::Controller *cntl));

private:
  void set_response_map();
  void setParaToRequestBody(brpc::Controller *, std::string);

private:
  std::string task_spec_info_;
  std::map<std::string, brpc::Channel *> channel_map_;
  std::map<std::string, Json::Value> paras_map_;
  void (*call_back_)(brpc::Controller *cntl);
};

class TaskManager : public kunlun::ErrorCup {
public:
  explicit TaskManager() {}
  ~TaskManager();

  void PushBackTask(RemoteTask *);
  const std::vector<RemoteTask *> &get_remote_task_vec();

private:
  // request which own current TaskManager
  std::vector<RemoteTask *> remote_task_vec_;
};

#endif /*_CLUSTER_MNG_REMOTE_TASK_H_*/
