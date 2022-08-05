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
#include <map>
#include <atomic>
//#include <mutex>
#include <string>

using namespace kunlunrpc;
using namespace kunlun;

enum REMOTE_TASK_STATUS {
  // R_* means remote
  R_NOT_STARTED = 0,
  R_WATIING_RESPONSE,
  R_SUCCESS_DONE,
  R_FAILED_DONE
};

class RemoteTask;
class RemoteTaskResponse : public kunlun::ErrorCup,
                           public kunlun::GlobalErrorNum {
public:
  RemoteTaskResponse() : failed_occour_(false){};
  ~RemoteTaskResponse(){};

public:
  bool ParseAttachment(const char *, const char *);
  std::string SerializeResponseToStr();
  bool ok();
  std::string get_request_id();
  std::string get_error_info();
  Json::Value get_all_response_json();
  void SetRpcFailedInfo(brpc::Controller *cntl,RemoteTask *task);

private:
  //std::mutex mutex_;
  KlWrapMutex mutex_;
  std::map<std::string, std::string> attachment_str_map_;
  std::map<std::string, Json::Value> attachment_json_map_;
  Json::Value all_response_;
  std::atomic<bool> failed_occour_;
  std::string error_info_;
  std::string task_spec_info_;
  std::string request_id_;
};

class TaskManager;
// RemoteTask is converted from the POST method's body
class RemoteTask : public kunlun::ErrorCup, public kunlun::GlobalErrorNum {
public:
  explicit RemoteTask(const char *task_name)
      : task_spec_info_(task_name), call_back_(nullptr), cb_context_(nullptr), prev_task_(nullptr){};
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
  void virtual SetUpStatus(); 
  // sync run in bthread
  bool virtual RunTask();
  std::string getTaskInfo();
  const char *get_task_spec_info() const;
  void SetPara(const char *, Json::Value);
  void Set_call_back(void (*function)(void *));
  void Set_cb_context(void *context);
  RemoteTaskResponse *get_response();
  // Dirved Class may override this method to implement different set-para
  // operation. For instance, current task para generated based on previous
  // task response
  virtual void SetParaToRequestBody(brpc::Controller *cntl, std::string node_hostaddr);
  void set_prev_task(RemoteTask *);

private:
  void set_response_map();

protected:
  std::string task_spec_info_;

private:
  std::map<std::string, brpc::Channel *> channel_map_;
  std::map<std::string, Json::Value> paras_map_;
  RemoteTaskResponse response_;

public:
  void (*call_back_)(void *);
  void *cb_context_;

protected:
  RemoteTask *prev_task_;

};

class TaskManager : public kunlun::ErrorCup {
public:
  explicit TaskManager() {
    serialized_result_ = "";
    error_occour_ = false;
  }
  ~TaskManager(){
    auto iter = remote_task_vec_.begin();
    for(;iter != remote_task_vec_.end();iter++){
      if((*iter) != nullptr){
        delete (*iter);
      }
    }
  };

  void PushBackTask(RemoteTask *);
  const std::vector<RemoteTask *> &get_remote_task_vec();
  void SerializeAllResponse();
  bool ok();
  void SetSerializeResult(const std::string& result);

public:
  std::string serialized_result_;

private:
  // request which own current TaskManager
  std::vector<RemoteTask *> remote_task_vec_;
  bool error_occour_;
};

#endif /*_CLUSTER_MNG_REMOTE_TASK_H_*/
