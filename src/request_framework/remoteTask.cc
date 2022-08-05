/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "remoteTask.h"
#include "zettalib/op_log.h"

using namespace kunlun;

static void CallBC(brpc::Controller *cntl, RemoteTask *task) {
  if (!cntl->Failed()) {

    // deal the attachment
    // store the channel response attachment to the task RESPONSE
    std::string channle_name = endpoint2str(cntl->remote_side()).c_str();
    std::string key =
        std::string(task->get_task_spec_info()) + "#" + channle_name;

    bool ret = task->get_response()->ParseAttachment(
        key.c_str(), cntl->response_attachment().to_string().c_str());
    if (!ret) {
      KLOG_ERROR("{}, {}", task->get_response()->getErr(),
                 task->get_response()->get_err_num_str());
    }
    KLOG_INFO("General Remote Task CallBC(): task {} response from {} is: {}",
              task->get_task_spec_info(), channle_name,
              cntl->response_attachment().to_string());

  } else {
    task->get_response()->SetRpcFailedInfo(cntl, task);
    KLOG_ERROR("{}", cntl->ErrorText());
  }
  // do the drived call back if defined
  if (task->call_back_ != nullptr) {
    (*(task->call_back_))(task->cb_context_);
  }
  return;
}

void RemoteTaskResponse::SetRpcFailedInfo(brpc::Controller *cntl,
                                          RemoteTask *task) {
  KlWrapGuard<KlWrapMutex> guard(mutex_);
  Json::Value root;
  root["status"] = "failed";
  root["info"] = "RPC failed";
  task_spec_info_ = std::string(task->get_task_spec_info());
  error_info_ += "RPC failed;";
  std::string channle_name = endpoint2str(cntl->remote_side()).c_str();
  std::string key =
      std::string(task->get_task_spec_info()) + "#" + channle_name;
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  attachment_str_map_[std::string(key)] = writer.write(root);
  attachment_json_map_[std::string(key)] = root;
  failed_occour_.store(true);
}

bool RemoteTaskResponse::ParseAttachment(const char *key,
                                         const char *attachment) {

  KlWrapGuard<KlWrapMutex> guard(mutex_);
  Json::Value root;
  Json::Reader reader;
  bool ret = reader.parse(attachment, root);
  if (!ret) {
    setErr("JSON parse error: %s, JSON string: %s",
           reader.getFormattedErrorMessages().c_str(), attachment);
    set_err_num(EIVALID_RESPONSE_PROTOCAL);
    return false;
  }
  if (!root.isMember("status")) {
    setErr("Missing `status` key/value pairs");
    set_err_num(EIVALID_RESPONSE_PROTOCAL);
    return false;
  }
  if (root["status"].asString() == "failed") {
    failed_occour_.store(true);

    if (root.isMember("info")){
      error_info_ = root["info"].asString() + ";";
    }
  }

  task_spec_info_ = root["task_spec_info"].asString();
  if(root.isMember("cluster_mgr_request_id")){
    request_id_ = root["cluster_mgr_request_id"].asString();
  }
  else{
    request_id_ = "-1";
  }
  std::string key_hash_suffix;
  std::hash<std::string> hasher;
  key_hash_suffix = "#hash" + std::to_string(hasher(std::string(attachment)) % 1000);
  attachment_str_map_[std::string(key)+key_hash_suffix] = std::string(attachment);
  attachment_json_map_[std::string(key)+key_hash_suffix] = root;
  return true;
}
bool RemoteTaskResponse::ok() {
  return !failed_occour_.load();
}

std::string RemoteTaskResponse::get_request_id() {
  KlWrapGuard<KlWrapMutex> guard(mutex_);
  //return (task_spec_info_.substr(task_spec_info_.rfind("_") + 1));
  return request_id_;
}

std::string RemoteTaskResponse::get_error_info() {
  KlWrapGuard<KlWrapMutex> guard(mutex_);
  return error_info_;
}

Json::Value RemoteTaskResponse::get_all_response_json() {
  return all_response_;
}

std::string RemoteTaskResponse::SerializeResponseToStr() {
  KlWrapGuard<KlWrapMutex> guard(mutex_);
  Json::Value root;
  for (auto iter = attachment_json_map_.begin();
       iter != attachment_json_map_.end(); ++iter) {
    Json::Value item;
    item["piece"] = iter->first;
    item["info"] = iter->second;
    root.append(item);
  }
  all_response_["task_spec_info"] = task_spec_info_;
  all_response_["response_array"] = root;
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  return writer.write(all_response_);
}
RemoteTaskResponse *RemoteTask::get_response() { return &response_; }
void RemoteTask::Set_call_back(void (*function)(void *)) {
  call_back_ = function;
}

void RemoteTask::Set_cb_context(void *context) { cb_context_ = context; }

bool RemoteTask::TaskReport() { return TaskReportImpl(); }

bool RemoteTask::TaskReportImpl() {
  // default action
  KLOG_INFO("Task Info Report, NotDefined");
  return true;
}

void RemoteTask::AddNodeSubChannel(const char *node_hostaddr,
                                   brpc::Channel *sub_channel) {
  auto iter = channel_map_.find(node_hostaddr);
  if (iter == channel_map_.end()) {
    std::pair<std::string, brpc::Channel *> pr =
        std::make_pair(node_hostaddr, sub_channel);
    channel_map_.insert(pr);
    return;
  }
  KLOG_INFO("Already exists {} related channel handler. So ignore and continue",
            node_hostaddr);
  return;
}

const char *RemoteTask::get_task_spec_info() const {
  return task_spec_info_.c_str();
}

void RemoteTask::SetPara(const char *node_hostaddr, Json::Value para) {
  paras_map_[node_hostaddr] = para;
}

void RemoteTask::SetParaToRequestBody(brpc::Controller *cntl,
                                      std::string node_hostaddr) {
  Json::Value para_json = paras_map_[node_hostaddr];
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  std::string body = writer.write(para_json);
  cntl->request_attachment().append(body);
  cntl->http_request().set_method(brpc::HTTP_METHOD_POST);
  cntl->set_timeout_ms(5000000);
  // TODO: check JSON object is valid
}

void RemoteTask::SetUpStatus() { return; }
std::string RemoteTask::getTaskInfo() {
  Json::Value root;
  auto iter = paras_map_.begin();
  for (; iter != paras_map_.end(); iter++) {
    Json::Value it;
    it["piece"] = iter->first;
    it["content"] = iter->second;
    root.append(it);
  }
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  return writer.write(root);
}

bool RemoteTask::RunTask() {
  SetUpStatus();

  std::map<std::string, brpc::CallId> call_id_map;
  auto iter = channel_map_.begin();
  for (; iter != channel_map_.end(); iter++) {
    if (iter->second == nullptr) {
      KLOG_ERROR("Got ivalid channel(nullptr) which related with {}",
                 iter->first);
      return false;
    }
    kunlunrpc::HttpService_Stub stub(iter->second);
    brpc::Controller *cntl = new brpc::Controller();

    SetParaToRequestBody(cntl, iter->first);

    google::protobuf::Closure *done = brpc::NewCallback(&CallBC, cntl, this);
    // `call_id` must be saved before the real RPC call
    call_id_map[iter->first] = (cntl->call_id());

    stub.Emit(cntl, nullptr, nullptr, done);
  }
  // sync wait response
  iter = channel_map_.begin();
  for (; iter != channel_map_.end(); iter++) {
    brpc::CallId id = call_id_map[iter->first];
    brpc::Join(id);
  }
  response_.SerializeResponseToStr();
  // report Info
  TaskReport();

  return true;
}

void RemoteTask::set_prev_task(RemoteTask *prev) { prev_task_ = prev; }

void TaskManager::PushBackTask(RemoteTask *sub_task) {
  return remote_task_vec_.push_back(sub_task);
}
const std::vector<RemoteTask *> &TaskManager::get_remote_task_vec() {
  return remote_task_vec_;
}

void TaskManager::SetSerializeResult(const std::string &result) {
  serialized_result_ = result;
}

void TaskManager::SerializeAllResponse() {
  Json::Value root;
  std::string error_info;
  std::string request_id;
  for (auto i = remote_task_vec_.begin(); i != remote_task_vec_.end(); i++) {
    if (!(*i)->get_response()->ok()) {
      error_occour_ = true;
      error_info += (*i)->get_response()->get_error_info() + "|";
    }
    request_id = (*i)->get_response()->get_request_id();
    root["result"].append((*i)->get_response()->get_all_response_json());
  }

  root["request_id"] = request_id;
  if (error_occour_) {
    root["status"] = "failed";
    root["error_info"] = error_info;
  } else {
    root["status"] = "done";
    root["error_info"] = "success";
  }
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  serialized_result_ = writer.write(root);
}

bool TaskManager::ok() { return !error_occour_; }
