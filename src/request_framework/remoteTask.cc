/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "remoteTask.h"

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
      syslog(Logger::ERROR, "%s, %s", task->get_response()->getErr(),
             task->get_response()->get_err_num_str());
    }
    syslog(Logger::INFO,
           "General Remote Task CallBC(): task %s response from %s is: %s",
           task->get_task_spec_info(),
           channle_name.c_str(),
           cntl->response_attachment().to_string().c_str());
    // do the drived call back if defined
    if (task->call_back_ != nullptr) {
      (*(task->call_back_))(task->cb_context_);
    }
    return;
  }
  syslog(Logger::ERROR, "%s", cntl->ErrorText().c_str());
}

bool RemoteTaskResponse::ParseAttachment(const char *key,
                                         const char *attachment) {

  std::lock_guard<std::mutex> guard(mutex_);
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
    failed_occour_ = true;
  }
  task_spec_info_ = root["task_spec_info"].asString();
  attachment_str_map_[std::string(key)] = std::string(attachment);
  attachment_json_map_[std::string(key)] = root;
  return true;
}
bool RemoteTaskResponse::ok() {
  std::lock_guard<std::mutex> guard(mutex_);
  return !failed_occour_;
}

Json::Value RemoteTaskResponse::get_all_response_json() {
  return all_response_;
}

std::string RemoteTaskResponse::SerializeResponseToStr() {
  std::lock_guard<std::mutex> guard(mutex_);
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
  syslog(Logger::INFO, "Task Info Report, NotDefined");
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
  syslog(Logger::INFO,
         "Already exists %s related channel handler. So ignore and continue",
         node_hostaddr);
  return;
}

const char *RemoteTask::get_task_spec_info() const {
  return task_spec_info_.c_str();
}

void RemoteTask::SetPara(const char *node_hostaddr, Json::Value para) {
  paras_map_[node_hostaddr] = para;
}

void RemoteTask::setParaToRequestBody(brpc::Controller *cntl,
                                      std::string node_hostaddr) {
  Json::Value para_json = paras_map_[node_hostaddr];
  Json::FastWriter writer;
  std::string body = writer.write(para_json);
  cntl->request_attachment().append(body);
  cntl->http_request().set_method(brpc::HTTP_METHOD_POST);
  // TODO: check JSON object is valid
}

bool RemoteTask::RunTask() {
  std::map<std::string, brpc::CallId> call_id_map;
  auto iter = channel_map_.begin();
  for (; iter != channel_map_.end(); iter++) {
    kunlunrpc::HttpService_Stub stub(iter->second);
    brpc::Controller *cntl = new brpc::Controller();

    setParaToRequestBody(cntl, iter->first);

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

void TaskManager::PushBackTask(RemoteTask *sub_task) {
  return remote_task_vec_.push_back(sub_task);
}
const std::vector<RemoteTask *> &TaskManager::get_remote_task_vec() {
  return remote_task_vec_;
}

void TaskManager::SerializeAllResponse() {
  Json::Value root;
  for (auto i = remote_task_vec_.begin(); i != remote_task_vec_.end(); i++) {
    if (!(*i)->get_response()->ok()) {
      error_occour_ = true;
    }
    root.append((*i)->get_response()->get_all_response_json());
  }
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  serialized_result_ = writer.write(root);
}

bool TaskManager::ok() { return !error_occour_; }
