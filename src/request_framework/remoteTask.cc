/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "remoteTask.h"

static void CallBC(brpc::Controller *cntl)
{
  if (!cntl->Failed())
  {
    syslog(Logger::INFO, "CallBc(): response is %s",cntl->response_attachment().to_string().c_str());
    return;
  }
  syslog(Logger::ERROR, "%s", cntl->ErrorText().c_str());
}

void RemoteTask::AddNodeSubChannel(const char *node_hostaddr, brpc::Channel *sub_channel)
{
  auto iter = channel_map_.find(node_hostaddr);
  if (iter == channel_map_.end())
  {
    std::pair<std::string, brpc::Channel *> pr = std::make_pair(node_hostaddr, sub_channel);
    channel_map_.insert(pr);
    return;
  }
  syslog(Logger::INFO, "Already exists %s related channel handler. So ignore and continue", node_hostaddr);
  return;
}

const char *RemoteTask::get_task_spec_info() const
{
  return task_spec_info_.c_str();
}

void RemoteTask::SetPara(const char *node_hostaddr, Json::Value para)
{
  paras_map_[node_hostaddr] = para;
}

void RemoteTask::setParaToRequestBody(brpc::Controller *cntl, std::string node_hostaddr)
{
  Json::Value para_json = paras_map_[node_hostaddr];
  Json::FastWriter writer;
  std::string body = writer.write(para_json);
  cntl->request_attachment().append(body);
  cntl->http_request().set_method(brpc::HTTP_METHOD_POST);
  // TODO: check JSON object is valid
}

bool RemoteTask::RunTaskImpl()
{
  std::map<std::string, brpc::CallId> call_id_map;
  auto iter = channel_map_.begin();
  for (; iter != channel_map_.end(); iter++)
  {
    kunlunrpc::HttpService_Stub stub(iter->second);
    brpc::Controller *cntl = new brpc::Controller();

    setParaToRequestBody(cntl, iter->first);

    google::protobuf::Closure *done = brpc::NewCallback(&CallBC, cntl);
    // `call_id` must be saved before the real RPC call
    call_id_map[iter->first] = (cntl->call_id());

    stub.Emit(cntl, nullptr, nullptr, done);
  }
  // sync wait response
  iter = channel_map_.begin();
  for (; iter != channel_map_.end(); iter++)
  {
    brpc::CallId id = call_id_map[iter->first];
    brpc::Join(id);
  }
  return true;
}

void TaskManager::PushBackTask(RemoteTask *sub_task)
{
  return remote_task_vec_.push_back(sub_task);
}
const std::vector<RemoteTask *> &TaskManager::get_remote_task_vec()
{
  return remote_task_vec_;
}
