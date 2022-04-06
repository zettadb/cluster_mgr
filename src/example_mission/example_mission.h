/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"

namespace kunlun {

class ExampleRemoteTask : public ::RemoteTask {
  typedef ::RemoteTask super;

public:
  explicit ExampleRemoteTask(const char *task_spec_info, const char *request_id)
      : super(task_spec_info), unique_request_id_(request_id) {}
  ~ExampleRemoteTask() {}
  void SetParaToRequestBody(brpc::Controller *cntl,
                            std::string node_hostaddr) override {
    if (prev_task_ == nullptr) {
      return super::SetParaToRequestBody(cntl, node_hostaddr);
    }

    Json::Value root;
    root["command_name"] = "echo";
    root["cluster_mgr_request_id"] = unique_request_id_;
    root["task_spec_info"] = task_spec_info_;
    Json::Value paras;
    paras.append(prev_task_->get_response()->SerializeResponseToStr());
    paras.append(" 1>&2");
    root["para"] = paras;

    Json::FastWriter writer;
    writer.omitEndingLineFeed();

    cntl->request_attachment().append(writer.write(root));
    cntl->http_request().set_method(brpc::HTTP_METHOD_POST);
  }

private:
  std::string unique_request_id_;
};

class ExampleMission : public ::MissionRequest {
  typedef MissionRequest super;

public:
  explicit ExampleMission(Json::Value *doc) : super(doc){};
  ~ExampleMission(){};

  virtual bool ArrangeRemoteTask() override {
    // for each node_channel, execute the ifconfig and fetch the output
    auto node_channle_map = g_node_channel_manager.get_nodes_channel_map();
    auto iter = node_channle_map.begin();
    for (; iter != node_channle_map.end(); iter++) {
      IfConfig((iter->first).c_str());
    }

    iter = node_channle_map.begin();
    ExampleRemoteTask *fetch_date =
        new ExampleRemoteTask("Example_fetch_date", get_request_unique_id().c_str());
    fetch_date->AddNodeSubChannel(
        (iter->first).c_str(),
        g_node_channel_manager.getNodeChannel((iter->first).c_str()));
    Json::Value root;
    root["command_name"] = "date";
    root["cluster_mgr_request_id"] = get_request_unique_id();
    root["task_spec_info"] = fetch_date->get_task_spec_info();
    Json::Value paras;
    paras.append(" 1>&2");
    root["para"] = paras;
    fetch_date->SetPara((iter->first).c_str(), root);
    get_task_manager()->PushBackTask(fetch_date);

    ExampleRemoteTask *echo_prev_date =
        new ExampleRemoteTask("Example_echo_prev", get_request_unique_id().c_str());
    echo_prev_date->set_prev_task(fetch_date);
    echo_prev_date->AddNodeSubChannel(
        (iter->first).c_str(),
        g_node_channel_manager.getNodeChannel((iter->first).c_str()));
    get_task_manager()->PushBackTask(echo_prev_date);

    return true;
  }
  virtual bool SetUpMisson() override { return true; }
  virtual bool TearDownMission() override { return true; }
  virtual bool FillRequestBodyStImpl() override { return true; }
  virtual void ReportStatus() override {
    syslog(Logger::INFO, "report status called here");
  }

  bool IfConfig(const char *node_ip) {
    brpc::Channel *channel = g_node_channel_manager.getNodeChannel(node_ip);

    // RemoteTask is a base class which has the callback_ facility to support
    // the special requirment, see the ExpandClusterTask for detail
    RemoteTask *task = new RemoteTask("Example_Ifconfig_info");
    task->AddNodeSubChannel(node_ip, channel);
    Json::Value root;
    root["command_name"] = "ifconfig";
    root["cluster_mgr_request_id"] = get_request_unique_id();
    root["task_spec_info"] = task->get_task_spec_info();
    Json::Value paras;
    paras.append("-a 1>&2");
    root["para"] = paras;
    task->SetPara(node_ip, root);
    get_task_manager()->PushBackTask(task);
    return true;
  };
};
} // namespace kunlun
