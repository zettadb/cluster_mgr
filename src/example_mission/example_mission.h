/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"

namespace kunlun {
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
