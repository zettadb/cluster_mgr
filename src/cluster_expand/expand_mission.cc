/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "expand_mission.h"
#include "json/json.h"

// http://192.168.0.104:10000/trac/wiki/kunlun.features.design.scale-out

bool ExpandClusterMission::ArrangeRemoteTask() {

  // Step1: Dump table
  RemoteTask *dump_table_task = new RemoteTask("Expand_Dump_Table");
  dump_table_task->AddNodeSubChannel(
      "192.168.0.135", g_node_channel_manager.getNodeChannel("192.168.0.135"));

  const Json::Value & request_json_body = get_body_json_document();
  Json::Value root;
  root["command_name"] = "mydumper";

  if (!request_json_body.isMember("paras")) {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setErr("Json Field `paras` is not specified in request body for the "
           "EXPAND_CLUSTER request");
    return false;
  }
  if (!request_json_body["paras"].isMember("table_list")) {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setErr("Json Filed `paras`->`table_list` is not specified in request body "
           "for the EXPAND_CLUSTER request");
    return false;
  }
  Json::Value table_list = get_body_json_document()["paras"]["table_list"];

  char mydumper_arg_buf[2048] = {'\0'};
  sprintf(mydumper_arg_buf," -h 127.0.0.1 -u sbtest -p sbtest -P 9001 -o ../data/mydumper-export-%s -c -T sbtest.sbtest1 ",
          //request_json_body["mysql_user"].asCString(),
          //request_json_body["mysql_passwd"].asCString(),
          //request_json_body["mysql_port"].asCString(),
          get_request_unique_id().c_str());

  Json::Value paras;
  paras.append(mydumper_arg_buf);
  root["para"] = paras;
  dump_table_task->SetPara("192.168.0.135", root);
  get_task_manager()->PushBackTask(dump_table_task);

  // Step2: Transfer dumped files

  // Step3: Load data through Myloader

  // Step4: SetUp the incremental data sync

  // Step5: Shift table

  // Step6: BroadCase the router changes

  return true;
}
bool ExpandClusterMission::FillRequestBodyStImpl() { return true; }

bool ExpandClusterMission::SetUpMisson() {
  syslog(Logger::INFO, "setup phase");
  return true;
}
void ExpandClusterMission::TearDownImpl() {
  syslog(Logger::INFO, "teardown phase");
  return;
}
