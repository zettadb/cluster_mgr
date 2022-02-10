/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "expand_mission.h"
#include "json/json.h"

// http://192.168.0.104:10000/trac/wiki/kunlun.features.design.scale-out

bool ExpandClusterMission::ArrangeRemoteTask()
{
  // Dump table
  RemoteTask *dump_table_task = new RemoteTask("Expand_Dump_Table");
  dump_table_task->AddNodeSubChannel("192.168.0.135:8010",
                                     g_node_channel_manager.getNodeChannel("192.168.0.135:8010"));

  std::string dump_table_action = "mydumper";
  Json::Value root;
  root["mysql_port"] = "8001";
  if (!get_body_json_document().isMember("paras"))
  {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setErr("Json Filed `paras` is not specified in request body for the EXPAND_CLUSTER request");
    return false;
  }
  if (!get_body_json_document()["paras"].isMember("table_list"))
  {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setErr("Json Filed `paras`->`table_list` is not specified in request body for the EXPAND_CLUSTER request");
    return false;
  }
  Json::Value table_list = get_body_json_document()["paras"]["table_list"];
  root["table_names"] = table_list;
  dump_table_task->SetPara("192.168.0.135:8010", root);

  get_task_manager()
      ->PushBackTask(dump_table_task);

  return true;
}
bool ExpandClusterMission::FillRequestBodyStImpl()
{
  return true;
}

bool ExpandClusterMission::SetUpMisson()
{
  syslog(Logger::INFO, "setup phase");
  return true;
}
void ExpandClusterMission::TearDownImpl()
{
  syslog(Logger::INFO, "teardown phase");
  return;
}
