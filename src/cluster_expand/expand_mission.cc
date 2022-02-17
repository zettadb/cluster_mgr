/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "expand_mission.h"
#include "json/json.h"

// http://192.168.0.104:10000/trac/wiki/kunlun.features.design.scale-out

bool ExpandClusterMission::MakeDir() {
  RemoteTask *make_dir_task = new RemoteTask("Expand_Make_Dir");
  make_dir_task->AddNodeSubChannel(
      "192.168.0.135", g_node_channel_manager.getNodeChannel("192.168.0.135"));
  Json::Value root;
  root["command_name"] = "mkdir -p ";
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = make_dir_task->get_task_spec_info();

  Json::Value paras;
  paras.append(mydumper_tmp_data_dir_);
  root["para"] = paras;
  make_dir_task->SetPara("192.168.0.135", root);
  get_task_manager()->PushBackTask(make_dir_task);
  return true;
}

bool ExpandClusterMission::DumpTable() {

  RemoteTask *dump_table_task = new RemoteTask("Expand_Dump_Table");
  dump_table_task->AddNodeSubChannel(
      "192.168.0.135", g_node_channel_manager.getNodeChannel("192.168.0.135"));

  const Json::Value &request_json_body = get_body_json_document();
  Json::Value root;
  root["command_name"] = "mydumper";
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = dump_table_task->get_task_spec_info();

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
  sprintf(mydumper_arg_buf,
          " -h 192.168.0.135 -u sbtest -p sbtest -P 9001 "
          "-o %s --logfile %s/mydumper.log "
          "-c -T sbtest.sbtest1 ",
          mydumper_tmp_data_dir_.c_str(), mydumper_tmp_data_dir_.c_str());

  Json::Value paras;
  paras.append(mydumper_arg_buf);
  root["para"] = paras;
  dump_table_task->SetPara("192.168.0.135", root);
  get_task_manager()->PushBackTask(dump_table_task);
  return true;
}

bool ExpandClusterMission::LoadTable() {

  RemoteTask *load_table_task = new RemoteTask("Expand_Load_Table");
  load_table_task->AddNodeSubChannel(
      "192.168.0.135", g_node_channel_manager.getNodeChannel("192.168.0.135"));

  const Json::Value &request_json_body = get_body_json_document();
  Json::Value root;
  root["command_name"] = "myloader";
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = load_table_task->get_task_spec_info();

  Json::Value table_list = get_body_json_document()["paras"]["table_list"];

  char myloader_arg_buf[2048] = {'\0'};
  sprintf(myloader_arg_buf,
          " -h 192.168.0.135 -u pgx -p pgx_pwd -P 8001 -e -d %s --logfile %s/myloader.log",
          mydumper_tmp_data_dir_.c_str(), mydumper_tmp_data_dir_.c_str());

  Json::Value paras;
  paras.append(myloader_arg_buf);
  root["para"] = paras;
  load_table_task->SetPara("192.168.0.135", root);
  get_task_manager()->PushBackTask(load_table_task);
  return true;
}

bool ExpandClusterMission::ArrangeRemoteTask() {

  if (!MakeDir()) {
    return false;
  }

  // Step1: Dump table
  if (!DumpTable()) {
    return false;
  }

  // Step2: Transfer dumped files

  // Step3: Load data through Myloader
  if (!LoadTable()) {
    return false;
  }

  // Step4: SetUp the incremental data sync

  // Step5: Shift table

  // Step6: BroadCase the router changes

  return true;
}
bool ExpandClusterMission::FillRequestBodyStImpl() { return true; }

bool ExpandClusterMission::SetUpMisson() {
  syslog(Logger::INFO, "setup phase");
  char mydumper_tmp_data_dir[1024] = {'\0'};
  sprintf(mydumper_tmp_data_dir,
          "TMP_DATA_PATH_PLACE_HOLDER/cluster_request_%s/mydumper-export-%s",
          get_request_unique_id().c_str(), get_request_unique_id().c_str());
  mydumper_tmp_data_dir_ = std::string(mydumper_tmp_data_dir);
  return true;
}
void ExpandClusterMission::TearDownImpl() {
  syslog(Logger::INFO, "teardown phase");
  return;
}
