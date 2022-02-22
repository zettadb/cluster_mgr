/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "expand_mission.h"
#include "http_server/node_channel.h"
#include "json/json.h"

extern GlobalNodeChannelManager g_node_channel_manager;
// http://192.168.0.104:10000/trac/wiki/kunlun.features.design.scale-out

bool ExpandClusterMission::MakeDir() {
  RemoteTask *make_dir_task = new RemoteTask("Expand_Make_Dir");
  make_dir_task->AddNodeSubChannel(
      src_shard_node_address_.c_str(),
      g_node_channel_manager.getNodeChannel(src_shard_node_address_.c_str()));
  Json::Value root;
  root["command_name"] = "mkdir -p ";
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = make_dir_task->get_task_spec_info();

  Json::Value paras;
  paras.append(mydumper_tmp_data_dir_);
  root["para"] = paras;
  make_dir_task->SetPara(src_shard_node_address_.c_str(), root);
  get_task_manager()->PushBackTask(make_dir_task);
  return true;
}

bool ExpandClusterMission::DumpTable() {

  RemoteTask *dump_table_task = new RemoteTask("Expand_Dump_Table");
  dump_table_task->AddNodeSubChannel(
      src_shard_node_address_.c_str(),
      g_node_channel_manager.getNodeChannel(src_shard_node_address_.c_str()));

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
          " -h %s -u pgx -p pgx_pwd -P %d "
          "-o %s --logfile %s/mydumper.log "
          "-c -T %s ",
          src_shard_node_address_.c_str(), src_shard_node_port_,
          mydumper_tmp_data_dir_.c_str(), mydumper_tmp_data_dir_.c_str(),
          table_list[0].asString().c_str());

  Json::Value paras;
  paras.append(mydumper_arg_buf);
  root["para"] = paras;
  dump_table_task->SetPara(src_shard_node_address_.c_str(), root);
  get_task_manager()->PushBackTask(dump_table_task);
  return true;
}

bool ExpandClusterMission::LoadTable() {

  RemoteTask *load_table_task = new RemoteTask("Expand_Load_Table");
  load_table_task->AddNodeSubChannel(
      dst_shard_node_address_.c_str(),
      g_node_channel_manager.getNodeChannel(dst_shard_node_address_.c_str()));

  const Json::Value &request_json_body = get_body_json_document();
  Json::Value root;
  root["command_name"] = "myloader";
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = load_table_task->get_task_spec_info();

  Json::Value table_list = get_body_json_document()["paras"]["table_list"];

  char myloader_arg_buf[2048] = {'\0'};
  sprintf(myloader_arg_buf,
          " -h %s -u pgx -p pgx_pwd -P %d -e -d %s --logfile %s/myloader.log",
          dst_shard_node_address_.c_str(), dst_shard_node_port_,
          mydumper_tmp_data_dir_.c_str(), mydumper_tmp_data_dir_.c_str(),
          table_list[0].asString().c_str());

  Json::Value paras;
  paras.append(myloader_arg_buf);
  root["para"] = paras;
  load_table_task->SetPara(dst_shard_node_address_.c_str(), root);
  get_task_manager()->PushBackTask(load_table_task);
  return true;
}

bool ExpandClusterMission::TableCatchUp() {

  RemoteTask *table_catchup_task = new RemoteTask("Expand_Catchup_Table");
  table_catchup_task->AddNodeSubChannel(
      dst_shard_node_address_.c_str(),
      g_node_channel_manager.getNodeChannel(dst_shard_node_address_.c_str()));

  const Json::Value &request_json_body = get_body_json_document();
  Json::Value root;
  root["command_name"] = "tablecatchup";
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = table_catchup_task->get_task_spec_info();

  Json::Value table_list = get_body_json_document()["paras"]["table_list"];

  char table_catchup_arg_buf[4096] = {'\0'};
  sprintf(table_catchup_arg_buf,
          " -src_addr %s -src_port %d"
          " -src_user pgx -src_pass pgx_pwd"
          " -dst_addr %s -dst_port %d"
          " -dst_user pgx -dst_pass pgx_pwd"
          " --mydumper_metadata_file %s/metadata"
          " -table_list %s",
          src_shard_node_address_.c_str(), src_shard_node_port_,
          dst_shard_node_address_.c_str(), dst_shard_node_port_,
          mydumper_tmp_data_dir_.c_str(),table_list[0].asString().c_str());

  Json::Value paras;
  paras.append(table_catchup_arg_buf);
  root["para"] = paras;
  table_catchup_task->SetPara(dst_shard_node_address_.c_str(), root);
  get_task_manager()->PushBackTask(table_catchup_task);
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
  if (!TransferFile()) {
    return false;
  }

  // Step3: Load data through Myloader
  if (!LoadTable()) {
    return false;
  }

  // Step4: SetUp the incremental data sync
  if (!TableCatchUp()) {
    return false;
  }

  // Step5: BroadCase the router changes

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
  // fetch source/target MySQL instance address
  Json::Value root = get_body_json_document();

  std::string src_shard_id = root["paras"]["src_shard_id"].asString();
  std::string dst_shard_id = root["paras"]["dst_shard_id"].asString();
  kunlun::MysqlConnection *meta_conn = g_node_channel_manager.get_meta_conn();

  char sql_stmt[1024] = {'\0'};
  // deal src shard meta info
  sprintf(
      sql_stmt,
      "select hostaddr,port from kunlun_metadata_db.shard_nodes where id = %s",
      src_shard_id.c_str());
  kunlun::MysqlResult result;
  int ret = meta_conn->ExcuteQuery(sql_stmt, &result);
  if (ret < 0) {
    setErr("%s", meta_conn->getErr());
    return false;
  }
  if (result.GetResultLinesNum() == 0) {
    setErr("Can't get MySQL instance address by shard id %s",
           src_shard_id.c_str());
    return false;
  }
  src_shard_node_address_ = result[0]["hostaddr"];
  src_shard_node_port_ = ::atoi(result[0]["port"]);

  bzero((void *)sql_stmt, 1024);
  result.Clean();
  // deal target shard meta info
  sprintf(
      sql_stmt,
      "select hostaddr,port from kunlun_metadata_db.shard_nodes where id = %s",
      dst_shard_id.c_str());
  ret = meta_conn->ExcuteQuery(sql_stmt, &result);
  if (ret < 0) {
    setErr("%s", meta_conn->getErr());
    return false;
  }
  if (result.GetResultLinesNum() == 0) {
    setErr("Can't get MySQL instance address by shard id %s",
           dst_shard_id.c_str());
    return false;
  }
  dst_shard_node_address_ = result[0]["hostaddr"];
  dst_shard_node_port_ = ::atoi(result[0]["port"]);

  return true;
}
void ExpandClusterMission::TearDownImpl() {
  syslog(Logger::INFO, "teardown phase");
  return;
}
bool ExpandClusterMission::TransferFile(){
  return true;
}
