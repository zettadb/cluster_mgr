/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "expand_mission.h"
#include "http_server/node_channel.h"
#include "util_func/meta_info.h"
#include "zettalib/tool_func.h"
#include "json/json.h"

// http://192.168.0.104:10000/trac/wiki/kunlun.features.design.scale-out
extern GlobalNodeChannelManager g_node_channel_manager;
extern std::string meta_svr_ip;
extern int64_t meta_svr_port;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

void Expand_Call_Back(void *cb_context) {
  ExpandClusterTask *task = static_cast<ExpandClusterTask *>(cb_context);

  syslog(Logger::INFO, "Expand_Call_Back() response");
}

void ExpandClusterMission::ReportStatus() {
  kunlun::MysqlConnection *meta_conn = g_node_channel_manager.get_meta_conn();
  kunlun::RequestStatus status = get_status();
  char sql[51200] = {'\0'};
  if (status <= kunlun::ON_GOING) {
    sprintf(sql,
            "update kunlun_metadata_db.cluster_general_job_log set status = %d "
            "where id = %s",
            get_status(), get_request_unique_id().c_str());
  } else {
    std::string memo = get_task_manager()->serialized_result_;
    sprintf(
        sql,
        "update kunlun_metadata_db.cluster_general_job_log set status = %d ,"
        "when_ended = CURRENT_TIMESTAMP(6) , memo = '%s' where id = %s",
        get_status(), memo.c_str(), get_request_unique_id().c_str());
  }
  kunlun::MysqlResult result;
  int ret = meta_conn->ExcuteQuery(sql, &result, true);
  if (ret < 0) {
    syslog(Logger::INFO, "Report Request status sql: %s ,failed: %s", sql,
           meta_conn->getErr());
  }
  return;
}

bool ExpandClusterMission::MakeDir() {
  ExpandClusterTask *make_dir_task =
      new ExpandClusterTask("Expand_Make_Dir", related_id_.c_str(), this);
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

  ExpandClusterTask *dump_table_task =
      new ExpandClusterTask("Expand_Dump_Table", related_id_.c_str(), this);
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
  char mydumper_arg_buf[2048] = {'\0'};
  sprintf(mydumper_arg_buf,
          " -h %s -u pgx -p pgx_pwd -P %d "
          "-o %s --logfile %s/mydumper.log "
          "-c -T %s ",
          src_shard_node_address_.c_str(), src_shard_node_port_,
          mydumper_tmp_data_dir_.c_str(), mydumper_tmp_data_dir_.c_str(),
          table_list_str_storage_.c_str());

  Json::Value paras;
  paras.append(mydumper_arg_buf);
  root["para"] = paras;
  dump_table_task->SetPara(src_shard_node_address_.c_str(), root);
  get_task_manager()->PushBackTask(dump_table_task);
  return true;
}

bool ExpandClusterMission::LoadTable() {

  ExpandClusterTask *load_table_task =
      new ExpandClusterTask("Expand_Load_Table", related_id_.c_str(), this);
  load_table_task->AddNodeSubChannel(
      dst_shard_node_address_.c_str(),
      g_node_channel_manager.getNodeChannel(dst_shard_node_address_.c_str()));

  const Json::Value &request_json_body = get_body_json_document();
  Json::Value root;
  root["command_name"] = "myloader";
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = load_table_task->get_task_spec_info();

  char myloader_arg_buf[2048] = {'\0'};
  sprintf(myloader_arg_buf,
          " -h %s -u pgx -p pgx_pwd -P %d -e -d %s --logfile %s/myloader.log",
          dst_shard_node_address_.c_str(), dst_shard_node_port_,
          mydumper_tmp_data_dir_.c_str(), mydumper_tmp_data_dir_.c_str(),
          table_list_str_storage_.c_str());

  Json::Value paras;
  paras.append(myloader_arg_buf);
  root["para"] = paras;
  load_table_task->SetPara(dst_shard_node_address_.c_str(), root);
  get_task_manager()->PushBackTask(load_table_task);
  return true;
}

bool ExpandClusterMission::TableCatchUp() {

  ExpandClusterTask *table_catchup_task =
      new ExpandClusterTask("Expand_Catchup_Table", related_id_.c_str(), this);
  table_catchup_task->AddNodeSubChannel(
      dst_shard_node_address_.c_str(),
      g_node_channel_manager.getNodeChannel(dst_shard_node_address_.c_str()));

  const Json::Value &request_json_body = get_body_json_document();
  Json::Value root;
  root["command_name"] = "tablecatchup";
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = table_catchup_task->get_task_spec_info();

  const Json::Value &orig_request = get_body_json_document()["paras"];
  char table_catchup_arg_buf[8192] = {'\0'};
  sprintf(table_catchup_arg_buf,
          " -src_shard_id=%s -src_addr=%s -src_port=%d -src_user=pgx"
          " -src_pass=pgx_pwd"
          " -dst_shard_id=%s -dst_addr=%s -dst_port=%d -dst_user=pgx"
          " -dst_pass=pgx_pwd"
          " -meta_url=%s -cluster_id=%s"
          " -table_list=%s -mydumper_metadata_file=%s/metadata"
          " -expand_info_suffix=%s"
          " -logger_directory=TMP_DATA_PATH_PLACE_HOLDER/cluster_request_%s"
          " -table_move_job_log_id=%s",
          orig_request["src_shard_id"].asString().c_str(),
          src_shard_node_address_.c_str(), src_shard_node_port_,
          orig_request["dst_shard_id"].asString().c_str(),
          dst_shard_node_address_.c_str(), dst_shard_node_port_,
          meta_cluster_url_.c_str(),
          orig_request["cluster_id"].asString().c_str(),
          table_list_str_.c_str(), mydumper_tmp_data_dir_.c_str(),
          get_request_unique_id().c_str(), get_request_unique_id().c_str(),
          related_id_.c_str());
  Json::Value paras;
  paras.append(table_catchup_arg_buf);
  root["para"] = paras;
  table_catchup_task->SetPara(dst_shard_node_address_.c_str(), root);
  get_task_manager()->PushBackTask(table_catchup_task);
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
  char meta_url_buff[2048] = {'\0'};
  sprintf(meta_url_buff, "%s:%s@\\(%s:%d\\)/mysql", meta_svr_user.c_str(),
          meta_svr_pwd.c_str(), meta_svr_ip.c_str(), meta_svr_port);
  meta_cluster_url_ = meta_url_buff;

  // table_list_str_
  struct {
    std::string db = "";
    std::string schema = "";
    std::string table = "";
  } table_spec_;

  Json::Value table_list = get_body_json_document()["paras"]["table_list"];
  for (auto i = 0; i != table_list.size(); i++) {
    std::string item = table_list[i].asString();
    // tokenize to table_spec_
    std::vector<std::string> tokenized = kunlun::StringTokenize(item, ".");
    if (tokenized.size() != 3) {
      setErr("table_list format is illegal,should by db.schema.table");
      return false;
    }
    table_spec_.db = tokenized[0];
    table_spec_.schema = tokenized[1];
    table_spec_.table = tokenized[2];

    std::string dspc = "";
    dspc += table_spec_.db;
    dspc += "_\\$\\$_";
    dspc += table_spec_.schema;

    if (i == 0) {
      table_list_str_.append(item);
      table_list_str_storage_.append(dspc + "." + table_spec_.table);
    } else {
      table_list_str_.append(",");
      table_list_str_.append(item);
      table_list_str_storage_.append(",");
      table_list_str_storage_.append(dspc + "." + table_spec_.table);
    }
  }
  // related_id banded with table_move_jobs
  bzero((void *)sql_stmt, 1024);
  result.Clean();

  sprintf(sql_stmt,
          "select related_id from kunlun_metadata_db.cluster_general_job_log "
          "where id = %s",
          get_request_unique_id().c_str());
  ret = meta_conn->ExcuteQuery(sql_stmt, &result);
  if (ret != 0 || result.GetResultLinesNum() < 1) {
    setErr("can't fetch valid `related_id`, connection info: %s , or select "
           "result is null",
           meta_conn->getErr());
    return false;
  }
  related_id_ = result[0]["related_id"];

  return true;
}
bool ExpandClusterMission::TearDownMission() {

  syslog(Logger::INFO, "teardown phase");
  return true;
}
bool ExpandClusterMission::TransferFile() { return true; }

std::string ExpandClusterMission::get_table_list_str() const {
  return table_list_str_;
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

  return true;
}
bool ExpandClusterTask::TaskReportImpl() {
  // here report the
  std::string task_spec = get_task_spec_info();
  kunlun::MysqlConnection *conn = g_node_channel_manager.get_meta_conn();
  kunlun::MysqlResult result;
  int ret = 0;
  char sql[4096] = {'\0'};

  Json::Value root = get_response()->get_all_response_json();
  bool task_ok = get_response()->ok();
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  std::string memo = writer.write(root);
  if (task_spec == "Expand_Make_Dir") {
    sprintf(sql,
            "update kunlun_metadata_db.table_move_jobs set memo = '%s',"
            "table_list='%s', src_shard = %s, dest_shard = %s where "
            "id = %s",
            memo.c_str(), mission_ptr_->get_table_list_str().c_str(),
            mission_ptr_->get_body_json_document()["paras"]["src_shard_id"]
                .asString()
                .c_str(),
            mission_ptr_->get_body_json_document()["paras"]["dst_shard_id"]
                .asString()
                .c_str(),
            related_id_.c_str());
    ret = conn->ExcuteQuery(sql, &result);
  } else if (task_spec == "Expand_Dump_Table") {
    sprintf(sql,
            "update kunlun_metadata_db.table_move_jobs set memo = "
            "'%s',status='transmitted' where "
            "id = %s",
            memo.c_str(), related_id_.c_str());
    ret = conn->ExcuteQuery(sql, &result);
  } else if (task_spec == "Expand_Load_Table") {
    sprintf(sql,
            "update kunlun_metadata_db.table_move_jobs set memo = "
            "'%s',status='loaded' where "
            "id = %s",
            memo.c_str(), related_id_.c_str());
    ret = conn->ExcuteQuery(sql, &result);
  } else if (task_spec == "Expand_Catchup_Table") {
    sprintf(sql,
            "update kunlun_metadata_db.table_move_jobs set memo = "
            "'%s',status='%s',when_ended = CURRENT_TIMESTAMP(6) where "
            "id = %s",
            memo.c_str(), task_ok ? "done" : "failed", related_id_.c_str());
    ret = conn->ExcuteQuery(sql, &result);
  } else {
    //
  }

  if (ret < 0) {
    syslog(Logger::ERROR, "%s", conn->getErr());
  }

  syslog(Logger::INFO, "ExpandClusterTask report: %s,sql: %s, related_id: %s",
         get_task_spec_info(), sql, related_id_.c_str());
  return true;
}
