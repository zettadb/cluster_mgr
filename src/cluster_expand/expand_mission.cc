/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "expand_mission.h"
#include "http_server/node_channel.h"
#include "kl_mentain/shard.h"
#include "util_func/meta_info.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"
#include "json/json.h"

using namespace kunlun;

// http://192.168.0.104:10000/trac/wiki/kunlun.features.design.scale-out
extern GlobalNodeChannelManager *g_node_channel_manager;
extern std::string meta_svr_ip;
extern int64_t meta_svr_port;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;
extern std::string meta_group_seeds;

void Expand_Call_Back(void *cb_context) {
  ExpandClusterTask *task = static_cast<ExpandClusterTask *>(cb_context);

  //  syslog(Logger::INFO, "ExpandTask_Call_Back() response");
}

void ExpandClusterMission::ReportStatus() {
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
    KLOG_INFO("memo is {}", memo);
  }
  kunlun::MysqlResult result;
  int ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if (ret) {
    // KLOG_INFO("Report Request status sql: {} ,failed: {}", sql,
    //         g_node_channel_manager->getErr());
  }
  return;
}

std::string ExpandClusterMission::MakeDir(std::string nodemgr_address,
                                          std::string path_suffix) {

  std::string nodemgr_path_prefix = kunlun::FetchNodemgrTmpDataPath(
      g_node_channel_manager, nodemgr_address.c_str());
  std::string path = nodemgr_path_prefix + "/" + path_suffix;
  brpc::Channel *channel =
      g_node_channel_manager->getNodeChannel(nodemgr_address.c_str());

  ExpandClusterTask *make_dir_task =
      new ExpandClusterTask("Expand_Make_Dir", related_id_.c_str(), this);

  make_dir_task->AddNodeSubChannel(nodemgr_address.c_str(), channel);

  Json::Value root;
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = make_dir_task->get_task_spec_info();
  root["job_type"] = "execute_command";

  Json::Value paras;
  paras["command_name"] = "mkdir -p ";
  Json::Value para_json_array;
  para_json_array.append(path);
  paras["command_para"] = para_json_array;
  root["paras"] = paras;

  make_dir_task->SetPara(nodemgr_address.c_str(), root);
  get_task_manager()->PushBackTask(make_dir_task);
  return path;
}

bool ExpandClusterMission::DumpTable() {

  // MakeDir On Remote NodeMgr
  std::string nodemgr_mydumper_path =
      MakeDir(src_shard_node_address_, mydumper_tmp_data_dir_suffix_);

  brpc::Channel *channel =
      g_node_channel_manager->getNodeChannel(src_shard_node_address_.c_str());

  ExpandClusterTask *dump_table_task =
      new ExpandClusterTask("Expand_Dump_Table", related_id_.c_str(), this);
  dump_table_task->AddNodeSubChannel(src_shard_node_address_.c_str(), channel);

  Json::Value root;
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = dump_table_task->get_task_spec_info();
  root["job_type"] = "execute_command";

  Json::Value paras;
  paras["command_name"] = "mydumper";

  char mydumper_arg_buf[2048] = {'\0'};
  sprintf(mydumper_arg_buf,
          " -h %s -u pgx -p pgx_pwd -P %ld "
          "-o %s -F 20 --logfile %s/mydumper.log "
          "-c -T %s --no-backup-locks",
          src_shard_node_address_.c_str(), src_shard_node_port_,
          nodemgr_mydumper_path.c_str(), nodemgr_mydumper_path.c_str(),
          table_list_str_storage_.c_str());

  Json::Value para_json_array;
  para_json_array.append(mydumper_arg_buf);
  paras["command_para"] = para_json_array;
  root["paras"] = paras;
  dump_table_task->SetPara(src_shard_node_address_.c_str(), root);
  get_task_manager()->PushBackTask(dump_table_task);
  return true;
}

bool ExpandClusterMission::CompressDumpedFile() {

  std::string nodemgr_path_prefix = kunlun::FetchNodemgrTmpDataPath(
      g_node_channel_manager, src_shard_node_address_.c_str());

  std::string path = nodemgr_path_prefix + mydumper_tmp_data_dir_suffix_;
  brpc::Channel *channel =
      g_node_channel_manager->getNodeChannel(src_shard_node_address_.c_str());
  ExpandClusterTask *compress_dumped_file_task =
      new ExpandClusterTask("Expand_Compress_file", related_id_.c_str(), this);
  compress_dumped_file_task->AddNodeSubChannel(src_shard_node_address_.c_str(),
                                               channel);

  Json::Value root;
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = compress_dumped_file_task->get_task_spec_info();
  root["job_type"] = "execute_command";

  Json::Value paras;
  paras["command_name"] = "tar";

  char tar_arg_buf[2048] = {'\0'};
  sprintf(tar_arg_buf, " -C %s/../ -czf %s/../mydumper-export-%s.tgz %s ",
          path.c_str(), path.c_str(), get_request_unique_id().c_str(),
          tarball_name_prefix_.c_str());

  Json::Value para_json_array;
  para_json_array.append(tar_arg_buf);
  paras["command_para"] = para_json_array;
  root["paras"] = paras;

  compress_dumped_file_task->SetPara(src_shard_node_address_.c_str(), root);
  get_task_manager()->PushBackTask(compress_dumped_file_task);
  return true;
}

bool ExpandClusterMission::TransferFile() {

  char buff[4096] = {'\0'};
  sprintf(buff, "cluster_request_%s", get_request_unique_id().c_str());

  std::string nodemgr_path = MakeDir(dst_shard_node_address_, buff);
  // std::string nodemgr_path = MakeDir("192.168.0.128", buff);

  // download_file on destination from source
  ExpandClusterTask *download_file_task = new ExpandClusterTask(
      "Expand_transfer_dumped_file", related_id_.c_str(), this);
  download_file_task->AddNodeSubChannel(
      dst_shard_node_address_.c_str(),
      g_node_channel_manager->getNodeChannel(dst_shard_node_address_.c_str()));
  // download_file_task->AddNodeSubChannel(
  //    "192.168.0.128",
  //    g_node_channel_manager->getNodeChannel("192.168.0.128"));

  Json::Value root;
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = download_file_task->get_task_spec_info();
  root["job_type"] = "execute_command";

  Json::Value paras;
  paras["command_name"] = "download_file";

  int64_t node_mgr_listen_port = kunlun::FetchNodeMgrListenPort(
      g_node_channel_manager, src_shard_node_address_.c_str());
  if (node_mgr_listen_port == -1) {
    KLOG_ERROR("Fetch node_mgr listen port failed, node_mgr address is {}",
               src_shard_node_address_);
  }
  char download_arg_buf[8192] = {'\0'};
  sprintf(download_arg_buf,
          " -url=\"http://%s:%ld/FileService/%s/%s.tgz\" -out_prefix=\"%s\" "
          "-out_filename=\"mydumper_output.tgz\" -output_override=true",
          src_shard_node_address_.c_str(), node_mgr_listen_port, buff,
          tarball_name_prefix_.c_str(), nodemgr_path.c_str());

  Json::Value para_json_array;
  para_json_array.append(download_arg_buf);
  paras["command_para"] = para_json_array;
  root["paras"] = paras;

  download_file_task->SetPara(dst_shard_node_address_.c_str(), root);
  // download_file_task->SetPara("192.168.0.128", root);
  get_task_manager()->PushBackTask(download_file_task);

  // do UnTar
  ExpandClusterTask *untar_task =
      new ExpandClusterTask("Expand_Untar_tarball", related_id_.c_str(), this);
  untar_task->AddNodeSubChannel(
      dst_shard_node_address_.c_str(),
      g_node_channel_manager->getNodeChannel(dst_shard_node_address_.c_str()));
  // untar_task->AddNodeSubChannel(
  //    "192.168.0.128",
  //    g_node_channel_manager->getNodeChannel("192.168.0.128"));

  Json::Value root1;
  root1["cluster_mgr_request_id"] = get_request_unique_id();
  root1["task_spec_info"] = untar_task->get_task_spec_info();
  root1["job_type"] = "execute_command";

  Json::Value paras1;
  paras1["command_name"] = "tar";

  char untar_arg_buff[8192] = {'\0'};
  sprintf(untar_arg_buff, " -C %s -xzf %s/mydumper_output.tgz",
          nodemgr_path.c_str(), nodemgr_path.c_str());

  Json::Value para_json_array1;
  para_json_array1.append(untar_arg_buff);
  paras1["command_para"] = para_json_array1;
  root1["paras"] = paras1;

  untar_task->SetPara(dst_shard_node_address_.c_str(), root1);
  get_task_manager()->PushBackTask(untar_task);

  return true;
}

bool ExpandClusterMission::LoadTable() {
  std::string nodemgr_path_prefix = kunlun::FetchNodemgrTmpDataPath(
      g_node_channel_manager, dst_shard_node_address_.c_str());
  // std::string nodemgr_path_prefix =
  //    kunlun::FetchNodemgrTmpDataPath(g_node_channel_manager,
  //    "192.168.0.128");

  std::string path = nodemgr_path_prefix + mydumper_tmp_data_dir_suffix_;

  brpc::Channel *channel =
      g_node_channel_manager->getNodeChannel(dst_shard_node_address_.c_str());
  // brpc::Channel *channel =
  //    g_node_channel_manager->getNodeChannel("192.168.0.128");

  ExpandClusterTask *load_table_task =
      new ExpandClusterTask("Expand_Load_Table", related_id_.c_str(), this);

  load_table_task->AddNodeSubChannel(
      dst_shard_node_address_.c_str(),
      g_node_channel_manager->getNodeChannel(dst_shard_node_address_.c_str()));
  // load_table_task->AddNodeSubChannel(
  //    "192.168.0.128",
  //    g_node_channel_manager->getNodeChannel("192.168.0.128"));

  Json::Value root;
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = load_table_task->get_task_spec_info();
  root["job_type"] = "execute_command";

  Json::Value paras;
  paras["command_name"] = "myloader";

  char myloader_arg_buf[2048] = {'\0'};
  sprintf(myloader_arg_buf,
          " -h %s -u pgx -p pgx_pwd -P %ld -e -d %s "
          "--max-threads-per-table 5 --ssl-mode DISABLED "
          "--logfile %s/myloader.log",
          dst_shard_node_address_.c_str(), dst_shard_node_port_, path.c_str(),
          path.c_str());

  Json::Value para_json_array;
  para_json_array.append(myloader_arg_buf);
  paras["command_para"] = para_json_array;
  root["paras"] = paras;

  load_table_task->SetPara(dst_shard_node_address_.c_str(), root);
  // load_table_task->SetPara("192.168.0.128", root);
  get_task_manager()->PushBackTask(load_table_task);
  return true;
}

bool ExpandClusterMission::TableCatchUp() {
  std::string nodemgr_path_prefix = kunlun::FetchNodemgrTmpDataPath(
      g_node_channel_manager, dst_shard_node_address_.c_str());
  // std::string nodemgr_path_prefix =
  //    kunlun::FetchNodemgrTmpDataPath(g_node_channel_manager,
  //    "192.168.0.128");

  std::string path = nodemgr_path_prefix + mydumper_tmp_data_dir_suffix_;

  ExpandClusterTask *table_catchup_task =
      new ExpandClusterTask("Expand_Catchup_Table", related_id_.c_str(), this);
  table_catchup_task->AddNodeSubChannel(
      dst_shard_node_address_.c_str(),
      g_node_channel_manager->getNodeChannel(dst_shard_node_address_.c_str()));
  // table_catchup_task->AddNodeSubChannel(
  //    "192.168.0.128",
  //    g_node_channel_manager->getNodeChannel("192.168.0.128"));

  Json::Value root;
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = table_catchup_task->get_task_spec_info();
  root["job_type"] = "execute_command";

  Json::Value paras;
  paras["command_name"] = "tablecatchup";

  const Json::Value &orig_request = get_body_json_document()["paras"];
  char table_catchup_arg_buf[8192] = {'\0'};
  sprintf(table_catchup_arg_buf,
          " -src_shard_id=%s -src_addr=%s -src_port=%ld -src_user=pgx"
          " -src_pass=pgx_pwd"
          " -dst_shard_id=%s -dst_addr=%s -dst_port=%ld -dst_user=pgx"
          " -dst_pass=pgx_pwd"
          " -meta_url=%s -cluster_id=%s"
          " -table_list=%s -mydumper_metadata_file=%s/metadata"
          " -expand_info_suffix=%s"
          " -logger_directory=TMP_DATA_PATH_PLACE_HOLDER/cluster_request_%s"
          " -table_move_job_log_id=%s -drop_old_table=%s",
          orig_request["src_shard_id"].asString().c_str(),
          src_shard_node_address_.c_str(), src_shard_node_port_,
          orig_request["dst_shard_id"].asString().c_str(),
          dst_shard_node_address_.c_str(), dst_shard_node_port_,
          meta_cluster_url_.c_str(),
          orig_request["cluster_id"].asString().c_str(),
          table_list_str_.c_str(), path.c_str(),
          get_request_unique_id().c_str(), get_request_unique_id().c_str(),
          related_id_.c_str(), drop_old_table_ ? "1" : "0");
  Json::Value para_json_array;
  para_json_array.append(table_catchup_arg_buf);
  paras["command_para"] = para_json_array;
  root["paras"] = paras;

  table_catchup_task->SetPara(dst_shard_node_address_.c_str(), root);
  // table_catchup_task->SetPara("192.168.0.128", root);
  get_task_manager()->PushBackTask(table_catchup_task);
  return true;
}

bool ExpandClusterMission::FillRequestBodyStImpl() { return true; }

bool ExpandClusterMission::SetUpMisson() {
  setup_success_ = false;

  KLOG_INFO("setup phase");
  char mydumper_tmp_data_dir[1024] = {'\0'};
  sprintf(mydumper_tmp_data_dir, "/cluster_request_%s/mydumper-export-%s",
          get_request_unique_id().c_str(), get_request_unique_id().c_str());
  mydumper_tmp_data_dir_suffix_ = std::string(mydumper_tmp_data_dir);
  // fetch source/target MySQL instance address
  Json::Value root = get_body_json_document();

  std::string src_shard_id = root["paras"]["src_shard_id"].asString();
  std::string dst_shard_id = root["paras"]["dst_shard_id"].asString();
  if (root["paras"].isMember("drop_old_table")) {
    std::string vl = root["paras"]["drop_old_table"].asString();
    if (vl == "1") {
      this->drop_old_table_ = true;
    }
  }

  char sql_stmt[1024] = {'\0'};
  // deal src shard meta info
  sprintf(sql_stmt,
          "select hostaddr,port from kunlun_metadata_db.shard_nodes where  "
          "member_state = 'source' and shard_id = %s",
          src_shard_id.c_str());
  kunlun::MysqlResult result;
  bool ret = g_node_channel_manager->send_stmt(sql_stmt, &result, stmt_retries);
  if (ret) {
    setErr("%s", g_node_channel_manager->getErr());
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
  sprintf(sql_stmt,
          "select hostaddr,port from kunlun_metadata_db.shard_nodes "
          "where member_state = 'source' and shard_id = %s",
          dst_shard_id.c_str());
  ret = g_node_channel_manager->send_stmt(sql_stmt, &result, stmt_retries);
  if (ret) {
    setErr("%s", g_node_channel_manager->getErr());
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
  sprintf(meta_url_buff, "%s:%s@\\(%s:%ld\\)/mysql", meta_svr_user.c_str(),
          meta_svr_pwd.c_str(), meta_svr_ip.c_str(), meta_svr_port);
  meta_cluster_url_ = meta_url_buff;

  // table_list_str_
  struct {
    std::string db = "";
    std::string schema = "";
    std::string table = "";
  } table_spec_;

  Json::Value table_list = get_body_json_document()["paras"]["table_list"];
  if (!get_body_json_document()["paras"].isMember("table_list") ||
      !table_list.isArray() || table_list.size() == 0) {
    // pick table by
    kunlun::MysqlConnectionOption option;
    option.ip = src_shard_node_address_;
    option.port_num = src_shard_node_port_;
    option.user = meta_svr_user;
    option.password = meta_svr_pwd;
    auto src_conn = new kunlun::MysqlConnection(option);
    shuffle_policy_ = new RandomShufflePolicy(src_conn);

    std::vector<std::string> tbl_list_vec;
    int ret = shuffle_policy_->GetTableList(tbl_list_vec);
    if (!ret) {
      setErr("%s", shuffle_policy_->getErr());
      return false;
    }
    table_list_str_ = shuffle_policy_->get_table_list_str();
    table_list_str_storage_ = shuffle_policy_->get_table_list_str_for_storage();

  } else {
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
  }
  // related_id banded with table_move_jobs
  bzero((void *)sql_stmt, 1024);
  result.Clean();

  sprintf(sql_stmt,
          "select related_id from kunlun_metadata_db.cluster_general_job_log "
          "where id = %s",
          get_request_unique_id().c_str());
  ret = g_node_channel_manager->send_stmt(sql_stmt, &result, stmt_retries);
  if (ret || result.GetResultLinesNum() < 1) {
    setErr("can't fetch valid `related_id`, connection info: %s , or select "
           "result is null",
           g_node_channel_manager->getErr());
    return false;
  }
  related_id_ = result[0]["related_id"];
  tarball_name_prefix_ = "mydumper-export-" + get_request_unique_id();

  setup_success_ = true;
  return true;
}
bool ExpandClusterMission::TearDownMission() {
  if (setup_success_ == false) {
    std::string sql = kunlun::string_sprintf(
        "update kunlun_metadata_db.cluster_general_job_log set status='failed' "
        ",memo = '%s' where id = %s",
        getErr(), get_request_unique_id().c_str());
    kunlun::MysqlResult result;
    int ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if (ret) {
      KLOG_INFO("Report Request status sql: {} ,failed: {}", sql,
                g_node_channel_manager->getErr());
    }
  }
  KLOG_INFO("teardown phase");
  return true;
}

std::string ExpandClusterMission::get_table_list_str() const {
  return table_list_str_;
}

bool ExpandClusterMission::ArrangeRemoteTask() {
  if (get_init_by_recover_flag() == true) {
    return ArrangeRemoteTaskForRecover();
  }

  // Step1: Dump table
  if (!DumpTable()) {
    return false;
  }
  if (!CompressDumpedFile()) {
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
  // Reroute will be done during the CatchUp stage
  if (!TableCatchUp()) {
    return false;
  }

  return true;
}

bool ExpandClusterMission::ArrangeRemoteTaskForRecover() {
  char sql_buff[2048] = {'\0'};
  sprintf(sql_buff,
          "select * from kunlun_metadata_db.table_move_jobs where id = %s",
          related_id_.c_str());
  std::string last_state = "not_started";
  kunlun::MysqlResult result;
  // kunlun::MysqlConnection *meta_conn =
  // g_node_channel_manager.get_meta_conn();
  int ret = g_node_channel_manager->send_stmt(sql_buff, &result, stmt_retries);
  if (ret) {
    setErr("%s", g_node_channel_manager->getErr());
    return false;
  }
  last_state = result[0]["status"];
  if (last_state == "not_started") {
    // we can just redo current job as if a new normal job
    return ArrangeRemoteTask();
  } else if (last_state == "dumped") {
    // we arrange the task begin with Transfer_file
    {
      // Step2: Transfer dumped files
      if (!TransferFile()) {
        return false;
      }

      // Step3: Load data through Myloader
      if (!LoadTable()) {
        return false;
      }

      // Step4: SetUp the incremental data sync
      // Reroute will be done during the CatchUp stage
      if (!TableCatchUp()) {
        return false;
      }

      return true;
    }
  } else if (last_state == "transmitted") {
    // we arrange the task begin with Load_file
    {
      // Step3: Load data through Myloader
      if (!LoadTable()) {
        return false;
      }

      // Step4: SetUp the incremental data sync
      // Reroute will be done during the CatchUp stage
      if (!TableCatchUp()) {
        return false;
      }

      return true;
    }
  } else if (last_state == "loaded") {
    // we arrange the task begin with catch_up
    {
      // Step4: SetUp the incremental data sync
      // Reroute will be done during the CatchUp stage
      if (!TableCatchUp()) {
        return false;
      }
      return true;
    }
  } else {
    KLOG_INFO("undefined recover status {} operation", last_state.c_str());
    return true;
  }
  // unreachable
  return true;
}
bool ExpandClusterTask::TaskReportImpl() {
  // here report the
  bool not_defined_stage = false;
  std::string task_spec = get_task_spec_info();
  // kunlun::MysqlConnection *conn = g_node_channel_manager.get_meta_conn();
  kunlun::MysqlResult result;
  bool ret = false;
  char sql[4096] = {'\0'};

  Json::Value root = get_response()->get_all_response_json();
  bool task_ok = get_response()->ok();
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  std::string memo = writer.write(root);
  if (task_spec == "Expand_Dump_Table") {
    sprintf(sql,
            "update kunlun_metadata_db.table_move_jobs set memo = "
            "'%s',table_list = '%s',src_shard=%s"
            ",dest_shard=%s,status='dumped' where "
            "id = %s",
            memo.c_str(), mission_ptr_->get_table_list_str().c_str(),
            mission_ptr_->get_body_json_document()["paras"]["src_shard_id"]
                .asString()
                .c_str(),
            mission_ptr_->get_body_json_document()["paras"]["dst_shard_id"]
                .asString()
                .c_str(),
            related_id_.c_str());
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  } else if (task_spec == "Expand_transfer_dumped_file") {
    sprintf(sql,
            "update kunlun_metadata_db.table_move_jobs set memo = "
            "'%s',status='transmitted' where "
            "id = %s",
            memo.c_str(), related_id_.c_str());
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  } else if (task_spec == "Expand_Load_Table") {
    sprintf(sql,
            "update kunlun_metadata_db.table_move_jobs set memo = "
            "'%s',status='loaded' where "
            "id = %s",
            memo.c_str(), related_id_.c_str());
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  } else if (task_spec == "Expand_Catchup_Table") {
    sprintf(sql,
            "update kunlun_metadata_db.table_move_jobs set memo = "
            "'%s',status='%s',when_ended = CURRENT_TIMESTAMP(6) where "
            "id = %s",
            memo.c_str(), task_ok ? "done" : "failed", related_id_.c_str());
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  } else {
    not_defined_stage = true;
  }

  if (ret) {
    KLOG_ERROR("{}", g_node_channel_manager->getErr());
  }

  if (not_defined_stage) {
    KLOG_INFO("ExpandClusterTask report: report info not defined in current "
              "stage: {}",
              task_spec);
  } else {
    KLOG_INFO("ExpandClusterTask report: %s,sql: {}, related_id: {}",
              get_task_spec_info(), sql, related_id_.c_str());
  }
  return true;
}

bool GetExpandTableListMission::SetUpMisson() {
  setup_success_ = false;

  kunlun::StorageShardConnection conn(meta_group_seeds,meta_svr_user,meta_svr_pwd);
  int ret = conn.init();
  if(!ret){
    KLOG_ERROR("{}",conn.getErr());
    setErr("%s",conn.getErr());
    return false;
  }

  auto conn_ptr = conn.get_master();
  if(conn_ptr == nullptr){
    KLOG_ERROR("{}",conn.getErr());
    setErr("%s",conn.getErr());
    return false;
  }
  
  shuffle_policy_ = new RandomShufflePolicy(conn_ptr);

  std::vector<std::string> tbl_list_vec;
  ret = shuffle_policy_->GetTableList(tbl_list_vec);
  if (!ret) {
    setErr("%s", shuffle_policy_->getErr());
    return false;
  }
  table_list_str_ = shuffle_policy_->get_table_list_str();
  setup_success_ = true;
  return true;
}

bool GetExpandTableListMission::TearDownMission() {

  if(setup_success_){

  }

  return true;
}
