#include "postgresInstallRemote.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"
#include "kl_mentain/shard.h"
#include <cstdlib>

using namespace kunlun;
using namespace std;
extern GlobalNodeChannelManager *g_node_channel_manager;
extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

bool PostgresInstallRemoteTask::InitInstanceInfo(
    vector<ComputeInstanceInfoSt> &info_vec) {

  auto iter = info_vec.begin();
  for (; iter != info_vec.end(); iter++) {
    ComputeInstanceInfoSt info_st = *iter;

    string key =
        kunlun::string_sprintf("%s_%lu", info_st.ip.c_str(), info_st.pg_port);
    auto found = instance_info_.find(key);
    if (found != instance_info_.end()) {
      KLOG_WARN("%s instance info already exists, new value %s will replace "
                "the original one %s",
                key.c_str(), info_st.to_string().c_str(),
                found->second.to_string().c_str());
    }
    instance_info_[key] = info_st;
  }
  return ComposeNodeChannelsAndParas();
}
bool PostgresInstallRemoteTask::InitInstanceInfoOneByOne(
    ComputeInstanceInfoSt &info_st) {
  string key =
      kunlun::string_sprintf("%s_%lu", info_st.ip.c_str(), info_st.pg_port);
  auto found = instance_info_.find(key);
  if (found != instance_info_.end()) {
    KLOG_WARN("%s instance info already exists, new value %s will replace "
              "the original one %s",
              key.c_str(), info_st.to_string().c_str(),
              found->second.to_string().c_str());
  }
  instance_info_[key] = info_st;
  return ComposeOneNodeChannelAndPara(key);
}

bool PostgresInstallRemoteTask::ComposeOneNodeChannelAndPara(std::string key) {
  // Get Related Channel handler
  auto found = instance_info_.find(key);
  if (found == instance_info_.end()) {
    setErr("Can not find %s related instance info", key.c_str());
    return false;
  }
  ComputeInstanceInfoSt info_st = found->second;
  auto channel = g_node_channel_manager->getNodeChannel(info_st.ip.c_str());
  AddNodeSubChannel(key.c_str(), channel);

  // Set Related paras
  Json::Value root;
  root["cluster_mgr_request_id"] = request_id_;
  root["task_spec_info"] = this->get_task_spec_info();
  root["job_type"] = "install_postgres";

  Json::Value paras;
  paras["command_name"] = "install-pg-rbr.py";

  paras["pg_protocal_port"] = std::to_string(info_st.pg_port);
  paras["mysql_protocal_port"] = std::to_string(info_st.mysql_port);
  paras["exporter_port"] = std::to_string(info_st.exporter_port);
  paras["compute_node_id"] = std::to_string(info_st.compute_id);
  paras["user"] = info_st.init_user;
  paras["password"] = info_st.init_pwd;
  paras["bind_address"] = info_st.ip;

  root["paras"] = paras;
  SetPara(key.c_str(), root);
  return true;
}

bool PostgresInstallRemoteTask::ComposeNodeChannelsAndParas() {
  auto iter = instance_info_.begin();
  for (; iter != instance_info_.end(); iter++) {
    bool ret = ComposeOneNodeChannelAndPara(iter->first);
    if (!ret) {
      return false;
    }
  }
  return true;
}

bool PostgresInstallRemoteTask::Init_mysql_pg_intall_log_table_id() {
  // init cluster_coldbackups table info
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  auto conn_ptr = conn.get_master();

  kunlun::MysqlResult result;
  char sql[8192] = {'\0'};
  bzero((void *)sql, 8192);
  snprintf(sql, 8192, "begin");
  ret = conn_ptr->ExcuteQuery(sql, &result);

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192,
           "insert into kunlun_metadata_db.mysql_pg_install_log "
           "(general_log_id, install_type,status,job_info,memo) values "
           "(%s,'postgres','not_started','storage','')",
           request_id_.c_str());
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0) {
    KLOG_ERROR(
        "fetch last_insert_id faild during the PostgresInstall remote task {}",
        conn_ptr->getErr());
  }

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "select last_insert_id() as last_insert_id");
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0 || result.GetResultLinesNum() != 1) {
    KLOG_ERROR(
        "fetch last_insert_id faild during the PostgresInstall remote task {}",
        conn_ptr->getErr());
  }
  mysql_pg_install_log_id_ = result[0]["last_insert_id"];

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "commit");
  ret = conn_ptr->ExcuteQuery(sql, &result);
  return true;
}

void PostgresInstallRemoteTask::SetUpStatus() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  ret = Init_mysql_pg_intall_log_table_id();
  if (!ret) {
    KLOG_ERROR("{}", getErr());
  }
  auto conn_ptr = conn.get_master();
  std::string job_info = getTaskInfo();
  char sql[10240] = {'\0'};
  snprintf(sql, 10240,
           "update kunlun_metadata_db.mysql_pg_install_log set status = "
           "'ongoing',job_info='%s' where id = %s",
           job_info.c_str(), mysql_pg_install_log_id_.c_str());
  kunlun::MysqlResult result;
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0) {
    KLOG_ERROR("Report Request status sql: {} ,failed: {}", sql,
               conn_ptr->getErr());
  } else {
    KLOG_INFO("Report Request status sql: {} ", sql);
  }
  return;
}

bool PostgresInstallRemoteTask::TaskReportImpl() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  auto conn_ptr = conn.get_master();
  // write result into cluster
  std::string memo;
  Json::Value root = get_response()->get_all_response_json();
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  memo = writer.write(root);
  bool ok = get_response()->ok();

  char sql[8192] = {'\0'};
  snprintf(sql, 8192,
           "update kunlun_metadata_db.mysql_pg_install_log set "
           "memo='%s',status='%s' where "
           "id = %s limit 1",
           memo.c_str(), ok ? "done" : "failed",
           mysql_pg_install_log_id_.c_str());

  kunlun::MysqlResult result;
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0) {
    KLOG_ERROR("Report Request status sql: {} ,failed: {}", sql,
               conn_ptr->getErr());
  } else {
    KLOG_INFO("Report Request status sql: {} ", sql);
  }
  return true; 
}

bool PostgresUninstallRemoteTask::InitInstanceInfo(
    vector<ComputeInstanceInfoSt> &info_vec) {

  auto iter = info_vec.begin();
  for (; iter != info_vec.end(); iter++) {
    ComputeInstanceInfoSt info_st = *iter;

    string key =
        kunlun::string_sprintf("%s_%lu", info_st.ip.c_str(), info_st.pg_port);
    auto found = instance_info_.find(key);
    if (found != instance_info_.end()) {
      KLOG_WARN("%s instance info already exists, new value %s will replace "
                "the original one %s",
                key.c_str(), info_st.to_string().c_str(),
                found->second.to_string().c_str());
    }
    instance_info_[key] = info_st;
  }
  return ComposeNodeChannelsAndParas();
}
bool PostgresUninstallRemoteTask::InitInstanceInfoOneByOne(
    ComputeInstanceInfoSt &info_st) {
  string key =
      kunlun::string_sprintf("%s_%lu", info_st.ip.c_str(), info_st.pg_port);
  auto found = instance_info_.find(key);
  if (found != instance_info_.end()) {
    KLOG_WARN("%s instance info already exists, new value %s will replace "
              "the original one %s",
              key.c_str(), info_st.to_string().c_str(),
              found->second.to_string().c_str());
  }
  instance_info_[key] = info_st;
  return ComposeOneNodeChannelAndPara(key);
}

bool PostgresUninstallRemoteTask::ComposeOneNodeChannelAndPara(
    std::string key) {
  // Get Related Channel handler
  auto found = instance_info_.find(key);
  if (found == instance_info_.end()) {
    setErr("Can not find %s related instance info", key.c_str());
    return false;
  }
  ComputeInstanceInfoSt info_st = found->second;
  auto channel = g_node_channel_manager->getNodeChannel(info_st.ip.c_str());
  AddNodeSubChannel(key.c_str(), channel);

  // Set Related paras
  Json::Value root;
  root["cluster_mgr_request_id"] = request_id_;
  root["task_spec_info"] = this->get_task_spec_info();
  root["job_type"] = "uninstall_postgres";

  Json::Value paras;
  paras["command_name"] = "uninstall-pg-rbr.py";

  paras["pg_protocal_port"] = std::to_string(info_st.pg_port);
  paras["mysql_protocal_port"] = std::to_string(info_st.mysql_port);
  paras["exporter_port"] = std::to_string(info_st.exporter_port);
  paras["compute_node_id"] = std::to_string(info_st.compute_id);
  paras["user"] = info_st.init_user;
  paras["password"] = info_st.init_pwd;
  paras["bind_address"] = info_st.ip;

  root["paras"] = paras;
  SetPara(key.c_str(), root);
  return true;
}

bool PostgresUninstallRemoteTask::ComposeNodeChannelsAndParas() {
  auto iter = instance_info_.begin();
  for (; iter != instance_info_.end(); iter++) {
    bool ret = ComposeOneNodeChannelAndPara(iter->first);
    if (!ret) {
      return false;
    }
  }
  return true;
}

bool PostgresUninstallRemoteTask::Init_mysql_pg_intall_log_table_id() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  auto conn_ptr = conn.get_master();
  // init cluster_coldbackups table info
  kunlun::MysqlResult result;
  char sql[8192] = {'\0'};
  bzero((void *)sql, 8192);
  snprintf(sql, 8192, "begin");
  ret = conn_ptr->ExcuteQuery(sql, &result);

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192,
           "insert into kunlun_metadata_db.mysql_pg_install_log "
           "(general_log_id, install_type,status,job_info,memo) values "
           "(%s,'uninstall_postgres','not_started','storage','')",
           request_id_.c_str());
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0) {
    KLOG_ERROR("fetch last_insert_id faild during the PostgresUninstall remote "
               "task {}",
               conn_ptr->getErr());
  }

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "select last_insert_id() as last_insert_id");
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0 || result.GetResultLinesNum() != 1) {
    KLOG_ERROR("fetch last_insert_id faild during the PostgresUninstall remote "
               "task {}",
               conn_ptr->getErr());
  }
  mysql_pg_install_log_id_ = result[0]["last_insert_id"];

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "commit");
  ret = conn_ptr->ExcuteQuery(sql, &result);
  return true;
}

void PostgresUninstallRemoteTask::SetUpStatus() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  ret = Init_mysql_pg_intall_log_table_id();
  if (!ret) {
    KLOG_ERROR("{}", getErr());
  }
  auto conn_ptr = conn.get_master();
  std::string job_info = getTaskInfo();
  char sql[10240] = {'\0'};
  snprintf(sql, 10240,
           "update kunlun_metadata_db.mysql_pg_install_log set status = "
           "'ongoing',job_info='%s' where id = %s",
           job_info.c_str(), mysql_pg_install_log_id_.c_str());
  kunlun::MysqlResult result;
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0) {
    KLOG_ERROR("Report Request status sql: {} ,failed: {}", sql,
               conn_ptr->getErr());
  } else {
    KLOG_INFO("Report Request status sql: {} ", sql);
  }
  return;
}

bool PostgresUninstallRemoteTask::TaskReportImpl() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  auto conn_ptr = conn.get_master();
  // write result into cluster
  std::string memo;
  Json::Value root = get_response()->get_all_response_json();
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  memo = writer.write(root);
  bool ok = get_response()->ok();

  char sql[8192] = {'\0'};
  snprintf(sql, 8192,
           "update kunlun_metadata_db.mysql_pg_install_log set "
           "memo='%s',status='%s' where "
           "id = %s limit 1",
           memo.c_str(), ok ? "done" : "failed",
           mysql_pg_install_log_id_.c_str());

  kunlun::MysqlResult result;
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0) {
    KLOG_ERROR("Report Request status sql: {} ,failed: {}", sql,
               conn_ptr->getErr());
  } else {
    KLOG_INFO("Report Request status sql: {} ", sql);
  }
  return true;
}