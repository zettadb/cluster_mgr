#include "mysqlInstallRemote.h"
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

bool MySQLInstallRemoteTask::InitInstanceInfo(
    vector<tuple<string, unsigned long, int, int>> &info_vec) {

  auto iter = info_vec.begin();
  for (; iter != info_vec.end(); iter++) {
    InstanceInfoSt info_st;
    info_st.ip = std::get<0>(*iter);
    info_st.port = std::get<1>(*iter);
    info_st.innodb_buffer_size_M = std::get<2>(*iter);
    info_st.db_cfg = std::get<3>(*iter);

    string key =
        kunlun::string_sprintf("%s_%lu", info_st.ip.c_str(), info_st.port);
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
bool MySQLInstallRemoteTask::InitInstanceInfoOneByOne(std::string ip,
                                                      std::string port,
                                                      std::string exporter_port,
                                                      int buffer_size,
                                                      int db_cfg) {
  InstanceInfoSt info_st;
  info_st.ip = ip;
  info_st.port = std::strtoul(port.c_str(), 0, 10);
  info_st.exporter_port = std::strtoul(exporter_port.c_str(), 0, 10);
  info_st.innodb_buffer_size_M = buffer_size;
  info_st.db_cfg = db_cfg;

  string key =
      kunlun::string_sprintf("%s_%lu", info_st.ip.c_str(), info_st.port);
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

bool MySQLInstallRemoteTask::InitInstanceInfoOneByOne(std::string ip,
                                                      unsigned long port,
                                                      unsigned long exporter_port,
                                                      unsigned long mgr_port,
                                                      unsigned long xport,
                                                      std::string mgr_seed,
                                                      std::string mgr_uuid,
                                                      int buffer_size,
                                                      int db_cfg,
                                                      std::string role) {
  InstanceInfoSt info_st;
  info_st.ip = ip;
  info_st.port = port;
  info_st.exporter_port = exporter_port;
  info_st.mgr_port = mgr_port;
  info_st.xport = xport;
  info_st.mgr_seed = mgr_seed;
  info_st.mgr_uuid = mgr_uuid;
  info_st.innodb_buffer_size_M = buffer_size;
  info_st.db_cfg = db_cfg;
  info_st.role = role;

  string key =
      kunlun::string_sprintf("%s_%lu", info_st.ip.c_str(), info_st.port);
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

bool MySQLInstallRemoteTask::ComposeOneNodeChannelAndPara(std::string key) {
  // Get Related Channel handler
  auto found = instance_info_.find(key);
  if (found == instance_info_.end()) {
    setErr("Can not find %s related instance info", key.c_str());
    return false;
  }
  InstanceInfoSt info_st = found->second;
  auto channel = g_node_channel_manager->getNodeChannel(info_st.ip.c_str());
  AddNodeSubChannel(key.c_str(), channel);

  // Set Related paras
  Json::Value root;
  root["cluster_mgr_request_id"] = request_id_;
  root["task_spec_info"] = this->get_task_spec_info();
  root["job_type"] = "install_mysql";

  Json::Value paras;
  paras["command_name"] = "install-mysql-mgr.py";

  paras["port"] = std::to_string(info_st.port);
  paras["exporter_port"] = std::to_string(info_st.exporter_port);
  paras["mgr_port"] = std::to_string(info_st.mgr_port);
  paras["xport"] = std::to_string(info_st.xport);
  paras["mgr_seed"] = info_st.mgr_seed;
  paras["mgr_uuid"] = info_st.mgr_uuid;
  paras["innodb_buffer_size_M"] = std::to_string(info_st.innodb_buffer_size_M);
  paras["db_cfg"] = std::to_string(info_st.db_cfg);
  paras["is_master"] = (info_st.role == "master" ? "1" : "0");
  root["paras"] = paras;
  SetPara(key.c_str(), root);
  return true;
}

bool MySQLInstallRemoteTask::ComposeNodeChannelsAndParas() {
  auto iter = instance_info_.begin();
  for (; iter != instance_info_.end(); iter++) {
    bool ret = ComposeOneNodeChannelAndPara(iter->first);
    if (!ret) {
      return false;
    }
  }
  return true;
}

bool MySQLInstallRemoteTask::Init_mysql_pg_intall_log_table_id() {
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
           "(%s,'mysql','not_started','storage','')",
           request_id_.c_str());
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0) {
    KLOG_ERROR(
        "fetch last_insert_id faild during the MySQLInstall remote task {}",
        conn_ptr->getErr());
  }

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "select last_insert_id() as last_insert_id");
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0 || result.GetResultLinesNum() != 1) {
    KLOG_ERROR(
        "fetch last_insert_id faild during the MySQLInstall remote task {}",
        conn_ptr->getErr());
  }
  mysql_pg_install_log_id_ = result[0]["last_insert_id"];

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "commit");
  ret = conn_ptr->ExcuteQuery(sql, &result);
  return true;
}

void MySQLInstallRemoteTask::SetUpStatus() {
  int ret = Init_mysql_pg_intall_log_table_id();
  if (!ret) {
    KLOG_ERROR("{}", getErr());
  }
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
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

bool MySQLInstallRemoteTask::TaskReportImpl() {
  // write result into cluster
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  auto conn_ptr = conn.get_master();
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

bool MySQLUninstallRemoteTask::InitInstanceInfo(
    vector<tuple<string, unsigned long, int>> &info_vec) {

  auto iter = info_vec.begin();
  for (; iter != info_vec.end(); iter++) {
    InstanceInfoSt info_st;
    info_st.ip = std::get<0>(*iter);
    info_st.port = std::get<1>(*iter);
    info_st.innodb_buffer_size_M = std::get<2>(*iter);

    string key =
        kunlun::string_sprintf("%s_%lu", info_st.ip.c_str(), info_st.port);
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
bool MySQLUninstallRemoteTask::InitInstanceInfoOneByOne(std::string ip,
                                                      std::string port,
                                                      std::string exporter_port,
                                                      int buffer_size) {
  InstanceInfoSt info_st;
  info_st.ip = ip;
  info_st.port = std::strtoul(port.c_str(), 0, 10);
  info_st.exporter_port = std::strtoul(exporter_port.c_str(), 0, 10);
  info_st.innodb_buffer_size_M = buffer_size;

  string key =
      kunlun::string_sprintf("%s_%lu", info_st.ip.c_str(), info_st.port);
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

bool MySQLUninstallRemoteTask::InitInstanceInfoOneByOne(std::string ip,
                                                      unsigned long port,
                                                      unsigned long exporter_port,
                                                      unsigned long mgr_port,
                                                      unsigned long xport,
                                                      int buffer_size) {
  InstanceInfoSt info_st;
  info_st.ip = ip;
  info_st.port = port;
  info_st.exporter_port = exporter_port;
  info_st.mgr_port = mgr_port;
  info_st.xport = xport;
  info_st.innodb_buffer_size_M = buffer_size;

  string key =
      kunlun::string_sprintf("%s_%lu", info_st.ip.c_str(), info_st.port);
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

bool MySQLUninstallRemoteTask::ComposeOneNodeChannelAndPara(std::string key) {
  // Get Related Channel handler
  auto found = instance_info_.find(key);
  if (found == instance_info_.end()) {
    setErr("Can not find %s related instance info", key.c_str());
    return false;
  }
  InstanceInfoSt info_st = found->second;
  auto channel = g_node_channel_manager->getNodeChannel(info_st.ip.c_str());
  AddNodeSubChannel(key.c_str(), channel);

  // Set Related paras
  Json::Value root;
  root["cluster_mgr_request_id"] = request_id_;
  root["task_spec_info"] = this->get_task_spec_info();
  root["job_type"] = "uninstall_mysql";

  Json::Value paras;
  paras["command_name"] = "uninstall-mysql-mgr.py";

  paras["port"] = std::to_string(info_st.port);
  paras["exporter_port"] = std::to_string(info_st.exporter_port);
  paras["mgr_port"] = std::to_string(info_st.mgr_port);
  paras["xport"] = std::to_string(info_st.xport);
  paras["innodb_buffer_size_M"] = std::to_string(info_st.innodb_buffer_size_M);
  root["paras"] = paras;
  SetPara(key.c_str(), root);
  return true;
}

bool MySQLUninstallRemoteTask::ComposeNodeChannelsAndParas() {
  auto iter = instance_info_.begin();
  for (; iter != instance_info_.end(); iter++) {
    bool ret = ComposeOneNodeChannelAndPara(iter->first);
    if (!ret) {
      return false;
    }
  }
  return true;
}

bool MySQLUninstallRemoteTask::Init_mysql_pg_intall_log_table_id() {
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
  conn_ptr->ExcuteQuery(sql, &result);

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192,
           "insert into kunlun_metadata_db.mysql_pg_install_log "
           "(general_log_id, install_type,status,job_info,memo) values "
           "(%s,'uninstall_mysql','not_started','storage','')",
           request_id_.c_str());
  ret = conn_ptr->ExcuteQuery(sql, &result, stmt_retries);
  if (ret < 0) {
    KLOG_ERROR(
        "fetch last_insert_id faild during the MySQLUninstall remote task {}",
        g_node_channel_manager->getErr());
  }

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "select last_insert_id() as last_insert_id");
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0 || result.GetResultLinesNum() != 1) {
    KLOG_ERROR(
        "fetch last_insert_id faild during the MySQLUninstall remote task {}",
        conn_ptr->getErr());
  }
  mysql_pg_install_log_id_ = result[0]["last_insert_id"];

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "commit");
  ret = conn_ptr->ExcuteQuery(sql, &result);
  return true;
}

void MySQLUninstallRemoteTask::SetUpStatus() {
  int ret = Init_mysql_pg_intall_log_table_id();
  if (!ret) {
    KLOG_ERROR("{}", getErr());
  }
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  auto conn_ptr = conn.get_master();
  std::string job_info = getTaskInfo();
  char sql[10240] = {'\0'};
  snprintf(sql, 10240,
           "update kunlun_metadata_db.mysql_pg_install_log set status = "
           "'ongoing',job_info='%s' where id = %s",
           job_info.c_str(), mysql_pg_install_log_id_.c_str());
  kunlun::MysqlResult result;
  ret = conn_ptr->ExcuteQuery(sql, &result, stmt_retries);
  if (ret < 0) {
    KLOG_ERROR("Report Request status sql: {} ,failed: {}", sql,
               conn_ptr->getErr());
  } else {
    KLOG_INFO("Report Request status sql: {} ", sql);
  }
  return;
}

bool MySQLUninstallRemoteTask::TaskReportImpl() {
  // write result into cluster
  std::string memo;
  Json::Value root = get_response()->get_all_response_json();
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  memo = writer.write(root);
  bool ok = get_response()->ok();

  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  auto conn_ptr = conn.get_master();
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
