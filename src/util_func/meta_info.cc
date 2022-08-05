/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "meta_info.h"
#include "arpa/inet.h"
#include "zettalib/tool_func.h"
#include "kl_mentain/shard.h"
#include <error.h>
#include <string>
#include <time.h>
#include <unistd.h>

namespace kunlun {

typedef std::uint64_t hash_t;
constexpr hash_t prime = 0x100000001B3ull;
constexpr hash_t basis = 0xCBF29CE484222325ull;

static hash_t hash_(char const *str) {
  hash_t ret{basis};
  while (*str) {
    ret ^= *str;
    ret *= prime;
    str++;
  }
  return ret;
}

constexpr hash_t hash_compile_time(char const *str, hash_t last_value = basis) {
  return *str ? hash_compile_time(str + 1, (*str ^ last_value) * prime)
              : last_value;
}

constexpr unsigned long long operator"" _hash(char const *p, size_t) {
  return hash_compile_time(p);
}

ClusterRequestTypes GetReqTypeEnumByStr(const char *type_str) {
  ClusterRequestTypes type_enum = kRequestTypeUndefined;
  switch (hash_(type_str)) {
  case "example_mission"_hash:
    type_enum = kExampleRequestType;
    break;
  case "rename_cluster"_hash:
    type_enum = kRenameClusterType;
    break;
  case "create_cluster"_hash:
    type_enum = kCreateClusterType;
    break;
  case "delete_cluster"_hash:
    type_enum = kDeleteClusterType;
    break;
  case "add_shards"_hash:
    type_enum = kAddShardsType;
    break;
  case "delete_shard"_hash:
    type_enum = kDeleteShardType;
    break;
  case "add_comps"_hash:
    type_enum = kAddCompsType;
    break;
  case "delete_comp"_hash:
    type_enum = kDeleteCompType;
    break;
  case "add_nodes"_hash:
    type_enum = kAddNodesType;
    break;
  case "delete_node"_hash:
    type_enum = kDeleteNodeType;
    break;
  case "backup_cluster"_hash:
    type_enum = kBackupClusterType;
    break;
  case "cluster_restore"_hash:
    type_enum = kRestoreNewClusterType;
    break;
  case "update_cluster_coldback_time_period"_hash:
    type_enum = kUpdateClusterColdBackTimePeriodType;
    break;
  case "manual_backup_cluster"_hash:
    type_enum = kManualBackupClusterType;
    break;
  case "expand_cluster"_hash:
    type_enum = kClusterExpandType;
    break;
  case "create_machine"_hash:
    type_enum = kCreateMachineType;
    break;
  case "update_machine"_hash:
    type_enum = kUpdateMachineType;
    break;
  case "delete_machine"_hash:
    type_enum = kDeleteMachineType;
    break;
  case "create_backup_storage"_hash:
    type_enum = kCreateBackupStorageType;
    break;
  case "update_backup_storage"_hash:
    type_enum = kUpdateBackupStorageType;
    break;
  case "delete_backup_storage"_hash:
    type_enum = kDeleteBackupStorageType;
    break;
  
  case "raft_mission"_hash:
    type_enum = kRaftMissionType;
    break;
    
  case "control_instance"_hash:
    type_enum = kControlInstanceType;
    break;
  case "update_prometheus"_hash:
    type_enum = kUpdatePrometheusType;
    break;
  case "postgres_exporter"_hash:
    type_enum = kPostgresExporterType;
    break;
  case "mysqld_exporter"_hash:
    type_enum = kMysqldExporterType;
    break;

  case "get_status"_hash:
    type_enum = kGetStatusType;
    break;
  case "get_meta_mode"_hash:
    type_enum = kGetMetaModeType;
    break;
  case "get_meta_summary"_hash:
    type_enum = kGetMetaSummaryType;
    break;
  case "get_backup_storage"_hash:
    type_enum = kGetBackupStorageType;
    break;
  case "get_cluster_detail"_hash:
    type_enum = kGetClusterDetailType;
    break;
  case "get_expand_table_list"_hash:
    type_enum = kGetExpandTableListType;
    break;
  case "get_variable"_hash:
    type_enum = kGetVariableType;
    break;
  case "set_variable"_hash:
    type_enum = kSetVariableType;
    break;
  case "rebuild_node"_hash:
    type_enum = kClusterRebuildNodeType;
    break;
    
#ifndef NDEBUG
  case "cluster_debug"_hash:
    type_enum = kClusterDebugType;
    break;
#endif
  // addtional type convert should add above
  default:
    type_enum = kRequestTypeMax;
  }
  return type_enum;
}

bool RecognizedRequestType(ClusterRequestTypes type) {
  return type < kRequestTypeMax && type > kRequestTypeUndefined;
}

bool RecognizedJobTypeStr(std::string &job_type) {
  ClusterRequestTypes type = GetReqTypeEnumByStr(job_type.c_str());
  return RecognizedRequestType(type);
}

std::string GenerateNewClusterIdStr(MysqlConnection *conn) { return ""; }

bool ValidNetWorkAddr(const char *addr) {
  char *dst[512];
  int ret = inet_pton(AF_INET, addr, dst);
  return ret == 1 ? true : false;
}

std::string FetchNodemgrTmpDataPath(GlobalNodeChannelManager *g_channel, const char *ip) {
  char sql[2048] = {'\0'};
  sprintf(sql,
          "select nodemgr_bin_path as bin_path"
          " from kunlun_metadata_db.server_nodes"
          " where hostaddr='%s' ",
          ip);
  kunlun::MysqlResult result;
  int ret = g_channel->send_stmt(sql, &result, 3);
  if (ret) {
    return "";
  }
  std::string bin_path = result[0]["bin_path"];
  std::string base = kunlun::GetBasePath(bin_path);
  return base + "/data";
}

int64_t FetchNodeMgrListenPort(GlobalNodeChannelManager *g_channel, const char *ip) {
  char sql[2048] = {'\0'};
  sprintf(sql,
          "select nodemgr_port as port from kunlun_metadata_db.server_nodes "
          "where hostaddr='%s'",
          ip);

  kunlun::MysqlResult result;
  bool ret = g_channel->send_stmt(sql, &result, 3);
  if (ret) {
    return -1;
  }
  return ::atoi(result[0]["port"]);
}

bool TimePeriod::parse() {

  auto c1 = kunlun::StringTokenize(time_str_, "-");
  if (c1.size() != 2) {
    setErr("misformat of the time period string");
    return false;
  }

  sscanf(c1[0].c_str(), "%d:%d:%d", &(start_.tm_hour),
                   &(start_.tm_min), &(start_.tm_sec));
  sscanf(c1[1].c_str(), "%d:%d:%d", &(stop_.tm_hour), &(stop_.tm_min),
               &(stop_.tm_sec));
  parsed_ = true;
  return true;

}

bool TimePeriod::init(std::string time_str) {

  if (parsed_ && time_str == time_str_) {
    return true;
  }
  time_str_ = time_str;
  parsed_ =false;
  return parse();
}

bool TimePeriod::TimesUp() {
  if(!parsed_){
    return false;
  }

  time_t now = 0;
  time(&now);
  struct tm now_tm;
  if (localtime_r(&now, &now_tm) == NULL) {
    setErr("localtime_r() failed: %s", strerror(errno));
    return false;
  }

  start_.tm_year = now_tm.tm_year;
  start_.tm_mon = now_tm.tm_mon;
  start_.tm_mday = now_tm.tm_mday;
  stop_.tm_year = now_tm.tm_year;
  stop_.tm_mon = now_tm.tm_mon;
  stop_.tm_mday = now_tm.tm_mday;
  time_t start_epoch = mktime(&start_);
  time_t stop_epoch = mktime(&stop_);

  return now > start_epoch && now < stop_epoch;
}

uint64_t GetNowTimestamp() {
    timespec time;
    clock_gettime(CLOCK_REALTIME, &time);
    return time.tv_sec;
}

}; // namespace kunlun
