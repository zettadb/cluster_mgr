/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "meta_info.h"
#include "arpa/inet.h"
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
  case "shard_node_create"_hash:
    type_enum = kShardNodeAddType;
    break;
  case "shard_node_drop"_hash:
    type_enum = kShardNodeDropType;
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

  case "get_status"_hash:
    type_enum = kGetStatusType;
    break;
  case "get_meta_mode"_hash:
    type_enum = kGetMetaModeType;
    break;
  case "get_meta"_hash:
    type_enum = kGetMetaType;
    break;
  case "get_backup_storage"_hash:
    type_enum = kGetBackupStorageType;
    break;

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

std::string FetchNodemgrTmpDataPath(MysqlConnection *meta, const char *ip) {
  char sql[2048] = {'\0'};
  sprintf(sql,
          "select nodemgr_tmp_data_abs_path as path"
          " from kunlun_metadata_db.server_nodes"
          " where hostaddr='%s'",
          ip);
  kunlun::MysqlResult result;
  int ret = meta->ExcuteQuery(sql, &result);
  if (ret < 0) {
    return "";
  }
  return result[0]["path"];
}

int64_t FetchNodeMgrListenPort(MysqlConnection *meta, const char *ip) {
  char sql[2048] = {'\0'};
  sprintf(sql,
          "select nodemgr_port as port from kunlun_metadata_db.server_nodes "
          "where hostaddr='%s'",
          ip);

  kunlun::MysqlResult result;
  int ret = meta->ExcuteQuery(sql, &result);
  if (ret < 0) {
    return -1;
  }
  return ::atoi(result[0]["port"]);
}

}; // namespace kunlun
