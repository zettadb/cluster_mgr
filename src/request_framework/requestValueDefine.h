/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _request_value_define_h_
#define _request_value_define_h_

#include <string>
#define KUNLUN_JSON_BODY_VERSION "1.0"
#define KUNLUN_METADATA_DB_NAME "kunlun_metadata_db"

namespace kunlun {
enum RequestStatus {
  NOT_STARTED = 1,
  ON_GOING,
  DONE,
  FAILED,

  // status should add above
  UNDIFINED = 1000
};

enum ClusterRequestTypes {
  kRequestTypeUndefined = 0,
  kExampleRequestType,
  kRenameClusterType,
  kCreateClusterType,
  kDeleteClusterType,
  kComputeNodeAddType,
  kComputeNodeDeleteType,
  kShardAddType,
  kShardDropType,
  kShardNodeAddType,
  kShardNodeDropType,
  kClusterExpandType,
  kCreateMachineType,
  kUpdateMachineType,
  kDeleteMachineType,
  kCreateBackupStorageType,
  kUpdateBackupStorageType,
  kDeleteBackupStorageType,

  kSyncReturnType = 300,
  kGetStatusType,
  kGetMetaModeType,
  kGetMetaType,
  kGetBackupStorageType,

  // addtional type should add above
  kRequestTypeMax = 1000
};

typedef struct RequestBody_ {
  std::string version = "";
  std::string request_id = "";
  std::string job_type_str = "";
  std::string timestamp = "";
  std::string user_name = "";
  void *paraList = nullptr;
} RequestBody;

inline bool ValidRpcBodyProtocal(std::string &version) {
  // TODO: ValidRpcBodyProtocal
  return true;
}
} // namespace kunlun

#endif /*_request_value_define_h_*/
