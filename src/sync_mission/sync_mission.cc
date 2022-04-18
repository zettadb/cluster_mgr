/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "sync_mission.h"
#include "strings.h"
#include "kl_mentain/sys.h"
#include "util_func/error_code.h"
#include "util_func/meta_info.h"

bool SyncMission::GetStatus() {
  Json::Value attachment;
  kunlun::MysqlConnection *meta_conn = g_node_channel_manager.get_meta_conn();
  char sql_buffer[4096] = {'\0'};
  int ret;
  kunlun::MysqlResult query_result;
  sprintf(
      sql_buffer,
      "select status,memo from %s.cluster_general_job_log where id=%s",
      KUNLUN_METADATA_DB_NAME,
      get_request_body().request_id.c_str());
  ret = meta_conn->ExcuteQuery(sql_buffer, &query_result);
  if (ret != 0) {
    setErr("%s", meta_conn->getErr());
    return false;
  }
  
  bzero((void *)sql_buffer, (size_t)4096);
  attachment["status"] = std::string(query_result[0]["status"]);
  attachment["info"] = std::string(query_result[0]["memo"]);
  query_result.Clean();

  set_body_json_attachment(attachment);
  return true;
};

bool SyncMission::GetMetaMode() {
  Json::Value attachment;

  bool ret = System::get_instance()->get_meta_mode(attachment);
  set_body_json_attachment(attachment);

  return ret;
}

bool SyncMission::GetMeta() {
  Json::Value attachment;

  bool ret = System::get_instance()->get_meta(attachment);
  set_body_json_attachment(attachment);

  return ret;
}

bool SyncMission::GetBackupStorage() {
  Json::Value attachment;

  bool ret = System::get_instance()->get_backup_storage(attachment);
  set_body_json_attachment(attachment);

  return ret;
}

