/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "sync_mission.h"
#include "strings.h"
#include "kl_mentain/sys.h"
#include "request_framework/syncBrpc.h"
#include "http_server/node_channel.h"
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

bool SyncMission::GetMetaSummary() {
  Json::Value attachment;
  bool ret = System::get_instance()->get_meta_summary(attachment);
  set_body_json_attachment(attachment);
  return ret;
}

bool SyncMission::GetBackupStorage() {
  Json::Value attachment;
  bool ret = System::get_instance()->get_backup_storage(attachment);
  set_body_json_attachment(attachment);
  return ret;
}

bool SyncMission::GetMachineSummary() {
  Json::Value attachment;

	std::vector<std::string> vec_machine;
	if(!System::get_instance()->get_machine_info_from_metadata(vec_machine)) {
		syslog(Logger::ERROR, "get_machine_info_from_metadata error");
		return false;
	}
  g_node_channel_manager.Init();

  for(auto &machine: vec_machine)	{
      Json::Value root_node;
      Json::Value paras;
      Json::Value list;
      root_node["cluster_mgr_request_id"] = "ping_pong";
      root_node["task_spec_info"] = "ping_pong";
      root_node["job_type"] = "ping_pong";
      root_node["paras"] = paras;

      SyncBrpc syncBrpc;
      if(syncBrpc.syncBrpcToNode(machine, root_node))
        list["status"] = "online";
      else
        list["status"] = "offline";
      
      list["hostaddr"] = machine;
      attachment.append(list);
  }

  set_body_json_attachment(attachment);
  return true;
}

bool SyncMission::GetClusterSummary() {
  Json::Value attachment;
  bool ret = System::get_instance()->get_cluster_summary(attachment);
  set_body_json_attachment(attachment);
  return ret;
}

bool SyncMission::GetClusterDetail() {
  Json::Value attachment;
  std::string cluster_name;
  
  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return false;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  bool ret = System::get_instance()->get_cluster_detail(paras, attachment);
  set_body_json_attachment(attachment);
  return ret;
}

bool SyncMission::GetVariable() {
  Json::Value attachment;
  std::string cluster_name;
  
  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return false;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  bool ret = System::get_instance()->get_variable(paras, attachment);
  set_body_json_attachment(attachment);
  return ret;
}

bool SyncMission::SetVariable() {
  Json::Value attachment;
  std::string cluster_name;
  
  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return false;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  bool ret = System::get_instance()->set_variable(paras, attachment);
  set_body_json_attachment(attachment);
  return ret;
}
