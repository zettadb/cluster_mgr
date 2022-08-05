/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "requestBase.h"
#include "http_server/node_channel.h"
#include "util_func/meta_info.h"
#include "zettalib/op_log.h"
#include "zettalib/op_mysql.h"
#include "zettalib/tool_func.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>

extern GlobalNodeChannelManager* g_node_channel_manager;
extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

void ClusterRequest::SetUp() {
  SetUpImpl();
  status_ = ON_GOING;
  ReportStatus();
}

void ClusterRequest::DealRequest() {
  DealRequestImpl();
  ReportStatus();
}
void ClusterRequest::TearDown() {
  TearDownImpl();
  ReportStatus();
}

void ClusterRequest::ReportStatus() {
  KLOG_INFO("Request %s status: {}", request_unique_id_, status_);
}

void ClusterRequest::set_status(RequestStatus status) {
  KlWrapGuard<KlWrapMutex> guard(mtx_);
  status_ = status;
  
}
RequestStatus ClusterRequest::get_status() {
  KlWrapGuard<KlWrapMutex> guard(mtx_);
  return status_;
}

bool ClusterRequest::FillRequestBodySt() {
  // parse version
  if (!body_json_document_.isMember("version")) {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setExtraErr("miss `version` key-value pair in the request body");
    return false;
  }
  request_body_.version = body_json_document_["version"].asString();

  // parse job_type
  if (!body_json_document_.isMember("job_type")) {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setExtraErr("missing `job_type` key-value pair in the request body");
    return false;
  }
  request_body_.job_type_str = body_json_document_["job_type"].asString();
  bool ret = kunlun::RecognizedJobTypeStr(request_body_.job_type_str);
  if (!ret) {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setExtraErr("Unrecognized job_type: %s",
                request_body_.job_type_str.c_str());
    return false;
  }
  // parse job_id

  request_body_.request_id = body_json_document_["job_id"].asString();

  // parse timestamp

  if (!body_json_document_.isMember("timestamp")) {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setExtraErr("missing `timestamp` key-value pair in the request body");
    return false;
  }
  request_body_.timestamp = body_json_document_["timestamp"].asString();
  // parse user_name
  if (!body_json_document_.isMember("user_name")) {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setExtraErr("missing `user_name` key-value pair in the request body");
    return false;
  }
  request_body_.user_name = body_json_document_["user_name"].asString();

  return FillRequestBodyStImpl(); // Drived Class to implemented for parsing the
                                  // individual paralist
}

RequestBody ClusterRequest::get_request_body() {
  if (request_body_.job_type_str.empty()) {
    // not init
    FillRequestBodySt();
  }
  return request_body_;
}

void ClusterRequest::set_request_unique_id(std::string &unique_id) {
  request_unique_id_ = unique_id;
}

ClusterRequestTypes ClusterRequest::get_request_type() {
  if (request_types_ == kunlun::kRequestTypeUndefined) {
    request_types_ = kunlun::GetReqTypeEnumByStr(
        body_json_document_["job_type"].asString().c_str());
  }
  return request_types_;
}

// return empty string if error occur
std::string ClusterRequest::get_request_unique_id() {
  return request_unique_id_;
}

void ClusterRequest::set_init_by_recover_flag(bool init_by_recover) {
  init_by_recover_ = init_by_recover;
}

bool ClusterRequest::get_init_by_recover_flag() { return init_by_recover_; }

void ClusterRequest::set_default_del_request(bool del_request) {
  default_del_request_ = del_request;
}

bool ClusterRequest::get_default_del_request() { return default_del_request_; }

bool ClusterRequest::ParseBodyToJson(const std::string &raw_body) {
  return true;
}

void ClusterRequest::GenerateRequestUniqueId() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("open shard connection for metadata cluster faild: {}",
               conn.getErr());
    return;
  }
  auto conn_ptr = conn.get_master();
  kunlun::MysqlResult result;

  char buff[2048] = {'\0'};
  snprintf(buff,2048, "begin");
  ret = conn_ptr->ExcuteQuery(buff, &result);

  bzero((void *)buff, 2048);
  result.Clean();
  snprintf(buff,2048, "insert into kunlun_metadata_db.cluster_general_job_log set "
                 "user_name='internal_user'");
  ret = conn_ptr->ExcuteQuery(buff, &result);
  if (ret <= 0) {
    KLOG_ERROR("generate unique request id faild: {}", conn_ptr->getErr());
    return;
  }

  bzero((void *)buff, 2048);
  result.Clean();
  snprintf(buff,2048, "select last_insert_id() as insert_id");
  ret = conn_ptr->ExcuteQuery(buff, &result);
  if (ret != 0) {
    KLOG_ERROR(
        "fetch last_insert_id during request unique id generation faild: {}",
        conn_ptr->getErr());
    return ;
  }
  request_unique_id_ = result[0]["insert_id"];

  snprintf(buff,2048, "commit");
  ret = conn_ptr->ExcuteQuery(buff, &result);


  return ;
}
bool ClusterRequest::InitFromInternal() { return false; }
void ClusterRequest::CompleteGeneralJobInfo(){return ;}
void ClusterRequest::FillCommonJobJsonDoc(Json::Value &doc) {
  GenerateRequestUniqueId();
  time_t epoch = 0;

  doc["version"] = "1.0";
  doc["job_id"] = request_unique_id_;
  doc["job_type"] = "";
  doc["timestamp"] = "test";
  doc["user_name"] = "internal_user";
  doc["paras"] = "";
}
