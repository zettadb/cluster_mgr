/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "requestBase.h"
#include "util_func/meta_info.h"
#include <stdio.h>
#include <stdlib.h>

void ClusterRequest::SetUp()
{
  SetUpImpl();
  status_ = ON_GOING;
  ReportStatus();
}

void ClusterRequest::DealRequest()
{
  DealRequestImpl(); 
  ReportStatus();
}
void ClusterRequest::TearDown()
{
  TearDownImpl();
  ReportStatus();
}

void ClusterRequest::ReportStatus(){
  syslog(Logger::INFO,"Request %s status: %d",request_unique_id_,status_);
}

void ClusterRequest::set_status(RequestStatus status)
{
  std::lock_guard<std::mutex> lock(mtx_);
  status_ = status;
}
RequestStatus ClusterRequest::get_status()
{
  std::lock_guard<std::mutex> lock(mtx_);
  return status_;
}

bool ClusterRequest::FillRequestBodySt()
{
  // parse version
  if (!body_json_document_.isMember("version"))
  {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setExtraErr("miss `version` key-value pair in the request body");
    return false;
  }
  request_body_.version = body_json_document_["version"].asString();

  // parse job_type
  if (!body_json_document_.isMember("job_type"))
  {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setExtraErr("missing `job_type` key-value pair in the request body");
    return false;
  }
  request_body_.job_type_str = body_json_document_["job_type"].asString();
  bool ret = kunlun::RecognizedJobTypeStr(request_body_.job_type_str);
  if (!ret)
  {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setExtraErr("Unrecognized job_type: %s", request_body_.job_type_str.c_str());
    return false;
  }
  // parse job_id

  request_body_.request_id = body_json_document_["job_id"].asString();

  // parse timestamp

  if (!body_json_document_.isMember("timestamp"))
  {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setExtraErr("missing `timestamp` key-value pair in the request body");
    return false;
  }
  request_body_.timestamp = body_json_document_["timestamp"].asString();
  // parse user_name
  if (!body_json_document_.isMember("user_name"))
  {
    set_err_num(EIVALID_REQUEST_PROTOCAL);
    setExtraErr("missing `user_name` key-value pair in the request body");
    return false;
  }
  request_body_.user_name = body_json_document_["user_name"].asString();


  return FillRequestBodyStImpl(); // Drived Class to implemented for parsing the individual paralist
}

RequestBody ClusterRequest::get_request_body()
{
  if (request_body_.job_type_str.empty())
  {
    // not init
    FillRequestBodySt();
  }
  return request_body_;
}

void ClusterRequest::set_request_unique_id(std::string &unique_id)
{
  request_unique_id_ = unique_id;
}

ClusterRequestTypes ClusterRequest::get_request_type()
{
  if (request_types_ == kunlun::kRequestTypeUndefined)
  {
    request_types_ = kunlun::GetReqTypeEnumByStr(
        body_json_document_["job_type"].asString().c_str());
  }
  return request_types_;
}

// return empty string if error occur
std::string ClusterRequest::get_request_unique_id()
{
  return request_unique_id_;
}

bool ClusterRequest::ParseBodyToJson(const std::string &raw_body) { return true; }
