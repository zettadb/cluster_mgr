/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _KUNLUN_CLUSTER_MNG_REQUEST_H_
#define _KUNLUN_CLUSTER_MNG_REQUEST_H_

//#include <mutex>
#include "brpc/channel.h"
#include "brpc/server.h"
#include "util_func/object_ptr.h"
#include "zettalib/op_mysql.h"
#include "json/json.h"
#include "requestValueDefine.h"
#include "zettalib/errorcup.h"
#include "util_func/error_code.h"
#include "http_server/proto/clustermng.pb.h"
#include "util_func/kl_mutex.h"

// from proto define
using namespace kunlunrpc;
using namespace kunlun;

// we use this class to build the asynchronous process of the request
class ClusterRequest : public ErrorCup, public ObjectRef, public GlobalErrorNum
{

public:
  ClusterRequest(){
    status_ = NOT_STARTED;
    request_types_ = kRequestTypeUndefined;
    init_by_recover_ = false;
    default_del_request_ = true;
  };
  explicit ClusterRequest(Json::Value *doc)
  {
    body_json_document_ = *doc;
    status_ = NOT_STARTED;
    request_types_ = kRequestTypeUndefined;
    init_by_recover_ = false;
    default_del_request_ = true;
  }
  virtual ~ClusterRequest() {}

  // sync task impl
  virtual bool SetUpSyncTaskImpl() = 0;

  void SetUp();
  // Derived class should implament it
  // Invoked by SetUp()
  virtual void SetUpImpl() = 0;
  virtual int GetErrorCode() = 0;
  void DealRequest();
  // Derived class should implament it
  // Invoked by DealRequestImpl()
  virtual void DealRequestImpl() = 0;

  // the response will be sent in TearDown()
  void TearDown();
  // Derived class should implament it
  // Invoked by TearDown()
  virtual void TearDownImpl() = 0;

  // getter & setter
  void set_status(RequestStatus);
  RequestStatus get_status();

  void set_init_by_recover_flag(bool init_by_recover);
  bool get_init_by_recover_flag();

  void set_default_del_request(bool del_request);
  bool get_default_del_request();
  
  std::string get_request_unique_id();
  void set_request_unique_id(std::string &);

  const Json::Value &get_body_json_document() const { return body_json_document_; }
  const Json::Value &get_body_json_attachment() const { return body_json_attachment_; }
  void set_body_json_attachment(Json::Value & attachment) { body_json_attachment_ = attachment; }
  void set_body_json_document(Json::Value & doc) { body_json_document_ = doc; }

  RequestBody get_request_body();
  bool FillRequestBodySt();
  virtual bool FillRequestBodyStImpl() = 0;

  ClusterRequestTypes get_request_type();

  bool ParseBodyToJson(const std::string &);

  virtual void ReportStatus();

  void GenerateRequestUniqueId();
  virtual void CompleteGeneralJobInfo();

  //used for initialize from internal useage
  virtual bool InitFromInternal();

  // forbid copy
  ClusterRequest(const ClusterRequest &) = delete;
  ClusterRequest &operator=(const ClusterRequest &) = delete;
protected:
  void FillCommonJobJsonDoc(Json::Value &doc);

private:
  enum RequestStatus status_;
  enum ClusterRequestTypes request_types_;
  //std::mutex mtx_;
  KlWrapMutex mtx_;
  std::string request_unique_id_;
  Json::Value body_json_document_;
  Json::Value body_json_attachment_;
  RequestBody request_body_;
  bool init_by_recover_;
  bool default_del_request_;
};

#endif /*_KUNLUN_CLUSTER_MNG_REQUEST_H_*/
