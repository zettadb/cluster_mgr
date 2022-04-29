/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _CLUSTER_MGR_HTTP_SERVER_H_
#define _CLUSTER_MGR_HTTP_SERVER_H_

#include "brpc/server.h"
#include "kl_mentain/log.h"
#include "proto/clustermng.pb.h"
#include "request_framework/handleRequestThread.h"
#include "request_framework/missionRequest.h"
#include "request_framework/requestBase.h"
#include "request_framework/requestValueDefine.h"
#include "util_func/error_code.h"
#include "zettalib/errorcup.h"
#include "zettalib/op_mysql.h"
#include "json/json.h"

using namespace kunlunrpc;
class HttpServiceImpl : public HttpService,
                        public kunlun::ErrorCup,
                        public kunlun::GlobalErrorNum {
public:
  HttpServiceImpl() {
    request_handle_thread_ = nullptr;
    meta_cluster_mysql_conn_ = nullptr;
  };
  virtual ~HttpServiceImpl(){};
  void Emit(google::protobuf::RpcController *, const HttpRequest *,
            HttpResponse *, google::protobuf::Closure *);

  bool RecoverInteruptedJobIfExists();
  ClusterRequest *GenerateRequest(google::protobuf::RpcController *);
  MissionRequest *MissionRequestFactory(Json::Value *);

  void set_request_handle_thread(HandleRequestThread *);
  void set_meta_cluster_mysql_conn(kunlun::MysqlConnection *);
  HandleRequestThread *get_request_handle_thread();

  std::string MakeErrorInstantResponseBody(const char *);
  std::string MakeAcceptInstantResponseBody(ClusterRequest *);
  std::string MakeSyncOkResponseBody(ClusterRequest *);

  std::string GenerateRequestUniqueId(ClusterRequest *);
  std::string FetchRelatedIdInSameSession(kunlun::MysqlConnection *,
                                          std::string);
  bool ParseBodyToJsonDoc(const std::string &, Json::Value *);

private:
  HandleRequestThread *request_handle_thread_;
  kunlun::MysqlConnection *meta_cluster_mysql_conn_;
};

extern brpc::Server *NewHttpServer();
extern void RecoverInteruptedJobIfExists(kunlun::MysqlConnection *,
                                         HandleRequestThread *);
extern void SetInteruptedJobAsFaild(kunlun::MysqlConnection *, const char *);
#endif /*_CLUSTER_MGR_HTTP_SERVER_H_*/
