/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "http_server.h"
#include "bthread/bthread.h"
#include "butil/iobuf.h"
#include "cluster_expand/expand_mission.h"
#include "example_mission/example_mission.h"
#include "sync_mission/sync_mission.h"
#include "machine_mission/machine_mission.h"
#include "backup_storage/backup_storage.h"
#include "cluster_mission/cluster_mission.h"
#include "request_framework/missionRequest.h"
#include "strings.h"
#include "util_func/error_code.h"
#include "util_func/meta_info.h"

typedef struct AsynArgs_ {
  HttpServiceImpl *http_serivce_impl;
  ClusterRequest *cluster_request;
} AsynArgs;

static void *AsyncDispatchRequest(void *args) {
  AsynArgs *asynargs = static_cast<AsynArgs *>(args);
  ClusterRequest *request = asynargs->cluster_request;
  asynargs->http_serivce_impl->get_request_handle_thread()->DispatchRequest(
      request);
  delete asynargs;
  return nullptr;
}

std::string
HttpServiceImpl::MakeErrorInstantResponseBody(const char *error_msg) {
  // Json format
  Json::Value root;
  root["version"] = KUNLUN_JSON_BODY_VERSION;
  root["error_code"] = "0";
  root["error_info"] = error_msg;
  root["status"] = "failed";
  Json::Value attachment;
  root["attachment"] = attachment;

  Json::FastWriter writer;
  return writer.write(root);
}

std::string
HttpServiceImpl::MakeAcceptInstantResponseBody(ClusterRequest *request) {
  // Json format
  Json::Value root;
  root["version"] = KUNLUN_JSON_BODY_VERSION;
  root["error_code"] = EintToStr(EOK);
  root["job_id"] = request->get_request_unique_id();
  root["status"] = "accept";
  Json::Value attachment;
  root["attachment"] = attachment;

  Json::FastWriter writer;
  return writer.write(root);
}

std::string
HttpServiceImpl::MakeSyncOkResponseBody(ClusterRequest *request) {
  // Json format
  Json::Value root;
  root["version"] = KUNLUN_JSON_BODY_VERSION;
  root["error_code"] = EintToStr(EOK);
  root["status"] = "succeed";
  root["attachment"] = request->get_body_json_attachment();

  Json::FastWriter writer;
  return writer.write(root);
}

void HttpServiceImpl::Emit(google::protobuf::RpcController *cntl_base,
                           const HttpRequest *request, HttpResponse *response,
                           google::protobuf::Closure *done) {
  brpc::ClosureGuard done_gurad(done);

  ClusterRequest *inner_request = GenerateRequest(cntl_base);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  if (!inner_request) {
    cntl->http_response().set_content_type("text/plain");
    butil::IOBufBuilder info_buffer;
    info_buffer << MakeErrorInstantResponseBody(getErr()) << '\n';
    info_buffer.move_to(cntl->response_attachment());
    return;
  }

  if(inner_request->get_request_type() < kSyncReturnType) { //for aSyncTask
    // dispatch the request
    // this option may block ( request queue may full ),
    // so we use bthread to invoke it
    bthread_t bid;
    // this object will free at AsyncDispatchRequest()
    AsynArgs *asynargs = new AsynArgs();
    asynargs->http_serivce_impl = this;
    asynargs->cluster_request = inner_request;

    if (bthread_start_background(&bid, NULL, AsyncDispatchRequest, asynargs) !=
        0) {
      cntl->http_response().set_content_type("text/plain");
      butil::IOBufBuilder info_buffer;
      info_buffer << MakeErrorInstantResponseBody(
                        "Kunlun Cluster deal request faild")
                  << '\n';
      info_buffer.move_to(cntl->response_attachment());
      syslog(Logger::ERROR, "start bthread to dispathc the request failed");
      return;
    }

    // TODO: make a wrapper to do this json stuff
    cntl->http_response().set_content_type("text/plain");
    butil::IOBufBuilder info_buffer;
    info_buffer << MakeAcceptInstantResponseBody(inner_request) << '\n';
    info_buffer.move_to(cntl->response_attachment());
  } else {  //for SyncReturn

    if(!inner_request->SetUpSyncTaskImpl()){
      cntl->http_response().set_content_type("text/plain");
      butil::IOBufBuilder info_buffer;
      info_buffer << MakeErrorInstantResponseBody(
                        "SetUpSyncTaskImpl faild")
                  << '\n';
      info_buffer.move_to(cntl->response_attachment());
      return;
    }

    // TODO: make a wrapper to do this json stuff
    cntl->http_response().set_content_type("text/plain");
    butil::IOBufBuilder info_buffer;
    info_buffer << MakeSyncOkResponseBody(inner_request) << '\n';
    info_buffer.move_to(cntl->response_attachment());
  }

  // here done_guard will be release and _done->Run() will be invoked
  return;
}

MissionRequest *HttpServiceImpl::MissionRequestFactory(Json::Value *doc) {
  std::string job_type = (*doc)["job_type"].asString();
  if (job_type.empty()) {
    setErr("The field `job_type` in Body must be a non empty valid string");
    return nullptr;
  }
  kunlun::ClusterRequestTypes request_type =
      kunlun::GetReqTypeEnumByStr(job_type.c_str());

  MissionRequest *request = nullptr;
  switch (request_type) {
  case kunlun::kClusterExpandType:
    request = new kunlun::ExpandClusterMission(doc);
    break;
  case kunlun::kExampleRequestType:
    request = new kunlun::ExampleMission(doc);
    break;
  case kunlun::kCreateMachineType:
  case kunlun::kUpdateMachineType:
  case kunlun::kDeleteMachineType:
    request = new kunlun::MachineMission(doc);
    break;
  case kunlun::kCreateBackupStorageType:
  case kunlun::kUpdateBackupStorageType:
  case kunlun::kDeleteBackupStorageType:
    request = new kunlun::BackupStorage(doc);
    break;
  case kunlun::kRenameClusterType:
  case kunlun::kCreateClusterType:
  case kunlun::kDeleteClusterType:
    request = new kunlun::ClusterMission(doc);
    break;
    // TODO: Add more above

  //kSyncReturnType
  case kunlun::kGetStatusType:
  case kunlun::kGetMetaModeType:
  case kunlun::kGetMetaType:
    request = new kunlun::SyncMission(doc);
    break;

  default:
    setErr("Unrecongnized job type");
    break;
  }
  return request;
}

bool HttpServiceImpl::ParseBodyToJsonDoc(const std::string &raw_body,
                                         Json::Value *doc) {
  Json::Reader reader;
  reader.parse(raw_body.c_str(), *doc);
  if (!reader.good()) {
    setErr("JSON parse error: %s, JSON string: %s",
           reader.getFormattedErrorMessages().c_str(), raw_body.c_str());
    return false;
  }

  // job_type is the only requeired filed in the request body
  if (!doc->isMember("job_type")) {
    setErr("missing `job_type` key-value pair in the request body");
    return false;
  }
  // TODO: addtional key valid check
  return true;
}

ClusterRequest *
HttpServiceImpl::GenerateRequest(google::protobuf::RpcController *cntl_base) {

  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
  brpc::HttpMethod request_method = cntl->http_request().method();

  if (request_method == brpc::HTTP_METHOD_POST) {
    // parse Body
    Json::Value body_json_doc;
    bool ret = ParseBodyToJsonDoc(cntl->request_attachment().to_string(),
                                  &body_json_doc);
    if (!ret) {
      return nullptr;
    }

    // generate missionRequest
    MissionRequest *mission_request = MissionRequestFactory(&body_json_doc);
    if (!mission_request) {
      // setErr() involked in Factory method
      // error info buffer has be already filled
      return nullptr;
    }

    //SyncReturn needn't to get request_id
    if(mission_request->get_request_type() > kSyncReturnType)
      return mission_request;

    // Generate request id here
    std::string request_id = GenerateRequestUniqueId(mission_request);
    if (request_id.empty()) {
      return nullptr;
    }
    mission_request->set_request_unique_id(request_id);
    return mission_request;
  }
  if (request_method == brpc::HTTP_METHOD_GET) {
    // TODO: generate fetchRequest
  }
  return nullptr;
}

void HttpServiceImpl::set_request_handle_thread(
    HandleRequestThread *thread_handler) {
  request_handle_thread_ = thread_handler;
}

void HttpServiceImpl::set_meta_cluster_mysql_conn(
    kunlun::MysqlConnection *conn) {
  meta_cluster_mysql_conn_ = conn;
}

HandleRequestThread *HttpServiceImpl::get_request_handle_thread() {
  return request_handle_thread_;
}

std::string
HttpServiceImpl::GenerateRequestUniqueId(ClusterRequest *inner_request) {
  char sql_buffer[4096] = {'\0'};
  kunlun::MysqlResult query_result;
  sprintf(sql_buffer, "begin");
  int ret = meta_cluster_mysql_conn_->ExcuteQuery(sql_buffer, &query_result);
  if (ret != 0) {
    setExtraErr("%s", meta_cluster_mysql_conn_->getErr());
    return "";
  }
  bzero((void *)sql_buffer, (size_t)4096);
  query_result.Clean();
  sprintf(
      sql_buffer,
      "insert into %s.cluster_general_job_log set job_type='%s',user_name='%s'",
      KUNLUN_METADATA_DB_NAME,
      inner_request->get_request_body().job_type_str.c_str(),
      inner_request->get_request_body().user_name.c_str());
  ret = meta_cluster_mysql_conn_->ExcuteQuery(sql_buffer, &query_result);
  if (ret != 1) {
    setExtraErr("%s", meta_cluster_mysql_conn_->getErr());
    return "";
  }
  bzero((void *)sql_buffer, (size_t)4096);
  query_result.Clean();
  sprintf(sql_buffer, "select last_insert_id() as insert_id");
  ret = meta_cluster_mysql_conn_->ExcuteQuery(sql_buffer, &query_result);
  if (ret != 0) {
    setErr("%s", meta_cluster_mysql_conn_->getErr());
    return "";
  }
  bzero((void *)sql_buffer, (size_t)4096);

  std::string last_insert_id = std::string(query_result[0]["insert_id"]);
  Json::Value job_info = inner_request->get_body_json_document();
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  std::string job_info_str = writer.write(job_info);

  std::string related_id = FetchRelatedIdInSameSession(
      meta_cluster_mysql_conn_, inner_request->get_request_body().job_type_str);
  if (related_id.empty()) {
    syslog(Logger::INFO, "Get empty related_id: %s", getErr());
  }
  query_result.Clean();
  sprintf(
      sql_buffer,
      "update %s.cluster_general_job_log set related_id= '%s',job_info='%s' "
      "where id = %s",
      KUNLUN_METADATA_DB_NAME, related_id.c_str(), job_info_str.c_str(),
      last_insert_id.c_str());
  ret = meta_cluster_mysql_conn_->ExcuteQuery(sql_buffer, &query_result);
  if (ret != 1) {
    setErr("%s", meta_cluster_mysql_conn_->getErr());
    return "";
  }
  bzero((void *)sql_buffer, (size_t)4096);
  query_result.Clean();
  sprintf(sql_buffer, "commit");
  ret = meta_cluster_mysql_conn_->ExcuteQuery(sql_buffer, &query_result);
  if (ret != 0) {
    setErr("%s", meta_cluster_mysql_conn_->getErr());
    return "";
  }
  return last_insert_id;
}

// **related id**
//
// For every job which recived by cluster_mgr, an unique record exists in
// kunlun_metadata_db.cluster_general_job_log which related to job itself
//
// But different job type may be consist of the different job info. For
// instance, table_move_job may have the different progressive information
// which generated during the job dealing procedure.
//
// Thus, we employ an new table `kunlun_metadata_db.table_move_job` to
// store the info mentioned above.
//
// From this point of view, we need an extra attribute to associate
// cluster_general_job_log and the table_move_job, which is implemented by
// using the cluster_general_job_log.related_id as the logical constrain
// bettwen two tables above.
//
// In the `Table moving` or `cluster expanding` sence, this filed `related_id`
// is associate with the table_move_kob.id.
//

std::string
HttpServiceImpl::FetchRelatedIdInSameSession(kunlun::MysqlConnection *conn,
                                             std::string job_type) {

  char sql[2048] = {'\0'};
  kunlun::MysqlResult result;
  std::string related_id = "";
  int ret = 0;

  kunlun::ClusterRequestTypes request_type =
      kunlun::GetReqTypeEnumByStr(job_type.c_str());

  switch (request_type) {
  case kunlun::kClusterExpandType:
    sprintf(sql, "insert into kunlun_metadata_db.table_move_jobs set "
                 "tab_file_format='logical'");
    ret = conn->ExcuteQuery(sql, &result);
    if (ret != 1) {
      setErr("%s", conn->getErr());
      break;
    }

    bzero((void *)sql, 2048);
    result.Clean();

    sprintf(sql, "select last_insert_id() as id");
    ret = conn->ExcuteQuery(sql, &result);
    if (ret != 0) {
      setErr("%s", conn->getErr());
      break;
    }
    related_id = result[0]["id"];
    break;
    // TODO: Add more above
  default:
    related_id = "0";
    //setErr("Undeal job type %s, related_id not set", job_type.c_str());
    break;
  }
  return related_id;
}

extern std::string meta_svr_ip;
extern int64_t meta_svr_port;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

int64_t cluster_mgr_brpc_http_port;

void SetInteruptedJobAsFaild(kunlun::MysqlConnection *meta_conn,
                             const char *request_id) {}
// **Recover Job from startup**
//
// When Cluster_mgr start or failover from another instance,there may be exists
// the interupted job caused by the failover.
//
// RecoverInteruptedJobIfExists is used to dealing such scenario.
// The roughly procedure is designed like described below:
//
// 1. We scan the cluster_general_job_log to find the job which status is nether
// `done` nor `failed`.
// 2. if job status is `not_starting`, we just abandon this job
// 3. if job status is `ongoing`, wo recover this job, marke the job as recoverd
// to identify that needs special task arranging tactic.
// 4. push the recoved job to the handle_request_thread just like the normal
// request.
bool HttpServiceImpl::RecoverInteruptedJobIfExists() {

  char sql_buff[4096] = {'\0'};
  sprintf(sql_buff, "select * from kunlun_metadata_db.cluster_general_job_log "
                    "where status <> 'done' and status <> 'failed'");
  kunlun::MysqlResult result;
  int ret = meta_cluster_mysql_conn_->ExcuteQuery(sql_buff, &result);
  if (ret < 0) {
    setErr("%s",meta_cluster_mysql_conn_->getErr());
    return false;
  }
  for (int i = 0; i < result.GetResultLinesNum(); i++) {
    std::string status = result[i]["status"];
    if (status == "not_started") {
      SetInteruptedJobAsFaild(meta_cluster_mysql_conn_, result[i]["id"]);
      continue;
    }
    // regenerate the job and push to the request_handle_thread
    std::string request_id = result[i]["id"];
    std::string job_info = result[i]["job_info"];
    Json::Value body_json_doc;
    Json::Reader reader;
    bool ret = reader.parse(job_info, body_json_doc);
    if (!ret) {
      SetInteruptedJobAsFaild(meta_cluster_mysql_conn_, request_id.c_str());
      syslog(Logger::ERROR, "Recover Interupted Job failed: %s",
             reader.getFormattedErrorMessages().c_str());
      continue;
    }
    MissionRequest *mission_request = MissionRequestFactory(&body_json_doc);
    if(mission_request == nullptr){
      syslog(Logger::ERROR,"recover interupted job %s on startup failed: %s",request_id.c_str(),this->getErr());
      continue;
    }
    mission_request->set_request_unique_id(request_id);
    // we indicate current job as init_by_recover
    mission_request->set_init_by_recover_flag(true);
    //dispatch the job
    request_handle_thread_->DispatchRequest(mission_request);
    syslog(Logger::INFO,"dispatch the recovered job %s success",request_id.c_str());
  }
  return true;
}

brpc::Server *NewHttpServer() {

  HandleRequestThread *request_handle_thread = new HandleRequestThread();
  int ret = request_handle_thread->start();
  if (ret < 0) {
    syslog(Logger::ERROR, "Handle request thread start faild");
    delete request_handle_thread;
    return nullptr;
  }

  kunlun::MysqlConnectionOption option;
  option.ip = meta_svr_ip;
  option.port_num = meta_svr_port;
  option.user = meta_svr_user;
  option.password = meta_svr_pwd;

  kunlun::MysqlConnection *conn = new MysqlConnection(option);
  ret = conn->Connect();
  if (!ret) {
    syslog(Logger::ERROR, "%s", conn->getErr());
    delete conn;
    return nullptr;
  }

  HttpServiceImpl *service = new HttpServiceImpl();
  service->set_request_handle_thread(request_handle_thread);
  service->set_meta_cluster_mysql_conn(conn);

  ret = service->RecoverInteruptedJobIfExists();
  if(ret == false){
    syslog(Logger::ERROR,"Error Happened in recover interupted job func: %s",service->getErr());
  }

  brpc::Server *server = new brpc::Server();
  if (server->AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    syslog(Logger::ERROR, "Add service to brpc::Server failed,");
    return nullptr;
  }
  brpc::ServerOptions *options = new brpc::ServerOptions();
  options->idle_timeout_sec = -1;
  options->num_threads = 1;
  if (server->Start(cluster_mgr_brpc_http_port, options) != 0) {
    syslog(Logger::ERROR, "http server start failed,");
    return nullptr;
  }
  return server;
}
