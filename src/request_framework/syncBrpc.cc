/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "syncBrpc.h"
#include "http_server/node_channel.h"
#include "kl_mentain/log.h"
#include "stdio.h"
#include "string.h"

static void CallBC(brpc::Controller *cntl, SyncBrpc *syncBrpc) {
  if (!cntl->Failed()) {
    syncBrpc->response = cntl->response_attachment().to_string();
    syncBrpc->result = true;
    return;
  }else
	  syncBrpc->result = false;
  
  syslog(Logger::ERROR, "%s", cntl->ErrorText().c_str());
}

bool SyncBrpc::syncBrpcToNode(std::string &hostaddr, Json::Value &para) {

  auto channel = g_node_channel_manager.getNodeChannel(hostaddr.c_str());
  if(channel == nullptr)
	  return false;

  kunlunrpc::HttpService_Stub stub(channel);
  brpc::Controller *cntl = new brpc::Controller();
  google::protobuf::Closure *done = brpc::NewCallback(&CallBC, cntl, this);

  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  std::string body = writer.write(para);
  cntl->request_attachment().append(body);
  cntl->http_request().set_method(brpc::HTTP_METHOD_POST);

  stub.Emit(cntl, nullptr, nullptr, done);
  brpc::Join(cntl->call_id());

  return true;
}
