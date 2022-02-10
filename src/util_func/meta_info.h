/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _KUNLUN_META_INFO_UTIL_FUNC_H_
#define _KUNLUN_META_INFO_UTIL_FUNC_H_
#include "request_framework/requestValueDefine.h"
#include "op_mysql/op_mysql.h"

namespace kunlun
{
  extern std::string GenerateNewClusterIdStr(MysqlConnection *);
  ClusterRequestTypes GetReqTypeEnumByStr(const char *);
  bool RecognizedRequestType(ClusterRequestTypes);
  bool RecognizedJobTypeStr(std::string &);
  bool ValidNetWorkAddr(const char *);
} // namespace kunlun

#endif /*_KUNLUN_META_INFO_UTIL_FUNC_H_*/
