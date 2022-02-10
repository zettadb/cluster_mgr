/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "error_code.h"
namespace kunlun {
const char *g_error_num_str[] = {
    "OK",                                       /*EOFFSET(0)*/
    "Invalid Request Protocal",                 /*EOFFSET(1)*/
    "Invalid Response Protocal",                /*EOFFSET(2)*/
    "Node Machine Or NodeManager Unreacheable", /*EOFFSET(3)*/
    // New item should add above
    "Undefined Error Number and Relating Readable Information" /*undefined*/};

#define ERROR_STR_DEFINED_NUM (sizeof(g_error_num_str) / sizeof(char *))
const char *GlobalErrorNum::get_err_num_str() {
  if ((err_num_ + 1 - EERROR_NUM_START) > ERROR_STR_DEFINED_NUM) {
    return g_error_num_str[ERROR_STR_DEFINED_NUM - 1];
  }
  return g_error_num_str[err_num_ - EERROR_NUM_START];
}

}; // namespace kunlun
