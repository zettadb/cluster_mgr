/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _CLUSTER_MANAGER_ERROR_CODE_H_
#define _CLUSTER_MANAGER_ERROR_CODE_H_

#include <string>
namespace kunlun
{

#define EERROR_NUM_START (0)

#define EOFFSET(offset) (EERROR_NUM_START) + (offset)

#define EOK EOFFSET(0)
#define EFAIL EOFFSET(1)
#define EIVALID_REQUEST_PROTOCAL EOFFSET(1)
#define EIVALID_RESPONSE_PROTOCAL EOFFSET(2)
#define ENODE_UNREACHEABLE EOFFSET(3)



// New item should add above
#define EUNDEFINED_ERROR EOFFSET(1000)

  // Defined in error_code.cc
  extern const char *g_error_num_str[];

  class GlobalErrorNum
  {
  public:
    GlobalErrorNum() { err_num_ = EOK; }
    int get_err_num() { return err_num_; }
    void set_err_num(int errnum) { err_num_ = errnum; }
    const char *get_err_num_str();
    bool Ok() { return err_num_ == EOK; }
    std::string EintToStr(int errnum)
    {
      return std::to_string(errnum);
    }

  private:
    int err_num_;
  };

}; // namespace kunlun

#endif /*_CLUSTER_MANAGER_ERROR_CODE_H_*/
