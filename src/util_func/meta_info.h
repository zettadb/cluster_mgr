/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _KUNLUN_META_INFO_UTIL_FUNC_H_
#define _KUNLUN_META_INFO_UTIL_FUNC_H_
#include "http_server/node_channel.h"
#include "request_framework/requestValueDefine.h"
#include "zettalib/errorcup.h"
#include "zettalib/op_mysql.h"
#include <ctime>
#include "kl_mutex.h"
//#include <mutex>

namespace kunlun {
extern std::string GenerateNewClusterIdStr(MysqlConnection *);
ClusterRequestTypes GetReqTypeEnumByStr(const char *);
bool RecognizedRequestType(ClusterRequestTypes);
bool RecognizedJobTypeStr(std::string &);
bool ValidNetWorkAddr(const char *);
uint64_t GetNowTimestamp();
std::string FetchNodemgrTmpDataPath(GlobalNodeChannelManager *g_channel,
                                    const char *ip);
int64_t FetchNodeMgrListenPort(GlobalNodeChannelManager *g_channel,
                               const char *ip);

class TimePeriod : public kunlun::ErrorCup {
public:
  // '01:00:00-02:00:00'
  explicit TimePeriod(const char *time_str)
      : time_str_(time_str), parsed_(false){};
  ~TimePeriod() = default;
  bool init(std::string time_str);

  bool TimesUp();
  std::string get_time_str() { return time_str_; }

private:
  bool parse();

private:
  struct tm start_;
  struct tm stop_;
  std::string time_str_;
  bool parsed_;
};

/*
 * Successfull trigger until reach the counter limit during the time period
 * */

class CounterTriggerTimePeriod : public kunlun::ErrorCup {
public:
  CounterTriggerTimePeriod():counter_limits_(1), timer_(""), counted_(0),force_(false){};
  CounterTriggerTimePeriod(int times, const char *time_str)
      : counter_limits_(times), timer_(time_str), counted_(0),force_(false){};
  ~CounterTriggerTimePeriod() = default;

  bool InitOrRefresh(std::string time_str,int times = 1) {

    KlWrapGuard<KlWrapMutex> scope_lock(mutex_);
    counter_limits_ = times;
    bool ret = timer_.init(time_str);
    if (!ret) {
      setErr("%s", timer_.getErr());
      return false;
    }
    return true;
  };

  bool trigger_on() {
    KlWrapGuard<KlWrapMutex> scope_lock(mutex_);

    if(force_){
      force_ = false;
      return true;
    }

    if (!timer_.TimesUp()) {
      counted_ = 0;
    }

    if (timer_.TimesUp() && counted_ < counter_limits_) {
      counted_++;
      return true;
    }
    return false;
  };

  void set_force(bool val){
    KlWrapGuard<KlWrapMutex> scope_lock(mutex_);
    force_ = val;
  }

  KlWrapMutex mutex_;
  int counter_limits_;
  TimePeriod timer_;
  int counted_;
  bool force_;
};
} // namespace kunlun

#endif /*_KUNLUN_META_INFO_UTIL_FUNC_H_*/
