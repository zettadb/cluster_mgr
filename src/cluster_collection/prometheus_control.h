/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_PROMETHEUS_CONTROL_H_
#define _CLUSTER_MGR_PROMETHEUS_CONTROL_H_
#include "zettalib/errorcup.h"
#include "zettalib/zthread.h"
#include <string>
#include <vector>
#include "util_func/kl_mutex.h"

namespace kunlun
{
class KProcStat : public ErrorCup {
public:
   KProcStat() : pid_(0) {}
   virtual ~KProcStat() {}
   bool Parse(const char * buf);
   
   pid_t GetPid() const {
      return pid_;
   }
   const std::string& GetBinName() const {
      return binName_;
   }
   const std::string& GetBinArgs() const {
        return binArgs_;
   }

private:
   pid_t pid_;
   std::string binName_;
   std::string binArgs_;
};

class KPrometheusControl :  public ZThread, public ErrorCup {
public:
    KPrometheusControl(int loop_interval = 10);
    virtual ~KPrometheusControl() {}

    int run();

    int Start();
    int Stop();
    int Restart();
    bool UpdatePrometheusConf(int need_reload=1);
    int ReloadConf();

    bool PrometheusPathIsExist();
    bool PrometheusPidAlive();
    int GetPrometheusPid(pid_t& pid);

   bool ReportAddMachine(const std::string& ip, const std::string& machine_type);
   bool ReportDelMachine(const std::string& ip, const std::string& machine_type);
   int StartNodeExporter(const std::string &ip, std::string& exporter_port);
    int StopNodeExporter(const std::string &ip, const std::string& exporter_port);
    bool AddComputerConf(const std::vector<std::string>& hosts);
    bool DelComputerConf(const std::vector<std::string>& hosts);
    bool AddStorageConf(const std::vector<std::string>& hosts);
    bool DelStorageConf(const std::vector<std::string>& hosts);
   void GetRunStatByMeta();
private:
    int SavePrometheusFile(const std::string &path, char* buf);
    
private:
    std::vector<std::string> storage_iplists_;
    std::vector<std::string> computer_iplists_;
    
    //ip:port
    std::vector<std::string> node_exporters_;
    std::vector<std::string> storage_exporters_;
    std::vector<std::string> computer_exporters_;
    int loop_interval_;
    std::string binName_;
    std::string binArgs_;
    KlWrapMutex mux_;
};

} // namespace kunlun

#endif