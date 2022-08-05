/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_MACHINE_ALIVE_H_
#define _CLUSTER_MGR_MACHINE_ALIVE_H_
#include <tuple>
#include <vector>
#include <map>
#include "zettalib/errorcup.h"
#include "zettalib/zthread.h"
#include "http_server/node_channel.h"
#include "util_func/object_ptr.h"
#include <string>

namespace kunlun
{

typedef std::tuple<std::string, std::string> Machine_Type;

class CMachineAlive : public ZThread, public ErrorCup {
public:
    CMachineAlive(int scan_interval) : scan_interval_(scan_interval) {}
    virtual ~CMachineAlive() {}

    int run();

    void GetCheckMachineIpLists(std::vector<Machine_Type>& hosts);
    bool SyncPingNodeMgrWithTimeout(const std::string& hostaddr);
    void ScanMachineJob(const Machine_Type& mach_host);

private:
    int scan_interval_;
    std::map<std::string, int> mach_states_;
    std::map<std::string, ObjectPtr<SyncNodeChannel> > host_channels_;
};

}

#endif