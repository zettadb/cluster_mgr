/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "machine_alive.h"
#include "kl_mentain/shard.h"
#include "zettalib/tool_func.h"

namespace kunlun 
{

int mach_alive_watchshed = 10;

int CMachineAlive::run() {
    while(m_state) {
        sleep(scan_interval_);

        std::vector<Machine_Type> hosts;
        GetCheckMachineIpLists(hosts);

        for(auto it : hosts) {
            ScanMachineJob(it);
        }
    }
    return 0;
}

void CMachineAlive::ScanMachineJob(const Machine_Type& mach_host) {
    std::string hostaddr = std::get<0>(mach_host);
    std::string node_stats = std::get<1>(mach_host);
    bool ret = SyncPingNodeMgrWithTimeout(hostaddr);
    std::string stat;
    if(ret) {
        if(node_stats == "dead")
            stat = "running";

        if(mach_states_.find(hostaddr) != mach_states_.end())    
            mach_states_.erase(hostaddr);
    } else {
        int num = 0;
        if(mach_states_.find(hostaddr) != mach_states_.end())
            num = mach_states_[hostaddr];

        if(node_stats == "running") {
            if(num > mach_alive_watchshed) {
                stat = "dead";
            }
        }
        num++;
        mach_states_[hostaddr] = num;
    }

    if(!stat.empty()) {
        std::string sql = string_sprintf("update %s.server_nodes set node_stats='%s' where hostaddr='%s'",
                    KUNLUN_METADATA_DB_NAME, stat.c_str(), hostaddr.c_str());
        MysqlResult result;
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("update hostaddr {} stat {} to server_nodes failed {}",
                            hostaddr, stat, g_node_channel_manager->getErr());
        }
    }
}

void CMachineAlive::GetCheckMachineIpLists(std::vector<Machine_Type>& mach_hosts) {
    std::string sql = string_sprintf("select distinct hostaddr, machine_type, node_stats from %s.server_nodes",
                            KUNLUN_METADATA_DB_NAME);

    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get hostaddr from server_nodes failed: {}", g_node_channel_manager->getErr());
        return;
    }
    uint32_t num_rows = result.GetResultLinesNum();
    for(uint32_t i=0; i<num_rows; i++) {
        std::string hostaddr = result[i]["hostaddr"];
        if(hostaddr == "pseudo_server_useless")
            continue;
        std::string machine_type = result[i]["machine_type"];
        if(machine_type == "NULL")
            continue;

        std::string node_stats = result[i]["node_stats"];
        Machine_Type tmp{hostaddr, node_stats};
        mach_hosts.emplace_back(tmp);
    }
}

bool CMachineAlive::SyncPingNodeMgrWithTimeout(const std::string& hostaddr) {
    std::string port = g_node_channel_manager->GetNodeMgrPort(hostaddr);

    std::string host = hostaddr+"_"+port;
    Json::Value root;
    Json::Value paras;
    root["cluster_mgr_request_id"] = "ping_pong";
    root["task_spec_info"] = "ping_pong";
    root["job_type"] = "ping_pong";
    root["paras"] = paras;

    if(host_channels_.find(host) == host_channels_.end()) {
        ObjectPtr<SyncNodeChannel> nodechannel(new SyncNodeChannel(30, hostaddr, port));
    
        if(!nodechannel->Init()) {
            KLOG_ERROR("node channel init failed: {}", nodechannel->getErr());
            return false;
        }

        if(nodechannel->ExecuteCmd(root)) {
            setErr("%s", nodechannel->getErr());
            KLOG_ERROR("execute cmd failed: {}", nodechannel->getErr());
            return false;
        }
        host_channels_[host] = nodechannel;
    } else {
        ObjectPtr<SyncNodeChannel> nodechannel = host_channels_[host];
        if(nodechannel->ExecuteCmd(root)) {
            setErr("%s", nodechannel->getErr());
            KLOG_ERROR("execute cmd failed: {}", nodechannel->getErr());
            host_channels_.erase(host);
            return false;
        }
    }

    return true;
}

}