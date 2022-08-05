/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_CLUSTER_COMM_H_
#define _CLUSTER_MGR_CLUSTER_COMM_H_
#include <string>
#include <map>
#include <unordered_map>
#include <tuple>
#include <vector>
#include "util_func/kl_mutex.h"

namespace kunlun
{

//hostaddr
//current maximum port
//begin port
//end port
//number of used_port + installing_port
//0 -- storage, 1 -- computer
typedef std::tuple<std::string, int, int, int, int, int> Host_Param;

typedef std::tuple<std::string, int> IComm_Param;

typedef enum {
    M_STORAGE,
    M_COMPUTER
} MachType;


typedef enum {
    S_PREPARE,
    C_PREPARE,
    S_DISPATCH,
    C_DISPATCH,
    S_DONE,
    C_DONE,
    S_FAILED,
    C_FAILED
}JobType;

typedef enum {
  K_APPEND,
  K_OVERLAP,
  K_CLEAR
} ColOp_Type;

struct HostParamComp {
    bool operator() (const Host_Param& l, const Host_Param& r) {
        return (std::get<4>(l) < std::get<4>(r));
    }
};

bool UpdateInstallingPort(ColOp_Type type, MachType itype, 
                          const std::map<std::string, std::vector<int> >& host_ports);
bool UpdateUsedPort(ColOp_Type type, MachType itype, 
                          const std::map<std::string, std::vector<int> >& host_ports);

bool GetStorageInstallHost(const std::string& hostaddr, std::vector<Host_Param>& avail_hosts,
                        std::unordered_map<std::string, std::vector<int> >& host_ports, int err_code);

bool GetComputerInstallHost(const std::string& hostaddr, std::vector<Host_Param>& avail_hosts,
                        std::unordered_map<std::string, std::vector<int> >& host_ports, int err_code);

bool GetBestInstallHosts(std::vector<Host_Param>& avail_hosts, int node_num, std::vector<IComm_Param>& iparams, 
                std::unordered_map<std::string, std::vector<int> >& host_ports);

std::string GetMysqlUuidByHost(const std::string& shard_name, const std::string& cluster_name, 
                const std::string& host);

bool PersistFullsyncLevel(const std::vector<std::string>& hosts, int fullsync_level);

bool UpdatePgTables(const std::string& ip, const std::string& port, 
                    const std::string& comp_user, const std::string& comp_pwd,
                    int cluster_id, int type, 
                    const std::unordered_map<std::string, std::vector<IComm_Param> >& storage_iparams,
                    const std::unordered_map<std::string, std::vector<std::string> >& shard_map_nids,
                    const std::unordered_map<std::string, std::string>& shard_map_sids,
                    std::vector<std::string>& sql_stmts);


bool AddShardToComputerNode(int cluster_id, const std::string& comp_user, const std::string& comp_pwd,
                    const std::unordered_map<std::string, std::vector<IComm_Param> >& storage_iparams,
                    const std::unordered_map<std::string, std::vector<std::string> >& shard_map_nids,
                    const std::unordered_map<std::string, std::string>& shard_map_sids, 
                    std::vector<std::string>& sql_stmts, int type=0);

bool WriteSqlToDdlLog(const std::string& cluster_name, const std::vector<std::string>& sql_stmt);

bool CheckMachineIplists(MachType mtype, const std::vector<std::string>& iplists);

bool CheckStringIsDigit(const std::string& str);

typedef std::tuple<bool, int, std::map<std::string, std::string> > CMemo_Params;
CMemo_Params GetClusterMemoParams(int cluster_id, const std::vector<std::string>& key_names);

bool CheckTableExists(const std::string& table_name);

bool UpdateClusterShardTopology(int cluster_id, const std::vector<int>& shard_ids, int max_commit_log_id,
              int max_ddl_log_id);

}
#endif