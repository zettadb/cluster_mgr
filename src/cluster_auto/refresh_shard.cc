/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "refresh_shard.h"
#include "kl_mentain/sys.h"
#include "kl_mentain/global.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"
#include "raft_ha/raft_ha.h"
#include "http_server/node_channel.h"
//#include <boost/format.hpp>
//#include <boost/algorithm/string.hpp>
#include "coldbackup/coldbackup.h"
#include "request_framework/handleRequestThread.h"
#include "cluster_debug/cluster_debug.h"

extern kunlun::CRefreshShard* g_rfshard;
extern HandleRequestThread * g_request_handle_thread;
int64_t backup_allow_replica_delay = 7200;

namespace kunlun
{

/*
 * auto backup computer node for every kl_clusterts
 * */

void CRefreshShard::CheckAndDispatchAutoBackupComputeMission(int state){
  while(state){
    //System::get_instance()->mtx.lock();
    const std::vector<ObjectPtr<KunlunCluster> >& kl_clusters = System::get_instance()->get_kl_clusters();
    for(auto iter: kl_clusters){
        if(!iter.Invalid()){
            continue;
        }
        auto ptr = iter->get_coldbackup_comp_node();
        if(!ptr.Invalid())
            continue;

        if(!(ptr->coldbackup_timer.trigger_on())){
            KLOG_DEBUG("compute ({},{}) of cluster ({},{}) no need backup right now",ptr->get_id(),ptr->get_name(),iter->get_id(),iter->get_name());
            continue;
        }

        KLOG_INFO("compute ({},{}) of cluster ({},{}) need backup right now",ptr->get_id(),ptr->get_name(),iter->get_id(),iter->get_name());

        std::string ip = "";
        int port = 0;
        ptr->get_ip_port(ip, port);
        std::string ip_port = ip+"_"+std::to_string(port);
        KColdBackUpMission * mission = new KColdBackUpMission(ip_port.c_str(),1);

        bool ret = mission->InitFromInternal();
        if (!ret){
            KLOG_ERROR("init compute coldbackupMisson faild: {}",mission->getErr());
            delete mission;
            continue;
        }
        KLOG_INFO("init compute coldbackupMisson successfully for compute ({},{}) of cluster ({},{})",ptr->get_id(),ptr->get_name(),iter->get_id(),iter->get_name());
        //dispatch current mission
        g_request_handle_thread->DispatchRequest(mission);
    }
    
    //System::get_instance()->mtx.unlock();
    KLOG_DEBUG("CheckAndDispatchAutoBackupComputeMission running");
    sleep(10);
  }
}


/*
 * autobackup shard
 * */

void CRefreshShard::CheckAndDispatchAutoBackupMission(int state){
  while(state){
    //System::get_instance()->mtx.lock();
    std::vector<ObjectPtr<Shard> > storage_shard = System::get_instance()->get_storage_shards();
    for(auto iter: storage_shard){
      if(!iter.Invalid()){
        continue;
      }
      if(iter->get_id() ==  -1){
        // TODO: BACKUP MEATDATA
        continue;
      }
      if(iter->get_bacupnode_ip_port().empty() ){
        KLOG_ERROR("shard ({},{}) backup_node info ivalid",iter->get_cluster_id(),iter->get_id());
        continue;
      }
      if(!(iter->coldback_time_trigger_.trigger_on())){
        KLOG_DEBUG("shard ({},{}) no need backup right now",iter->get_cluster_id(),iter->get_id());
        continue;
      }
      
      KLOG_INFO("shard ({},{}) need backup right now",iter->get_cluster_id(),iter->get_id());
      KColdBackUpMission * mission = new KColdBackUpMission(iter->get_bacupnode_ip_port().c_str());
      bool ret = mission->InitFromInternal();
      if (!ret){
        KLOG_ERROR("init coldbackupMisson faild: {}",mission->getErr());
        delete mission;
        continue;
      }
      KLOG_INFO("init coldbackupMisson successfully for shard ({},{})",iter->get_cluster_id(),iter->get_id());
      //dispatch current mission
      g_request_handle_thread->DispatchRequest(mission);
    }
    
    //System::get_instance()->mtx.unlock();
    KLOG_DEBUG("CheckAndDispatchAutoBackupMission running");
    sleep(10);
  }
}

void CRefreshShard::LoopShardBackupState(int state) {
    while(state) {
        std::vector<ObjectPtr<Shard> > storage_shards = System::get_instance()->get_storage_shards();
        for(auto it : storage_shards) {
            if(!it.Invalid())
                continue;
            if(it->get_is_delete())
                continue;

            CheckShardBackupState(it);
        }
        sleep(30);
    }
    return;
}

void CRefreshShard::CheckMetaShardBackupState(ObjectPtr<Shard> shard) {
    ObjectPtr<Shard_node> cur_master = shard->get_master();
    if(!cur_master.Invalid())
        return;

    MysqlResult res;
    std::string sql = string_sprintf("show global variables like 'binlog_backup_open_mode'");
    bool ret = cur_master->send_stmt(sql.c_str(), &res, stmt_retries);
    if(ret) {
        KLOG_ERROR("host {} get global binlog_backup_open_mode failed", cur_master->get_host());
        return;
    }

    if(res.GetResultLinesNum() != 1) {
        KLOG_ERROR("host {} get result num failed", cur_master->get_host());
        return;
    }
    std::string value = res[0]["Value"];
    if(value == "ON")
        return;
    
    std::vector<ObjectPtr<Shard_node> > sd_nodes = shard->get_nodes();
    for(auto node : sd_nodes) {
        if(!node.Invalid())
            continue;
        if(node->is_master())
            continue;

        ret = node->send_stmt(sql.c_str(), &res, stmt_retries);
        if(ret) {
            KLOG_ERROR("host {} get global binlog_backup_open_mode failed", node->get_host());
            continue;
        }
        if(res.GetResultLinesNum() != 1) {
            KLOG_ERROR("host {} get result num failed", node->get_host());
            return;
        }

        value = res[0]["Value"];
        if(value == "ON") {
            sql = string_sprintf("set global binlog_backup_open_mode='OFF'");
            ret = node->send_stmt(sql.c_str(), &res, stmt_retries);
            if(ret) {
                KLOG_ERROR("host {} set global binlog_backup_open_mode OFF failed", node->get_host());
                continue;
            }
        }
    }

    sql = string_sprintf("set global binlog_backup_open_mode='ON'");
    ret = cur_master->send_stmt(sql.c_str(), &res, stmt_retries);
    if(ret) {
        KLOG_ERROR("host {} set global binlog_backup_open_mode ON failed", cur_master->get_host());
    }

    BackupByRpc(cur_master->get_host());
}

void CRefreshShard::CheckShardBackupState(ObjectPtr<Shard> shard) {
    ObjectPtr<MetadataShard> metashard = System::get_instance()->get_MetadataShard();
    if(!metashard.Invalid())
        return;

    ObjectPtr<Shard_node> cur_master =  metashard->get_master();
    if(!cur_master.Invalid())
        return;

    int cluster_id = shard->get_cluster_id();
    int shard_id = shard->get_id();
    if(shard_id == -1) {
        CheckMetaShardBackupState(shard);
        return;
    }

    MysqlResult result;
    std::string sql = string_sprintf("select id, hostaddr, port, sync_state, member_state, replica_delay, backup_node from %s.shard_nodes where shard_id=%d and db_cluster_id=%d",
                    KUNLUN_METADATA_DBNAME, shard_id, cluster_id);
#ifndef NDEBUG
    KLOG_INFO("sql: {}", sql);
#endif
    if(cur_master->send_stmt(sql.c_str(), &result, stmt_retries)) {
        KLOG_ERROR("get shard_nodes backup_node state failed");
        return;
    }

    std::string id, host, close_id, master_host, master_id;
    uint32_t nrows = result.GetResultLinesNum();
    for(uint32_t i=0; i<nrows; i++) {
        std::string backup_node = result[i]["backup_node"];
            
        int replica_delay = atoi(result[i]["replica_delay"]);
        if(backup_node == "ON") {
            if(replica_delay <= backup_allow_replica_delay) 
                return;
            else
                close_id = result[i]["id"];
        }
            
        std::string member_state = result[i]["member_state"];
        std::string sync_state = result[i]["sync_state"];
        std::string hostaddr = result[i]["hostaddr"];
        std::string port = result[i]["port"];
        if(member_state == "source") {
            master_host = hostaddr+"_"+port;
            master_id = result[i]["id"];
        }

        if(member_state == "replica" && sync_state == "fsync" &&
                            replica_delay <= backup_allow_replica_delay ) {
            id = result[i]["id"];
            host = hostaddr+"_"+port;
        }
    }
    if(host.empty()) {
        host = master_host;
        id = master_id;
        //return;
    }

    g_rfshard->GetThreadPool()->commit([shard, id, host, close_id]{
        if(!shard.Invalid())
            return;
            
        ObjectPtr<MetadataShard> metashard = System::get_instance()->get_MetadataShard();
        if(!metashard.Invalid())
            return;
        ObjectPtr<Shard_node> cur_master = metashard->get_master();
        if(!cur_master.Invalid())
            return;

        std::string ip = host.substr(0, host.rfind("_"));
        int port = atoi(host.substr(host.rfind("_")+1).c_str());
        ObjectPtr<Shard_node> s_node = shard->get_node_by_ip_port(ip, port);
        if(!s_node.Invalid()) {
            KLOG_ERROR("get host {} shard node failed", host);
            return;
        }

        MysqlResult result;
        std::string sql = string_sprintf("set global binlog_backup_open_mode=on");
        if(s_node->send_stmt(sql.c_str(), &result, stmt_retries)) {
            KLOG_ERROR("set host {} binlog backup open mode failed", host);
            return;
        }

        sql = string_sprintf("update shard_nodes set backup_node='ON' where id=%s",
                                id.c_str());
        cur_master->send_stmt(sql.c_str(), &result, stmt_retries);
        if(!close_id.empty()) {
            sql = string_sprintf("update shard_nodes set backup_node='OFF' where id=%s",
                                close_id.c_str());
            cur_master->send_stmt(sql.c_str(), &result, stmt_retries);
        }
        shard->set_backupnode_ip_port(ip.c_str(),std::to_string(port).c_str());

        KLOG_INFO("host: {} start to back rpc", host);
        g_rfshard->BackupByRpc(host);
    });
    return;
}

void CRefreshShard::BackupCallBack(Json::Value& root, void *arg) {
    //KLOG_INFO("coldbackup callback result {}", result);
}

bool CRefreshShard::BackupByRpc(const std::string& host) {
    KColdBackUpMission* kbackup = new KColdBackUpMission(host.c_str());
    kbackup->set_cb(CRefreshShard::BackupCallBack);
    bool ret = kbackup->InitFromInternal();
    if(!ret) {
        delete kbackup;
        return false;
    }

    g_request_handle_thread->DispatchRequest(kbackup);
    return true;
}


int CRefreshShard::run() {
    int state = m_state;

    tpool_ = new CThreadPool(5);
    
    //deal auto backup
    tpool_->commit([this, state]{
        this->CheckAndDispatchAutoBackupMission(state);
    });
    //deal auto backup compute
    tpool_->commit([this, state]{
        this->CheckAndDispatchAutoBackupComputeMission(state);
    });

    tpool_->commit([this, state] {
        this->LoopShardBackupState(state);
    });
    
    while(m_state) {
        sleep(300);
    }

    return 0;
}

}