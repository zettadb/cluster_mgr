/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "raft_mission.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"
#include "kl_mentain/sys.h"
#include "raft_ha.h"

#include <braft/cli.h>

extern std::string raft_groupid;
extern std::string raft_group_member_init_config;
extern NodeHa* g_cluster_ha_handler;

namespace kunlun
{
    
bool RaftMission::SetUpMisson() {
    bool ret = true;
    job_id_ = get_request_unique_id();
    Json::Value paras;
    if(get_init_by_recover_flag() == true) {
         KLOG_INFO("Raft mission ignore recover state in setup phase");
        return ret;
    }
    
    KLOG_INFO("Raft mission setup phase");
    if (!super::get_body_json_document().isMember("paras")) {
        err_code_ = RAFT_MISSION_QUEST_LOST_PARAS_ERROR;
        KLOG_ERROR("missing `paras` key-value pair in the request body");
        goto end;
    }
    paras = super::get_body_json_document()["paras"];

    if(!paras.isMember("task_type")) {
        err_code_ = RAFT_MISSION_QUEST_LOST_TASK_TYPE_ERROR;
        KLOG_ERROR("missing `task_type` key-value pair in the request body");
        goto end;
    }
    task_type_ = paras["task_type"].asString();
    if(task_type_ == "transfer_leader") {
        if(!TransferLeaderParse(paras)) {
            goto end;
        }
    } else if(task_type_ == "add_peer") {
        if(!AddPeerParse(paras))
            goto end;
    } else if(task_type_ == "remove_peer") {
        if(!RemovePeerParse(paras))
            goto end;
    } else {
        err_code_ = RAFT_MISSION_UNKNOWN_TASK_TYPE_ERROR;
        KLOG_ERROR("missing `task_type` key-value pair in the request body");
        goto end;
    }

end:
    if(err_code_) {
        ret = false;
        job_status_ = "failed";
        UpdateOperationRecord();
    } else
        job_status_ = "ongoing";
    return ret;
}

void RaftMission::GetCurrentConfPeer() {
    std::vector<std::string> vec = StringTokenize(raft_group_member_init_config, ",");
    for(auto it : vec) {
        std::string host = it.substr(0, it.rfind(":"));
        if(!host.empty())
            cur_peers_.emplace_back(host);
    }
}

bool RaftMission::TransferLeaderParse(Json::Value& doc) {
    if(!doc.isMember("target_leader")) {
        err_code_ = RAFT_MISSION_QUEST_LOST_TARGET_LEADER_ERROR;
        KLOG_ERROR("raft mission transfer_leader no assign target_leader");
        return false;
    }

    target_leader_ = doc["target_leader"].asString();
    GetCurrentConfPeer();
    if(std::find(cur_peers_.begin(), cur_peers_.end(), target_leader_) == cur_peers_.end()) {
        err_code_ = RAFT_MISSION_ASSIGN_TARGET_LEADER_ERROR;
        KLOG_ERROR("assign target_leader {} is not in current configuations", target_leader_);
        return false;
    }
    return true;
}

bool RaftMission::AddPeerParse(Json::Value& doc) {
    if(!doc.isMember("peer")) {
        err_code_ = RAFT_MISSION_QUEST_LOST_PEER_ERROR;
        KLOG_ERROR("raft mission add_peer no assign peer");
        return false;
    }

    peer_ = doc["peer"].asString();
    GetCurrentConfPeer();
    if(std::find(cur_peers_.begin(), cur_peers_.end(), peer_) != cur_peers_.end()) {
        err_code_ = RAFT_MISSION_ASSIGN_PEER_ERROR;
        KLOG_ERROR("assign peer {} is in current configuations", peer_);
        return false;
    }
    return true;
}

bool RaftMission::RemovePeerParse(Json::Value& doc) {
    if(!doc.isMember("peer")) {
        err_code_ = RAFT_MISSION_QUEST_LOST_PEER_ERROR;
        KLOG_ERROR("raft mission remove_peer no assign peer");
        return false;
    }

    peer_ = doc["peer"].asString();
    GetCurrentConfPeer();
    if(std::find(cur_peers_.begin(), cur_peers_.end(), peer_) == cur_peers_.end()) {
        err_code_ = RAFT_MISSION_ASSIGN_PEER_ERROR;
        KLOG_ERROR("assign peer {} is not in current configuations", peer_);
        return false;
    }
    return true;
}

void RaftMission::DealLocal() {
    KLOG_INFO("raft mission deal local phase");
    if(job_status_ == "failed" || job_status_ == "done") {
        return;
    }

    if(task_type_ == "transfer_leader")
        TransferLeader();
    else if (task_type_ == "add_peer") 
        AddPeer();
    else if (task_type_ == "remove_peer")
        RemovePeer();
}

void RaftMission::TransferLeader() {
    braft::Configuration conf;
    if(conf.parse_from(raft_group_member_init_config) != 0) {
        err_code_ = RAFT_MISSION_PARSE_CONFIG_ERROR;
        job_status_ = "failed";
        KLOG_ERROR("parse current init_config {} failed", raft_group_member_init_config);
        UpdateOperationRecord();
        return;
    }

    braft::PeerId target_peer;
    if(target_peer.parse(target_leader_+":0") != 0) {
        err_code_ = RAFT_MISSION_CONV_TARGET_LEADER_ERROR;
        job_status_ = "failed";
        KLOG_ERROR("conv target_leader {} to peerid failed", target_leader_);
        UpdateOperationRecord();
        return;
    }

    braft::cli::CliOptions opt;
    opt.timeout_ms = -1;
    opt.max_retry = 3;
    butil::Status st = braft::cli::transfer_leader(raft_groupid, conf, target_peer, opt);
    if (!st.ok()) {
        err_code_ = RAFT_MISSION_TRANSFER_LEADER_ERROR;
        job_status_ = "failed";
        KLOG_ERROR("Fail to transfer_leader: {}", st.error_cstr());
        UpdateOperationRecord();
        return;
    }
    job_status_ = "done";
    UpdateOperationRecord();
    return;
}

void RaftMission::AddPeer() {
    braft::Configuration conf;
    if(conf.parse_from(raft_group_member_init_config) != 0) {
        err_code_ = RAFT_MISSION_PARSE_CONFIG_ERROR;
        job_status_ = "failed";
        KLOG_ERROR("parse current init_config {} failed", raft_group_member_init_config);
        UpdateOperationRecord();
        return;
    }

    braft::PeerId new_peer;
    if(new_peer.parse(peer_+":0") != 0) {
        err_code_ = RAFT_MISSION_CONV_PEER_ERROR;
        job_status_ = "failed";
        KLOG_ERROR("conv input peer {} to peerid failed", peer_);
        UpdateOperationRecord();
        return;
    }

    braft::cli::CliOptions opt;
    opt.timeout_ms = -1;
    opt.max_retry = 3;
    butil::Status st = braft::cli::add_peer(raft_groupid, conf, new_peer, opt);
    if (!st.ok()) {
        err_code_ = RAFT_MISSION_ADD_PEER_ERROR;
        job_status_ = "failed";
        KLOG_ERROR("Fail to add_peer: {}", st.error_cstr());
        UpdateOperationRecord();
        return;
    }

    cur_peers_.push_back(peer_);
    if(!SyncNewConfToDisk())
        return;

    job_status_ = "done";
    UpdateOperationRecord();
    return;
}

void RaftMission::RemovePeer() {
    braft::Configuration conf;
    if(conf.parse_from(raft_group_member_init_config) != 0) {
        err_code_ = RAFT_MISSION_PARSE_CONFIG_ERROR;
        job_status_ = "failed";
        KLOG_ERROR("parse current init_config {} failed", raft_group_member_init_config);
        UpdateOperationRecord();
        return;
    }

    braft::PeerId remove_peer;
    if(remove_peer.parse(peer_+":0") != 0) {
        err_code_ = RAFT_MISSION_CONV_PEER_ERROR;
        job_status_ = "failed";
        KLOG_ERROR("conv input peer {} to peerid failed", peer_);
        UpdateOperationRecord();
        return;
    }

    braft::cli::CliOptions opt;
    opt.timeout_ms = -1;
    opt.max_retry = 3;
    butil::Status st = braft::cli::remove_peer(raft_groupid, conf, remove_peer, opt);
    if (!st.ok()) {
        err_code_ = RAFT_MISSION_REMOVE_PEER_ERROR;
        job_status_ = "failed";
        KLOG_ERROR("Fail to remove_peer: {}", st.error_cstr());
        UpdateOperationRecord();
        return;
    }

    std::vector<std::string>::iterator it = cur_peers_.begin();
    for(; it != cur_peers_.end();) {
        if(*it == peer_) {
            cur_peers_.erase(it);
            break;
        }
    }
    if(!SyncNewConfToDisk())
        return;

    job_status_ = "done";
    UpdateOperationRecord();
    return;
}

bool RaftMission::SyncNewConfToDisk() {
    std::string confs;
    for(auto it : cur_peers_) 
        confs = confs + it+":0,";

    //g_cluster_ha_handler->
    return true;
}

bool RaftMission::UpdateOperationRecord() {
    std::string str_sql,memo;
    Json::Value memo_json;
    Json::FastWriter writer;

    memo_json["error_code"] = err_code_;
    memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
    writer.omitEndingLineFeed();
    memo = writer.write(memo_json);

    str_sql = "UPDATE cluster_general_job_log set status='" + job_status_ + "',memo='" + memo;
    str_sql += "',when_ended=current_timestamp(6) where id=" + job_id_;

    if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) {
        KLOG_ERROR( "execute_metadate_opertation error");
        return false;
    }

    KLOG_INFO("update cluster_general_job_log success sql: {}", str_sql);
    return true;
}
    
} // namespace kunlun


