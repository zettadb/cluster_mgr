/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

//#include "generate_id.h"
#include "raft_ha.h"

extern NodeHa* g_cluster_ha_handler;

void CGenerateId::Init(uint64_t id) {
    id_num_.store(id + 1000);
}

uint64_t CGenerateId::GetInt64Id() {
    uint64_t id = id_num_.fetch_add(1, std::memory_order_relaxed);
    g_cluster_ha_handler->InsertMsg("KUNLUN_GLOBAL_ID", std::to_string(id_num_.load(std::memory_order_acquire)));
    return id;
}

std::string CGenerateId::GetStringId() {
    uint64_t id = id_num_.fetch_add(1, std::memory_order_relaxed);
    g_cluster_ha_handler->InsertMsg("KUNLUN_GLOBAL_ID", std::to_string(id_num_.load(std::memory_order_acquire)));
    return std::to_string(id);
}

void CGenerateId::SlaveClusterMgrUpdateId(uint64_t id) {
    id_num_.store(id);
}