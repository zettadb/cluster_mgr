/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_REFRESH_SHARD_
#define _CLUSTER_MGR_REFRESH_SHARD_

#include <unordered_map>
#include <vector>
#include "kl_mentain/async_mysql.h"
#include "zettalib/errorcup.h"
#include "zettalib/zthread.h"
#include "kl_mentain/shard.h"
#include "kl_mentain/thread_pool.h"
#include "util_func/object_ptr.h"

using namespace kunlun;

namespace kunlun
{

class CRefreshShard : public ZThread, public ErrorCup {
public:
    CRefreshShard()  {}
    virtual ~CRefreshShard() {
        if(tpool_)
            delete tpool_;
    }

    int run();

    void CheckAndDispatchAutoBackupMission(int state);
    void CheckAndDispatchAutoBackupComputeMission(int state); 
 
    void CheckShardBackupState(ObjectPtr<Shard> shard);
    bool BackupByRpc(const std::string& host);
    
    void CheckMetaShardBackupState(ObjectPtr<Shard> shard);
    
    void LoopShardBackupState(int state);

    CThreadPool* GetThreadPool() {
        return tpool_;
    }

public:
    static void BackupCallBack(Json::Value& root, void *arg);

private:
    CThreadPool* tpool_;
};

}
#endif