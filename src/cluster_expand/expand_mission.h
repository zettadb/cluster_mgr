/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _EXPAND_MISSION_H_
#define _EXPAND_MISSION_H_
#include "request_framework/missionRequest.h"
#include "http_server/node_channel.h"

namespace kunlun
{

  class ExpandClusterMission : public MissionRequest
  {
    typedef MissionRequest super;

  public:
    explicit ExpandClusterMission(Json::Value *doc) : super(doc){};
    ~ExpandClusterMission(){};

    virtual bool ArrangeRemoteTask() override final;
    virtual bool SetUpMisson() override final;
    virtual void TearDownImpl() override final;
    virtual bool FillRequestBodyStImpl() override final;

  private:
    bool MakeDir();
    bool DumpTable();
    bool LoadTable();
    bool TableCatchUp();
    bool TransferFile();

  private:
    //Will be initialized in setup phase
    std::string mydumper_tmp_data_dir_;
    std::string src_shard_node_address_;
    int64_t src_shard_node_port_;
    std::string dst_shard_node_address_;
    int64_t dst_shard_node_port_;
  };
};     // namespace kunlun
#endif /*_EXPAND_MISSION_H_*/
