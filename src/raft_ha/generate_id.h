/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

/*
* generate global id for consfailover
* id must increase
*/
#include <atomic>
#include <string>

class CGenerateId {
public:
    CGenerateId() {
      std::atomic_init<uint64_t>(&id_num_, 0);
    }
    virtual ~CGenerateId() {}

    void Init(uint64_t id);
    uint64_t GetInt64Id();
    std::string GetStringId();

    //slave cluster_mgr update id
    void SlaveClusterMgrUpdateId(uint64_t id);

private:
    std::atomic<uint64_t> id_num_; 
};