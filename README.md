# Introduction

Clust_mgr is an important compnent of KunlunBase. It provides an API via HTTP connections for KunlunBase users to do cluster management, provisioning and monitor work, so that uses can install/uninstall a KunlunBase cluster, a kunlun-server node, a kunlun-storage shard or a kunlun-storage node, and do scale-out or cluster backup&restore by calling such APIs. Such capability enables users to integrate KunlunBase cluster management and provisioning as part of their existing application or GUIs.

cluster_mgr also maintains correct working status for kunlun-storage shards and meta data shard of one or more KunlunBase clusters.
You can build this software from source or download it from downloads.kunlunbase.com.

# cluster_mgr instances form a raft group (using braft library) and the primary node takes the responsibility, and whenever the primary node is down, a new primary node is automatically elected via the raft consistency algorithm and there is no single point of failure in any part of a KunlunBase cluster.

# Building from source

Use cmake to build. Typically one should create a build directory in source root directory, call it 'bld', then in bld, do 
cmake .. -DCMAKE_BUILD_TYPE=Debug -DWITH_DEBUG=0 -DCMAKE_INSTALL_PREFIX=<install dir> && make -j8
then do below, possibly as root, depending on where one is installing this software:
make install 

# Usage

Give cluster_mgr a metadata cluster's connection parameters and a list of cluster IDs for it to work on, plus a config file with properly set parameters. Edit a copy of the 'cluster_mgr.cnf' file and set parameters properly according to comments in it. and then startup cluster_manager program with correct parameters.

Start only one instance/process for each metadata shard, otherwise errors could occur in extreme conditions.
For this software to work correctly you must keep its assumptions below true:
0. The meta-data shard node provided in config file (meta_svr_ip and meta_svr_port) is really a current effective node of the metadata shard and it contains latest meta data nodes.
1. Metadata shard always has almost latest meta/storage shard topology info or will be updated very soon. Inconsistency is allowed but the time window should be small otherwise leftover prepared transactions (caused by crash or abnormal exit of kunlun-server nodes and/or Kunlun-storage nodes) may not be discovered and ended in time. So if you added/removed a node to/from a shard you must update the metadata in metadata shard ASAP.

