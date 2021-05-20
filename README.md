#Introduction
The cluster manager process maintains correct working status for storage shards and meta data shard of one or more Kunlun DDCs.
You can build this software from source or download it from www.zettadb.com.
#Building from source
Use cmake to build. Typically one should create a build directory in source root directory, call it 'bld', then in bld, do 
cmake .. -DCMAKE_BUILD_TYPE=Debug -DWITH_DEBUG=0 -DCMAKE_INSTALL_PREFIX=<install dir> && make -j8
then do below, possibly as root, depending on where one is installing this software:
make install 

#Usage
Give cluster_mgr a metadata cluster's connection parameters and a list of cluster IDs for it to work on, plus a config file with properly set parameters. Edit a copy of the 'cluster_mgr.cnf' file and set parameters properly according to comments in it. and then startup cluster_manager program with correct parameters.

Start only one instance/process for each metadata shard, otherwise errors could occur in extreme conditions.
For this software to work correctly you must keep its assumptions below true:
0. The meta-data shard node provided in config file (meta_svr_ip and meta_svr_port) is really a current effective node of the metadata shard and it contains latest meta data nodes.
1. A shard's mysqld processes are all up and running, it's the local Linux system's service manager or cron's responsibility to keep them up and running.
2. Metadata shard always has almost latest meta/storage shard topology info or will be updated very soon. Inconsistency is allowed but the time window should be small otherwise leftover prepared transactions may not be discovered and ended. So if you added/removed a node to/from a shard you must update the metadata in metadata shard ASAP.

