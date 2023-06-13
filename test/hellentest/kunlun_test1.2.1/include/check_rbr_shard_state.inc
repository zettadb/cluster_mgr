#
# Description:
# check rbr mode state
#   1. check metadb shard_nodes
#   2. check m/s 

--connect(shard_master, $shardid, clustmgr, clustmgr_pwd)
--let $slave_host = query_get_value(SHOW SLAVE HOSTS, Host, 1)

if($slave_host == "No such row")
{
--echo "connect master host and get slave hosts failed"
--exit
}

if($slave_host == "NULL")
{
--echo "connect master host and get slave hosts empty"
--exit
}

disconnect shard_master;

--connection default
