#
# Description:
# 1. check shards, shard_nodes
# input parameters
#       cluster_id, shard_id
#

--connection default

--let $sds_num=query_get_value(SELECT COUNT(*) FROM shards WHERE id=$shard_id AND db_cluster_id=$cluster_id, COUNT(*), 1)
if($sds_num != 0)
{
    --echo shards is not empty
    --exit
}

--let $sn_num=query_get_value(SELECT COUNT(*) FROM shard_nodes WHERE shard_id=$shard_id AND db_cluster_id=$cluster_id, COUNT(*), 1)
if($sn_num != 0)
{
    --echo shard_nodes is not empty
    --exit
}