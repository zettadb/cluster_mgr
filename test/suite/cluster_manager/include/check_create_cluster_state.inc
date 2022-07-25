#
# Description:
# 1. check shard status
# 2. check pg status
# 3. check metadb shards, shard_nodes, comp_nodes
#
# input parameters:
#       cluster_id

--connection default

--let $shard_nums = `SELECT COUNT(*) FROM shards WHERE db_cluster_id=$cluster_id`
--let $comp_nums = `SELECT COUNT(*) FROM comp_nodes WHERE db_cluster_id=$cluster_id`

while($shard_nums > 0) 
{
    --let $shard_name=shardname.$shard_nums
    --let $shardid=shard.$shard_nums
    --let $shard_id=query_get_value(SELECT id FROM shards WHERE db_cluster_id=$cluster_id, id, $shard_nums)

    --source kunlun-test/suite/cluster_manager/include/check_rbr_shard_state.inc

    --let $shard_node_num=query_get_value(SELECT COUNT(*) FROM shard_nodes WHERE shard_id=$shard_id AND db_cluster_id=$cluster_id, COUNT(*), 1)
    if($shard_node_num != $nodes)
    {
        --echo install shard nodes number is unmatch nodes
        --exit
    }

    dec $shard_nums;
}


if($comp_nums != $comps)
{
    --echo install computer nodes number is unmatch comps
    --exit
}

--let c_inc=1
while($comp_nums > 0) 
{
    let $cconn_name=cc_pg_conn.$c_inc;
    let $comp = computer.$comp_nums;   
    --pg_connect($cconn_name, $comp, $comp_user, $comp_pwd)
    --source kunlun-test/suite/cluster_manager/include/check_computer_state.inc
    dec $comp_nums;
    inc $c_inc;
    --disconnect $cconn_name
}

--connection default

--let $commit_name='commit_log_$cluster_name'
--let $ddlops_name='ddl_ops_log_$cluster_name'
--let $commit_exist=query_get_value(SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=$commit_name, COUNT(*), 1)
if($commit_exist != 1) 
{
    --echo commit_log is not created, please check
    --exit
}

--let $ddlops_exist=query_get_value(SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=$ddlops_name, COUNT(*), 1)
if($ddlops_exist != 1) 
{
    --echo ddl_ops_log is not created, please check
    --exit
}