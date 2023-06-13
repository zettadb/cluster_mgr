#
# Description:
# 1. check shards, shard_nodes
# 2. check comp_nodes
# 3. check metadb commit_log_xxx and ddl_ops_log_xxxx
#

--connection default

--let $cn_num=`SELECT COUNT(*) FROM comp_nodes;`
if($cn_num != 0)
{
	    --echo "comp is not empty"
	        --exit
}

--let $sds_num=`SELECT COUNT(*) FROM shards;`
if($sds_num != 0)
{
    --echo "shards is not empty"
    --exit
}

--let $sn_num=`SELECT COUNT(*) FROM shard_nodes;`
if($sn_num != 0)
{
    --echo "shard_nods is not empty"
    --exit
}

--let $dc_num=`SELECT COUNT(*) FROM db_clusters;`
if($dc_num != 0)
{
    --echo "db_clusters is not empty"
    --exit
}

--let $commit_name='commit_log_$cluster_name'
--let $ddlops_name='ddl_ops_log_$cluster_name'
--let $commit_exist=`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=$commit_name;`
if($commit_exist != 0) 
{
    --echo "commit_log is not deleted, please check"
    --exit
}

--let $ddlops_exist=`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=$ddlops_name;`
if($ddlops_exist != 0) 
{
    --echo "ddl_ops_log is not deleted, please check"
    --exit
}

--echo "==== delete cluster ok ===="
