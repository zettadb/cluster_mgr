#
# Description:
# check rbr mode state
#   1. check metadb shard_nodes
#   2. check m/s 
# input parameters:
#          shard_name, cluster_id, shard_id, shardid, 
#

# check master host status
#   show slave hosts
--connect($shard_name, $shardid, clustmgr, clustmgr_pwd)
--let $slave_host = query_get_value(SHOW SLAVE HOSTS, Host, 1)
if($slave_host == "No such row")
{
    --echo connect master host and get slave hosts failed
    --exit
}

if($slave_host == "NULL")
{
    --echo connect master host and get slave hosts empty
    --exit
}
--disconnect $shard_name

#check slave host status
# 
--connection default

--let $sd_master=`SELECT CONCAT(hostaddr, '_', port) FROM shard_nodes WHERE shard_id=$shard_id AND member_state='source' AND db_cluster_id=$cluster_id`
--let $ssd_nums=query_get_value(SELECT COUNT(*) FROM shard_nodes WHERE shard_id=$shard_id AND db_cluster_id=$cluster_id, COUNT(*), 1)
--dec $ssd_nums
while($ssd_nums > 0)
{
    --connection default
    --let $sl_hostaddr=query_get_value(SELECT hostaddr FROM shard_nodes WHERE shard_id=$shard_id AND member_state='replica' AND db_cluster_id=$cluster_id, hostaddr, $ssd_nums)
    --let $sl_port=query_get_value(SELECT port FROM shard_nodes WHERE shard_id=$shard_id AND member_state='replica' AND db_cluster_id=$cluster_id, port, $ssd_nums)

    --let $conn_name=kl_str_concat(rbr_slave_conn, _, $ssd_nums)
    --connect($conn_name, $sl_hostaddr, clustmgr, clustmgr_pwd, kunlun_sysdb, $sl_port)
    --let $slave_map_master=`SELECT CONCAT(host, '_', port) FROM PERFORMANCE_SCHEMA.REPLICATION_CONNECTION_CONFIGURATION WHERE CHANNEL_NAME='kunlun_repl'`
    if($slave_map_master != $sd_master)
    {
        --echo $sl_host connect master host not $sd_master
        --exit
    }

    --disconnect $conn_name
    --dec $ssd_nums
}

# check node_map_master records
--connection default
--let $sd_maste='$sd_master'
--let $sd_slaves=kl_query_get_value(`SELECT CONCAT(hostaddr, '_', port) AS host FROM shard_nodes WHERE shard_id=$shard_id AND member_state='replica' AND db_cluster_id=$cluster_id`)
--let $node_map_slaves=kl_query_get_value(`SELECT node_host AS host FROM node_map_master WHERE cluster_id=$cluster_id AND master_host=$sd_maste`)
--kl_sql_compare($sd_slaves, $node_map_slaves, host)