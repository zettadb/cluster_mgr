--connection default
--let $shard_nums = `SELECT COUNT(*) FROM shards `
#--let $i = 0

while($shard_nums > 0)
{
	#--let $shardid = `select shard_id from shard_nodes where member_state ="source" and status = "creating" limit $i,1;`
	--let $shardid=shard.$shard_nums
	--echo $shardid
	#--set_cluster_id(4)
	#--kl_cluster_id(1)
	--connect(shard_master,$shardid, clustmgr, clustmgr_pwd)
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
	dec $shard_nums;
	#inc $i;
}
