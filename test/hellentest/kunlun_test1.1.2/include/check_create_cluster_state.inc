#
# Description:
# 1. check shard status
# 2. check pg status
# 3. check metadb shards, shard_nodes, comp_nodes
#

--connection default

--let $shard_nums = `SELECT COUNT(*) FROM shards`
--let $comp_nums = `SELECT COUNT(*) FROM comp_nodes`

if($ha_mode == rbr)
{

while($shard_nums > 0) 
{
--let $shardid=shard.$shard_nums
--source kunlun-test/suite/cluster_manager/check_rbr_shard_state.inc
dec $shard_nums;
}

}

--let c_inc=1
while($comp_nums > 0) 
{
let $cconn_name=cc_pg_conn.$c_inc;
let $comp = computer.$comp_nums;   
--pg_connect($cconn_name, $comp, $comp_user, $comp_pwd)
--source kunlun-test/suite/cluster_manager/check_computer_state.inc
dec $comp_nums;
inc $c_inc;
--connection default
}


