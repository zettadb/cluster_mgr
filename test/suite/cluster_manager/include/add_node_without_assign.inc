#
# Description:
# add node based on input params
#   need input params:
#           cluster_id, shard_id, nodes

--http
request_type: POST
header:Content-Type:application/json
body:{   
    "version": "1.0",
    "job_id":"",
    "job_type": "add_nodes",
    "timestamp" : "1435749309",
    "user_name":"kunlun_test",
    "paras":{
        "cluster_id":"${cluster_id}",
        "shard_id":"${shard_id}",
        "nodes":"${nodes}"
    }
}
EOF

--source kunlun-test/suite/cluster_manager/include/get_http_request_result.inc

--let $shard_nums = `SELECT COUNT(*) FROM shards WHERE db_cluster_id=$cluster_id`

while($shard_nums > 0) 
{
    --let $shard_name=shardname.$shard_nums
    --let $shardid=shard.$shard_nums
    --source kunlun-test/suite/cluster_manager/include/check_rbr_shard_state.inc
    dec $shard_nums;
}