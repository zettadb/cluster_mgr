#
# Description:
# add shard based on input params
#   need input params:
#           cluster_id, shards, nodes, storage_iplists

--http
request_type: POST
header:Content-Type:application/json
body:{   
    "version": "1.0",
    "job_id":"",
    "job_type": "add_shards",
    "timestamp" : "1435749309",
    "user_name":"kunlun_test",
    "paras":{
        "cluster_id":"${cluster_id}",
        "shards":"${shards}",
        "nodes":"${nodes}",
        "storage_iplists":[
            ${storage_iplists}
        ]
    }
}
EOF

--source kunlun-test/suite/cluster_manager/include/get_http_request_result.inc

--connection default
--let $shard_nums = `SELECT COUNT(*) FROM shards WHERE db_cluster_id=$cluster_id`

while($shard_nums > 0) 
{
    --let $shard_name=shardname.$shard_nums
    --let $shardid=shard.$shard_nums
    --let $shard_id=query_get_value(SELECT id FROM shards WHERE db_cluster_id=$cluster_id, id, $shard_nums)
    
    --source kunlun-test/suite/cluster_manager/include/check_rbr_shard_state.inc
    --dec $shard_nums
}