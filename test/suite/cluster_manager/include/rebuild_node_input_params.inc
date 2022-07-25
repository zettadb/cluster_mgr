#
# Description:
# rebuild node based on input params
# need input params:
#           cluster_id, shard_id, allow_pull_from_master, allow_replica_delay,
#           rb_nodes
#               {"hostaddr":"127.0.0.1","port":"57001","need_backup":"0","hdfs_host":"hdfs","pv_limit":"10"
#               },{"hostaddr":"127.0.0.2","port":"57001","need_backup":"0","hdfs_host":"hdfs","pv_limit":"10"}
#     

--http
request_type: POST
header:Content-Type:application/json
body:{
    "version":"1.0",
    "job_id":"",
    "job_type":"rebuild_node",
    "user_name":"kunlun_test",
    "timestamp":"202205131532",
    "paras":{
        "cluster_id":"${cluster_id}",
        "shard_id":"${shard_id}",
        "allow_pull_from_master":"${allow_pull_from_master}",
        "allow_replica_delay":"${allow_replica_delay}",
        "rb_nodes":[
            ${rb_nodes}
        ]
    }
}
EOF


--source kunlun-test/suite/cluster_manager/include/get_http_request_result.inc

--source kunlun-test/suite/cluster_manager/include/check_create_cluster_state.inc
