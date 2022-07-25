#
# Description:
# create cluster based on input params
# need input params:
#           nick_name, ha_mode, shards, nodes, comps, fullsync_level, dbcfg, storage_iplists
#     

if(!$fullsync_level)
{
    --let $fullsync_level = 1
}

if(!$dbcfg)
{
    --let $dbcfg = 1
}

--http
request_type: POST
header:Content-Type:application/json
body:{
    "version":"1.0",
    "job_id":"",
    "job_type":"create_cluster",
    "user_name":"kunlun_test",
    "timestamp":"202205131532",
    "paras":{
        "nick_name":"${nick_name}",
        "ha_mode":"${ha_mode}",
        "shards":"${shards}",
        "nodes":"${nodes}",
        "comps":"${comps}",
        "max_storage_size":"20",
        "max_connections":"6",
        "cpu_cores":"8",
        "innodb_size":"1",
        "dbcfg":"${dbcfg}",
        "fullsync_level":"${fullsync_level}",
        "storage_iplists":[
            ${storage_iplists}
        ]
    }
}
EOF


--source kunlun-test/suite/cluster_manager/include/get_http_request_result.inc

--let $cluster_id = `http_reap(attachment.cluster_id)`
--let $cluster_name = `http_reap(attachment.cluster_name)`
--let $comp_user = `http_reap(attachment.computer_user)`
--let $comp_pwd = `http_reap(attachment.computer_pwd)`

--source kunlun-test/suite/cluster_manager/include/check_create_cluster_state.inc