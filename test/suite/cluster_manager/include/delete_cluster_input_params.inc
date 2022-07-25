#
# Description:
# delete cluster based on input params
# need input params:
#           cluster_id
#

--http
request_type: POST
header:Content-Type:application/json
body:{   
    "version": "1.0",
    "job_id":"",
    "job_type": "delete_cluster",
    "timestamp" : "1435749309",
    "user_name":"kunlun_test",
    "paras":{
        "cluster_id":"${cluster_id}"
    }
}
EOF

--source kunlun-test/suite/cluster_manager/include/get_http_request_result.inc

--let $cluster_name=`http_reap(attachment.cluster_name)`

--source kunlun-test/suite/cluster_manager/include/check_delete_cluster_state.inc