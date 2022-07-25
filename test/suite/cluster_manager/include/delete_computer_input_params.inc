#
# Description:
# delete computer based on input params
# need input params:
#           cluster_id, comp_id
#

--http
request_type: POST
header:Content-Type:application/json
body:{   
    "version": "1.0",
    "job_id":"",
    "job_type": "delete_comp",
    "timestamp" : "1435749309",
    "user_name":"kunlun_test",
    "paras":{
        "cluster_id":"${cluster_id}",
        "comp_id":"${comp_id}"
    }
}
EOF

--source kunlun-test/suite/cluster_manager/include/get_http_request_result.inc
--sleep 2
--source kunlun-test/suite/cluster_manager/include/check_delete_computer_state.inc