#
# Description:
# add computer based on input params
#   need input params:
#           cluster_id, comps

--http
request_type: POST
header:Content-Type:application/json
body:{   
    "version": "1.0",
    "job_id":"",
    "job_type": "add_comps",
    "timestamp" : "1435749309",
    "user_name":"kunlun_test",
    "paras":{
        "cluster_id":"${cluster_id}",
        "comps":"${comps}"
    }
}
EOF

--source kunlun-test/suite/cluster_manager/include/get_http_request_result.inc

--connection default
--let $comp_nums = `SELECT COUNT(*) FROM comp_nodes WHERE db_cluster_id=$cluster_id`

--let c_inc=1
while($comp_nums > 0) 
{
    let $cconn_name=cc_pg_conn.$c_inc;
    let $comp = computer.$comp_nums;   
    --pg_connect($cconn_name, $comp, $comp_user, $comp_pwd)
    --source kunlun-test/suite/cluster_manager/include/check_computer_state.inc
    dec $comp_nums;
    inc $c_inc;
    --disconnect $cconn_name
}