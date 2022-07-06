#
# Include this script to wait until the http connect to check
# request finished, status=done || status=failed
# http_req_timeout need be setted

--disable_result_log
--disable_query_log

if(!$http_req_timeout) 
{
    let $http_req_timeout = 500;
}

--let $job_id = `http_reap(job_id)`

--http
request_type: POST
header:Content-Type:application/json
body:{
"version": "1.0",
"job_id":"${job_id}",
"job_type": "get_status",
"timestamp" : "1435749309",
"paras": {}
}
EOF

--let $status = `http_reap(status)`

while ($status == 'ongoing' && $http_req_timeout > 0)
{
--http
request_type: POST
header:Content-Type:application/json
body:{
"version": "1.0",
"job_id":"${job_id}",
"job_type": "get_status",
"timestamp" : "1435749309",
"paras": {} 
}
EOF

--let $status = `http_reap(status)`
--sleep 10

let $http_req_timeout = $http_req_timeout - 10;
}

--enable_query_log
--enable_result_log