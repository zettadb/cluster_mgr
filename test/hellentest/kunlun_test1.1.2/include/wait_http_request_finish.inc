
if(!$http_req_timeout)
{
    --let $http_req_timeout = 500
}

#--echo $job_id
--sleep 5

--let $condition=1
while ($condition == 1)
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
if ($status != ongoing)
  {
        --let $condition=0
   }

--sleep 10
#--echo status: $status
--dec $http_req_timeout
if($http_req_timeout <= 0)
  {
       --let $condition=0
       --exit
   }

--let $cluster_name = `http_reap(attachment.cluster_name)`
--let $cluster_id = `http_reap(attachment.cluster_id)`
}

