
if(!$http_req_timeout)
{
    --let $http_req_timeout = 500
}

#--echo $job_id
--sleep 5

--let $condition=1
while ($condition == 1)
{
--connection default
--let $status = `select status from cluster_general_job_log  where job_type='shard_coldbackup' order by id desc limit 1;`

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

}
