#
# Description:
# 1. check shard_nodes
# input parameters
#       hostaddr, port
#

--connection default

--let $wrapp_hostaddr=kl_str_concat(', $hostaddr, ')
--let $sn_num=query_get_value(SELECT COUNT(*) FROM shard_nodes WHERE hostaddr=$wrapp_hostaddr AND port=$port, COUNT(*), 1)
if($sn_num != 0)
{
    --echo $hostaddr still is in shard_nodes
    --exit
}