#
# Description:
# create cluster without assigned storage and computer nodes
#

--http_connect($http_connection_name, cluster_mgr)

--let $nick_name=$nickname
--let $ha_mode=rbr
--let $shards=1
--let $nodes=3
--let $comps=1
--let $dbcfg=1

--source kunlun-test/suite/cluster_manager/include/create_cluster_without_assign.inc

--echo === create cluster ok ===