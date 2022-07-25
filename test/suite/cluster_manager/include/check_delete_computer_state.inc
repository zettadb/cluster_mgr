#
# Description:
# 1. check comp_nodes
# input params
#   cluster_id, comp_id
#

--connection default

--let $comps_num=query_get_value(SELECT COUNT(*) FROM comp_nodes WHERE id=$comp_id AND db_cluster_id=$cluster_id, COUNT(*), 1)
if($comps_num != 0)
{
    --echo comp_id $comp_id is not deleted from comp_nodes
    --exit
}