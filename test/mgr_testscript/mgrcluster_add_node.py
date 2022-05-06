#coding:utf-8
import requests,sys,psycopg2
import time,json
def Create_Cluster(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"create_cluster",
        "shards":"2",
        "nodes":"3",
        "comps":"1",
        "ha_mode":"mgr",
        "max_storage_size":"20",
        "max_connections":"6",
        "cpu_cores":"8",
        "innodb_size":"1",
        "user_name":"test1",
        "machinelist": [ {"hostaddr":"192.168.0.128"} ]
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)

def Query_Cluster(job_id):
    datas = {"job_type":"get_status","job_id":job_id}
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    result = json.loads(req.text)
    cluster_name = result["info"]
    return cluster_name
    if ("done" not in req.text ) :
        sys.exit(1)

def Query_Clusterdetail(job_id,cluster_name):
    datas = { "ver":"0.1", "job_type":"get_cluster_detail","job_id":job_id,"cluster_name":cluster_name}
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    cluster_detailinfo = json.loads(req.text)
    print(cluster_detailinfo)
    total_num=len(cluster_detailinfo)
    shard_port = cluster_detailinfo[0]["port"]
    com_ip = cluster_detailinfo[total_num-1]["ip"]
    com_port = cluster_detailinfo[total_num-1]["port"]
    return shard_port,com_ip,com_port

def Check_Comp(hoststr, portstr):
        intport = int(portstr)
        conn = psycopg2.connect(host=hoststr, port=intport, user='abc', password='abc', database='postgres')
        conn.autocommit = True
        cur = conn.cursor()
        sqls=["SET client_min_messages TO 'warning';",
                "drop table if exists t1111",
                "RESET client_min_messages;",
                "create table t1111(id int primary key, info text, wt int)",
                "insert into t1111(id,info,wt) values(1, 'record1', 1)",
                "insert into t1111(id,info,wt) values(2, 'record2', 2)",
                "update t1111 set wt = 12 where id = 1", "select * from t1111",
                "delete from t1111 where id = 1", "select * from t1111",
                "prepare q1(int) as select*from t1111 where id=$1",
                "begin",
                "execute q1(1)",
                "execute q1(2)",
                "prepare q2(text,int, int) as update t1111 set info=$1 , wt=$2 where id=$3",
                "execute q2('Rec1',2,1)",
                "commit",
                "execute q2('Rec2',3,2)",
                "drop table t1111"]
        for sql in sqls:
                res = cur.execute(sql+";")
                print ("command:%s, res:%s" % (sql, str(res)))


def Add_nodes(cluster_name,job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"add_nodes",
        "cluster_name":cluster_name,
        "shard_name":shardname,
        "nodes":"2",
        "user_name":"test1",
        "machinelist": [ {"hostaddr":"192.168.0.128"} ]
            }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)


def Del_nodes(cluster_name,job_id,shardname,port_num):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"delete_node",
        "cluster_name":cluster_name,
        "shard_name":shardname,
        "ip":"192.168.0.128", 
        "port":port_num,
        "user_name":"test1"
            }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)

def Delete_Cluster(cluster_name,job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"delete_cluster",
        "cluster_name":cluster_name,
        "user_name":"test1"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)


if __name__ == "__main__":  
    url ='http://192.168.0.128:59000'   
    head = {"Content-Type": "application/json"} 
    job_id = "1646895372"
    shardname = "shard2"
    Create_Cluster(job_id)
    time.sleep(400)
    cluster_name = Query_Cluster(job_id) 
    shard_port,com_ip,com_port = Query_Clusterdetail(job_id,cluster_name)
    Check_Comp(com_ip, com_port)
    Add_nodes(cluster_name,job_id)
    time.sleep(600)
    #cluster_name = "cluster_1651818709_000001"
    shard_port,com_ip,com_port = Query_Clusterdetail(job_id,cluster_name)
    time.sleep(60)
    Del_nodes(cluster_name,job_id,shardname,shard_port) 
    time.sleep(100)
    Check_Comp(com_ip, com_port)
    Query_Cluster(job_id)
    time.sleep(100)
    Delete_Cluster(cluster_name,job_id)





