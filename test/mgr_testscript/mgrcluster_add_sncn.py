#coding:utf-8
import requests,psycopg2,sys
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
    com_ip = cluster_detailinfo[total_num-1]["ip"]
    com_port = cluster_detailinfo[total_num-1]["port"]
    return com_ip,com_port

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

def Add_shards(cluster_name,job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"add_shards",
        "cluster_name":cluster_name,
        "shards":"1",
        "user_name":"test1",
        "machinelist": [ {"hostaddr":"192.168.0.128"} ]
            }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)


def Del_shards(cluster_name,job_id,shardname):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"delete_shard",
        "cluster_name":cluster_name,
        "shard_name":shardname,
        "user_name":"test1"
            }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)


def Add_comps(cluster_name,job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"add_comps",
        "cluster_name":cluster_name,
        "comps":"2",
        "user_name":"test1",
        "machinelist": [ {"hostaddr":"192.168.0.128"} ]
            }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)


def Del_comps(cluster_name,job_id,compname):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"delete_comp",
        "cluster_name":cluster_name,
        "comp_name":compname,
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
    job_id = "1646995209"
    shardname = "shard1"
    compname = "comp1"
    Create_Cluster(job_id)
    time.sleep(400)
    cluster_name = Query_Cluster(job_id) 
    time.sleep(60)
    com_ip,com_port = Query_Clusterdetail(job_id,cluster_name)
    Check_Comp(com_ip, com_port)
    Add_shards(cluster_name,job_id)
    time.sleep(300)
    #Del_shards(cluster_name,job_id,shardname) 
    #time.sleep(200)
    #cluster_name = "cluster_1650364300_000001"
    Add_comps(cluster_name,job_id)
    time.sleep(200)
    com_ip,com_port = Query_Clusterdetail(job_id,cluster_name)
    Check_Comp(com_ip, com_port)
    Del_comps(cluster_name,job_id,compname)
    time.sleep(200)
    Delete_Cluster(cluster_name,job_id)





