#coding:utf-8
import requests,psycopg2
import time,json,sys
def Create_Cluster(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"create_cluster",
        "shards":"2",
        "nodes":"1",
        "comps":"1",
        "ha_mode":"no_rep",
        "max_storage_size":"20",
        "max_connections":"6",
        "cpu_cores":"8",
        "innodb_size":"1",
        "user_name":"test1", 
        "machinelist": [ {"hostaddr":"192.168.0.129"} ] 
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
    #return cluster_name
    if ("done" not in req.text ) :
        sys.exit(1)
    else :
        return cluster_name

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

def Rename_Cluster(cluster_name,job_id,nick_name):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"rename_cluster",
        "cluster_name":cluster_name,
        "nick_name":nick_name,
        "user_name":"test1"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("done" not in req.text) :
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
    url ='http://192.168.0.129:59000'   
    head = {"Content-Type": "application/json"} 
    job_id= "1646819726"
    Create_Cluster(job_id)
    time.sleep(200)
    cluster_name = Query_Cluster(job_id)
    com_ip,com_port = Query_Clusterdetail(job_id,cluster_name)
    Check_Comp(com_ip, com_port)
    nick_name = "kunlun@cluster_001"
    Rename_Cluster(cluster_name,job_id,nick_name)
    Delete_Cluster(cluster_name,job_id)
    #Delete_Cluster(nick_name,job_id)
    #time.sleep(80)
    #cluster_name = Query_Cluster(job_id)




