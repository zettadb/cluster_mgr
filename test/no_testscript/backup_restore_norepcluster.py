#coding:utf-8
import requests,psycopg2
import time,json,sys,datetime
def Create_Cluster(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"create_cluster",
        "shards":"1",
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
                "drop table t1111",
                "create table if not exists student(id serial4 PRIMARY KEY, num int4,name varchar(25))"]
        for sql in sqls:
                res = cur.execute(sql+";")
                print ("command:%s, res:%s" % (sql, str(res)))

def Pg_adddata(hoststr, portstr):
    intport = int(portstr)
    conn = psycopg2.connect(host=hoststr, port=intport, user='abc', password='abc', database='postgres')
    conn.autocommit = True
    cur = conn.cursor()
    sql = """CREATE TABLE student (
id serial4 PRIMARY KEY, 
num int4,
name varchar(25));"""
    for i in range(1,100001):
        sql ="INSERT INTO student (num, name) \
                    VALUES (%s, '%s')" % \
                    (100, 'zszxz')
        cur.execute(sql)
        i += 1
    conn.commit()
    conn.close()

def Pg_querydata(hoststr, portstr):
    intport = int(portstr)
    conn = psycopg2.connect(host=hoststr, port=intport, user='abc', password='abc', database='postgres')
    conn.autocommit = True
    cur = conn.cursor()
    sql ="""SELECT * FROM student;"""
    cur.execute(sql)
    rows = cur.fetchall()
    with open('data.txt','a+') as f:
        for i in range(len(rows)):
            #print(type(str(rows[i])))
            f.write(str(rows[i])+'\n')
    f.close()
    conn.commit()
    cur.close()
    conn.close()


def Backup_Cluster(cluster_name,job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"backup_cluster",
        "backup_cluster_name":cluster_name,
        "user_name":"test1"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)

def Restore_NewCluster(cluster_name,job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"restore_new_cluster",
        "timestamp":current_latetime,
        "backup_cluster_name":cluster_name,
        "user_name":"test1",
        "machinelist": [ {"hostaddr":"192.168.0.129"} ]
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
    url ='http://192.168.0.129:59000'   
    head = {"Content-Type": "application/json"}
    current_latetime = (datetime.datetime.now()+datetime.timedelta(minutes = 15)).strftime("%Y-%m-%d %H:%M:%S")
    #job_id= "1649838539"
    job_id= "1646883822"
    print(current_latetime)
    Create_Cluster(job_id)
    time.sleep(200)
    cluster_name = Query_Cluster(job_id)
    #cluster_name = "cluster_1650858502_000001"
    shard_port,com_ip,com_port = Query_Clusterdetail(job_id,cluster_name)
    Check_Comp(com_ip, com_port)
    Pg_adddata(com_ip, com_port)
    time.sleep(100)
    Pg_querydata(com_ip, com_port)
    time.sleep(10)
    Backup_Cluster(cluster_name,job_id)
    time.sleep(200)
    #Pg_querydata(com_ip, com_port)  #以下留给增量备份，目前增量备份没有开。
    #time.sleep(10)
    #Pg_adddata(com_ip, com_port)
    #time.sleep(100)
    Restore_NewCluster(cluster_name,job_id)
    time.sleep(600)
    cluster_name = Query_Cluster(job_id)
    shard_port,com_ip,com_port = Query_Clusterdetail(job_id,cluster_name)
    Check_Comp(com_ip, com_port)
    Delete_Cluster(cluster_name,job_id)
