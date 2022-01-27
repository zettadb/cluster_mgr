#coding:utf-8
import requests
import time,json
def Create_Cluster():
    datas = {
        "ver":"0.1",
        "job_id":"202112291043",
        "job_type":"create_cluster",
        "shards":"2",
        "nodes":"1",
        "comps":"1",
        "ha_mode":"no_rep",
        "max_storage_size":"20",
        "max_connections":"6",
        "cpu_cores":"8",
        "innodb_size":"4"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)

def Query_Cluster():
    datas = {"job_type":"get_status","job_id":"202112291043"}
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    result = json.loads(req.text)
    cluster_name = result["info"]
    #return cluster_name
    if ("succeed" not in req.text ) :
        sys.exit(1)
    else :
        cluster_name

def Add_shards(cluster_name):
    datas = {
        "ver":"0.1",
        "job_id":"202112291043",
        "job_type":"add_shards",
        "cluster_name":cluster_name,
        "shards":"2"
            }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)


def Del_shards(cluster_name):
    datas = {
        "ver":"0.1",
        "job_id":"202112291043",
        "job_type":"delete_shard",
        "cluster_name":cluster_name,
        "shard_name":"shard2"
            }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)


def Add_comps(cluster_name):
    datas = {
        "ver":"0.1",
        "job_id":"202112291043",
        "job_type":"add_comps",
        "cluster_name":cluster_name,
        "comps":"2"
            }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)


def Del_comps(cluster_name):
    datas = {
        "ver":"0.1",
        "job_id":"202112291043",
        "job_type":"delete_comp",
        "cluster_name":cluster_name,
        "comp_name":"comp2"
            }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)


def Delete_Cluster(cluster_name):
    datas = {
        "ver":"0.1",
        "job_id":"202112291043",
        "job_type":"delete_cluster",
        "cluster_name":cluster_name
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)


if __name__ == "__main__":  
    url ='http://192.168.0.127:57000'   
    head = {"Content-Type": "application/json"} 
    Create_Cluster()
    time.sleep(300)
    cluster_name = Query_Cluster() 
    time.sleep(60)
    Add_shards(cluster_name)
    time.sleep(180)
    Del_shards(cluster_name) 
    time.sleep(30)
    Add_comps(cluster_name)
    time.sleep(60)
    Del_comps(cluster_name)
    time.sleep(30)
    Delete_Cluster(cluster_name)





