#coding:utf-8
import requests
import time,json,sys
def Create_Cluster():
    datas = {
        "ver":"0.1",
        "job_id":"202112290957",
        "job_type":"create_cluster",
        "shards":"2",
        "nodes":"3",
        "comps":"1",
        "ha_mode":"mgr",
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
    datas = {"job_type":"get_status","job_id":"202112290957"}
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    result = json.loads(req.text)
    cluster_name = result["info"]
    #return cluster_name
    if ("succeed" not in req.text ) :
        sys.exit(1)
    else :
        return cluster_name


def Delete_Cluster(cluster_name):
    datas = {
        "ver":"0.1",
        "job_id":"202112290957",
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
    #Query_Cluster()
    cluster_name = Query_Cluster()
    Delete_Cluster(cluster_name)



