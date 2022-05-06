#coding:utf-8
import requests,sys
import time,json
class Clustername:
    def Query_Allcluster(job_id):
        datas = {
            "ver":"0.1",
            "job_id":job_id,
            "job_type":"get_cluster_summary"
            }
        data = json.dumps(datas)
        req = requests.post(url,data,headers=head,verify=False)
        allcluster_info = json.loads(req.text)
        total_num=len(allcluster_info)
        print(total_num)
        cluster_name_all = []
        for i in range(0,total_num):
            cluster_name = allcluster_info[i]["name"]
            cluster_name_all.append(cluster_name)
        #print(cluster_name_all)
        return cluster_name_all
        if ("name" not in req.text) :
            sys.exit(1)    

    def Handle_Clustername(cluster_name_all):
        cluster_name_number = len(cluster_name_all)
        for i in range(0,cluster_name_number):
            cluster_name = cluster_name_all[i]
            Clustername.Delete_Cluster(cluster_name,job_id)
            time.sleep(100)

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
    job_id = "1646883822"
    cluster_name_all =  Clustername.Query_Allcluster(job_id)
    Clustername.Handle_Clustername(cluster_name_all)

