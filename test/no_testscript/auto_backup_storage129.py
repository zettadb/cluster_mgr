import requests,sys,time,json
  
def Create_backup_storage(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"create_backup_storage",
        "name":"hdfs_backup1", 
        "stype":"HDFS",
        "hostaddr":"192.168.0.129",
        "port":"57030"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("done" not in req.text) :
        sys.exit(1)

def Query_backup_storage(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"get_backup_storage"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    backup_storage_detailinfo = json.loads(req.text)
    print(backup_storage_detailinfo)
    backup_storage_name = backup_storage_detailinfo[0]["name"]
    return backup_storage_name

def Update_backup_storage(job_id,backup_storage_name):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"update_backup_storage",
        "name":backup_storage_name,
        "stype":"HDFS",
        "hostaddr":"192.168.0.129",
        "port":"59003"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("done" not in req.text) :
        sys.exit(1)

def Delete_backup_storage(backup_storage_name,job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"delete_backup_storage",
        "name":backup_storage_name
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("done" not in req.text) :
        sys.exit(1)

if __name__ == "__main__":
    url ='http://192.168.0.129:59000'  #
    head = {"Content-Type": "application/json"}
    job_id = "1649673219"
    Create_backup_storage(job_id)
    time.sleep(10)
    backup_storage_name = Query_backup_storage(job_id)
    #Update_backup_storage(job_id,backup_storage_name)
    #backup_storage_name = Query_backup_storage(job_id)
    #Delete_backup_storage(backup_storage_name,job_id)

