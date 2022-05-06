import requests,sys,time,json
  
def Create_machine(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"create_machine",
        "hostaddr":"192.168.0.129",
        "rack_id":"1",
        "datadir":"/home/kunlun/testmgr",
        "logdir":"/home/kunlun/testmgr",
        "wal_log_dir":"/home/kunlun/testmgr",
        "comp_datadir":"/home/kunlun/testmgr",
        "total_mem":"1024",
        "total_cpu_cores":"8",
        "user_name":"test1"
         }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)

def Query_machine_summary(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"machine_summary"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    machine_detailinfo = json.loads(req.text)
    print(machine_detailinfo)
    machine_ip = machine_detailinfo[0]["ip"]
    return machine_ip

def Update_machine(job_id,machine_ip):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"update_machine",
        "hostaddr":machine_ip,
        "rack_id":"1",
        "datadir":"/home/kunlun/testmgr",
        "logdir":"/nvme2/kunlun/testmgr",
        "wal_log_dir":"/nvme2/kunlun/testmgr",
        "comp_datadir":"/nvme2/kunlun/testmgr",
        "total_mem":"4096",
        "total_cpu_cores":"8",
        "user_name":"test1"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)

def Delete_machine(job_id,machine_ip):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"delete_machine",
        "hostaddr":machine_ip,
        "user_name":"test1"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)

if __name__ == "__main__":
    url ='http://192.168.0.129:59000'  #
    head = {"Content-Type": "application/json"}
    job_id = "1649668396"
    Create_machine(job_id)
    time.sleep(5)
    machine_ip = Query_machine_summary(job_id)
    #Update_machine(job_id,machine_ip)
    #machine_ip = Query_machine_summary(job_id)
    Delete_machine(job_id,machine_ip)
    time.sleep(5)
    Create_machine(job_id)
