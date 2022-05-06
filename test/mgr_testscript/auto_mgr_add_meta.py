import requests,sys,time,json

def Create_metadata(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"create_meta",
        "ha_mode":"mgr",
        "innodb_size":"1",
        "datadir":"/home/kunlun/testmgr",
        "logdir":"/home/kunlun/testmgr",
        "wal_log_dir":"/home/kunlun/testmgr"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)


def Query_meta(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"get_status"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("done" not in req.text) :
        sys.exit(1)


def Delete_meta(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"delete_meta"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("accept" not in req.text) :
        sys.exit(1)

if __name__ == "__main__":
    url ='http://192.168.0.128:59000'  #
    head = {"Content-Type": "application/json"}
    job_id = "1649661611"
    Create_metadata(job_id)
    time.sleep(150)
    Query_meta(job_id)
    Delete_meta(job_id)
    time.sleep(10)
    Create_metadata(job_id)
    time.sleep(200)
    Query_meta(job_id)


