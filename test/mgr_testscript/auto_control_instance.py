#coding:utf-8
import requests,sys
import time,json

def Control_instance(job_id,step):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"control_instance",
        "control":step,
        "ip":"192.168.0.128", 
        "port":"59030",
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
    job_id = "1649748060"
    step = "stop"
    Control_instance(job_id,step)
    time.sleep(100)
    step = "start"
    Control_instance(job_id,step)
    time.sleep(50)
    step = "restart"
    Control_instance(job_id,step)
    time.sleep(200)
