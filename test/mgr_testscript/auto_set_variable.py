#coding:utf-8
import requests,sys
import time,json

def Set_variable(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"set_variable",
        "variable":"max_connections", 
        "value_int":"50000",
        "value_str":"50000",
        "ip":"192.168.0.128", 
        "port":"59301"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("true" not in req.text) :
        sys.exit(1)

def Get_variable(job_id):
    datas = {
        "ver":"0.1",
        "job_id":job_id,
        "job_type":"get_variable",
        "variable":"max_connections",  
        "ip":"192.168.0.128",  
        "port":"59301"
        }
    data = json.dumps(datas)
    req = requests.post(url,data,headers=head,verify=False)
    print(req.status_code,req.text)
    if ("true" not in req.text) :
        sys.exit(1)        

if __name__ == "__main__":
    url ='http://192.168.0.128:59000'
    head = {"Content-Type": "application/json"}
    job_id = "1649674169"
    Get_variable(job_id)
    Set_variable(job_id)
    Get_variable(job_id)
