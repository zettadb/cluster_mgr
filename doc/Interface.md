### 1 发起扩容任务
#### 请求参数
```
{
  "version": "1.0",
  "job_id":"",
  "job_type": "expand_cluster",
  "timestamp" : "1435749309",
  "paras": {
    "cluster_id": "1",
    "dst_shard_id": "5",
    "src_shard_id": "2",
    "table_list": [
      "`sbtest`.`sbtest1`",
      "`sbtest`.`sbtest2`"
    ]
  }
}
```
#### 应答参数
```
{
  "version": "1.0",
  "error_code":"",
  "error_info":"",
  "extra_info":"",
  "timestamp" : "1435749309"
  "status":"",
  "job_id":"",
  "attachment":{}
}
```
### 2 查询扩容任务状态
#### 请求参数
```
{
  "version": "1.0",
  "job_id": "1",
  "job_type":"",
  "timestamp" : "1435749309",
  "paras": {}
}
```
#### 应答参数
```
{
  "version": "1.0",
  "error_code":"",
  "error_info":"",
  "extra_info":"",
  "timestamp" : "1435749309"
  "status":"",
  "job_id":"",
  "attachment":{}
}
```