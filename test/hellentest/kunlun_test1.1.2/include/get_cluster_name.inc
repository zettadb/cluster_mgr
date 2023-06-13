--http
request_type: POST
header:Content-Type:application/json
body:{
	        "version":"1.0",
		 "job_id":"",
                 "job_type":"get_cluster_summary",
                 "timestamp" : "202205131532",
		 "paras":{ }
	}
EOF
--let $cluster_name = `http_reap(attachment.list_cluster[0].cluster_name)`
--let $cluster_id = `http_reap(attachment.list_cluster[0].cluster_id)`











