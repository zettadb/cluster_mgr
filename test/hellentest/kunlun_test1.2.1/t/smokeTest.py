#! /usr/bin/python
# Copyright (c) 2019 ZettaDB inc. All rights reserved.
# This source code is licensed under Apache 2.0 License,
# combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

import psycopg2
import sys
import argparse
def test(hoststr, portstr):
	intport = int(portstr)
	conn = psycopg2.connect(host=hoststr, port=intport, user='abc', password='abc', database='postgres')
	conn.autocommit = True
	cur = conn.cursor()
	sqls=["SET client_min_messages TO 'warning';",
		"drop table if exists t1111",
		"RESET client_min_messages;",
		"create table t1111(id int primary key, info text, wt int)",
		"insert into t1111(id,info,wt) values(1, 'record1', 1)",
		"insert into t1111(id,info,wt) values(2, 'record2', 2)",
		"update t1111 set wt = 12 where id = 1", "select * from t1111",
		"delete from t1111 where id = 1", "select * from t1111",
		"prepare q1(int) as select*from t1111 where id=$1",
		"begin",
		"execute q1(1)",
		"execute q1(2)",
		"prepare q2(text,int, int) as update t1111 set info=$1 , wt=$2 where id=$3",
		"execute q2('Rec1',2,1)",
		"commit",
		"execute q2('Rec2',3,2)",
		"drop table t1111"]
	for sql in sqls:
        	res = cur.execute(sql+";")
		print "command:%s, res:%s" % (sql, str(res))

if __name__ == '__main__':
        parser = argparse.ArgumentParser(description="insert data for caict testing")
        parser.add_argument('--host', type=str, required=True, help='comp ip')
        parser.add_argument('--port', type=int, required=True, help='comp port')
        args = parser.parse_args()
        test(args.host,args.port)

