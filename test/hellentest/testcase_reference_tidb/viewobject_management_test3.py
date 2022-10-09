import psycopg2


def test(hoststr, portstr):
        intport = int(portstr)
        conn = psycopg2.connect(host=hoststr, port=intport, user='abc', password='abc', database='postgres')
        conn.autocommit = True
        cur = conn.cursor()
        sqls=[
                "create table t_test (id int not null primary key auto_increment, col1 int);",
                "create view v_test as select * from t_test;",
                "create or replace view v_test as select * from t_test where 1=0;",
                "drop view v_test;",
                "drop table t_test;"
                ]
        for sql in sqls:
                res = cur.execute(sql+";")
                print ("command:%s, res:%s" % (sql, str(res)))

if __name__ == '__main__':
        host = "192.168.0.129"
        port = "5341"
        test(host, port)

