import psycopg2


def test(hoststr, portstr):
        intport = int(portstr)
        conn = psycopg2.connect(host=hoststr, port=intport, user='abc', password='abc', database='postgres')
        conn.autocommit = True
        cur = conn.cursor()
        sqls=[
                "create table t1 (id int not null primary key auto_increment, col1 int);",
                "insert into t1 (col1) values (1),(2),(3),(4),(5);",
                "create user test1 with password '123456';",
                "alter user test1 with password 'abc123';",
                "grant all ON t1 to test1;",
                "revoke  all ON t1 from test1;",
                "drop user test1;",
                "drop table t1;"
                ]
        for sql in sqls:
                res = cur.execute(sql+";")
                print ("command:%s, res:%s" % (sql, str(res)))

if __name__ == '__main__':
        host = "192.168.0.129"
        port = "5341"
        test(host, port)

