import psycopg2


def test(hoststr, portstr):
        intport = int(portstr)
        conn = psycopg2.connect(host=hoststr, port=intport, user='abc', password='abc', database='postgres')
        conn.autocommit = True
        cur = conn.cursor()
        sqls=[
                "drop database if exists testdb;",
                "create database testdb;",
                "create table t1 (id int not null primary key auto_increment, col1 int);",
                "insert into t1 (col1) values (1),(2),(3),(4),(5);",
                "alter table t1 alter column col1 type bigint;",
                "alter table t1 add name varchar(20);",
                "alter table t1 drop column name;",
                "alter table t1 add sale_time date;",
                "alter table t1 alter column sale_time set default current_timestamp;",
                "drop table if exists t1;"
                ]
        for sql in sqls:
                res = cur.execute(sql+";")
                print ("command:%s, res:%s" % (sql, str(res)))

if __name__ == '__main__':
        host = "192.168.0.129"
        port = "5341"
        test(host, port)

