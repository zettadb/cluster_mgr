import psycopg2


def test(hoststr, portstr):
        intport = int(portstr)
        conn = psycopg2.connect(host=hoststr, port=intport, user='abc', password='abc', database='postgres')
        conn.autocommit = True
        cur = conn.cursor()
        sqls=[
                "DROP TABLE IF EXISTS TABLE_6 CASCADE;",
                "CREATE TABLE TABLE_6 (CODE CHAR(5) PRIMARY KEY,TITLE VARCHAR(40),DID INTEGER,DATE_PROD DATE,KIND VARCHAR(10));",
                "DROP TABLE IF EXISTS TABLE_7;",
                "CREATE TABLE TABLE_7 (CODE CHAR(5),TITLE VARCHAR(40),DID INTEGER,DATE_PROD DATE,KIND VARCHAR(10),CONSTRAINT CODE_TITLE PRIMARY KEY(CODE,TITLE));"
                "drop table table_6;"
                ]
        for sql in sqls:
                res = cur.execute(sql+";")
                print ("command:%s, res:%s" % (sql, str(res)))

if __name__ == '__main__':
        host = "192.168.0.129"
        port = "5341"
        test(host, port)

