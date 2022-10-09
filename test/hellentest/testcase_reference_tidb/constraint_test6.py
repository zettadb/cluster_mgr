import psycopg2


def test(hoststr, portstr):
        intport = int(portstr)
        conn = psycopg2.connect(host=hoststr, port=intport, user='abc', password='abc', database='postgres')
        conn.autocommit = True
        cur = conn.cursor()
        sqls=[
                "CREATE UNIQUE INDEX IDX_CODE_TABLE_7 ON TABLE_7(CODE);",
                "DROP INDEX IDX_CODE_TABLE_7;",
                "CREATE UNIQUE INDEX IDX_CODE_TABLE_7 ON TABLE_7(CODE,TITLE);"
                ]
        for sql in sqls:
                res = cur.execute(sql+";")
                print ("command:%s, res:%s" % (sql, str(res)))

if __name__ == '__main__':
        host = "192.168.0.129"
        port = "5341"
        test(host, port)

