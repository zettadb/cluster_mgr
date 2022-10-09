import psycopg2


def test(hoststr, portstr):
        intport = int(portstr)
        conn = psycopg2.connect(host=hoststr, port=intport, user='abc', password='abc', database='postgres')
        conn.autocommit = True
        cur = conn.cursor()
        sqls=[
                "DROP TABLE IF EXISTS users;",
                "CREATE TABLE users (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,username VARCHAR(60) NOT NULL,UNIQUE (username),CONSTRAINT min_username_length CHECK (CHARACTER_LENGTH(username) >=4));",
                "INSERT INTO users (username) VALUES ('a');"
                ]
        for sql in sqls:
                res = cur.execute(sql+";")
                print ("command:%s, res:%s" % (sql, str(res)))

if __name__ == '__main__':
        host = "192.168.0.129"
        port = "5341"
        test(host, port)

