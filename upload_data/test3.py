import sched, time
import pymssql
import datetime
import copy
from collections import Counter
s = sched.scheduler(time.time, time.sleep)


'''def do_something(sc):
    print("Doing stuff...")
    # do your stuff
    s.enter(5, 1, do_something, (sc,))


s.enter(5, 1, do_something, (s,))
s.run()'''


class ConnectServer:
    def __init__(self, server, user, password, database):
        self.server = server
        self.user = user
        self.password = password
        self.database = database

    def __get_connect(self):
        if not self.database:
            raise (NameError, "没有设置数据库信息")
        self.conn = pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database)
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur

    def exec_all(self, sql):
        cur = self.__get_connect()
        cur.execute(sql)
        result = cur.fetchall()
        self.conn.close()
        return result

    def exec_one(self, sql):
        cur = self.__get_connect()
        cur.execute(sql)
        result = cur.fetchone()
        self.conn.close()
        return result

    def exec(self, sql):
        cur = self.__get_connect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()


def check_recent_table():
    connect_jitai = ConnectServer(server="127.0.0.1", user="sa", password="123", database="DZVS")
    connect_server = ConnectServer(server="127.0.0.1", user="sa", password="123", database="AnalyzedData")
    sql_get_table_name = "SELECT name, create_date FROM (SELECT name, create_date, ROW_NUMBER() OVER (ORDER BY create_date DESC)\
            AS num FROM sys.tables) AS B WHERE B.num = 2"   #返回最新创建的倒数第二个表名
    recent_table_name = connect_jitai.exec_one(sql_get_table_name)[0]
    sql_check_if_exist = "SELECT COUNT(1) from dbo.AllIndex where WangonName = '" + recent_table_name + "'"
    if_exist_in_server = connect_server.exec_one(sql_check_if_exist)[0]
    if if_exist_in_server == 0:
        connect_server.conn.close()
        connect_jitai.conn.close()
    elif if_exist_in_server != 0:
        main()

check_recent_table()