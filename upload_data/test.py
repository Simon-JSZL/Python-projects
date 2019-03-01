# coding=utf-8
import pymssql
import datetime


def connect_server():
    conn = pymssql.connect(server="127.0.0.1", user="sa", password="123", database="AnalyzedData")
    cur = conn.cursor()
    wagon_name = '10ab123'
    m_id = 'J5'
    create_time = datetime.datetime.now()
    sql = "insert into dbo.AllIndex(WangonName,MachineId_W2) values('12321', 'J3')"
    #sql = "select * from dbo.AllIndex"
    cur.execute("insert into dbo.AllIndex(WangonName,MachineId_W2) values('123333', 'J3')")
    conn.commit()
    #value = cur.fetchall()
    #print(value)
    conn.close()

connect_server()

