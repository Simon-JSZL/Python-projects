# coding=utf-8
import pymssql
import datetime
import copy
from collections import Counter


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


def machine_id():
    this_machine_id = 'J5'
    return this_machine_id


def machine_procedure():
    this_machine_procedure = 'W2'
    return this_machine_procedure


def find_last_day():
    count = 0
    current_date = datetime.date.today()-datetime.timedelta(days=1)
    #current_date = datetime.date(2019, 2, 10)
    connect_jitai = ConnectServer(server="127.0.0.1", user="sa", password="123", database="DZVS")
    stop_date = datetime.date(2018, 1, 1)
    while 1:
        sql_searchlastday = "select count(1) as count from dbo.Indextable where convert(varchar(10), Createtime, 120) =\
         '" + str(current_date) + "'"
        result = connect_jitai.exec_all(sql_searchlastday)
        for (Value) in result:
            if Value[0] == 0:
                current_date = current_date-datetime.timedelta(days=1)
            elif Value[0] != 0:
                count += 1
        if count > 0 or current_date < stop_date:
            break
    return current_date


def return_macro_name(macro_id):
    if macro_id == 0:
        return '走折废'
    else:
        connect_jitai = ConnectServer(server="127.0.0.1", user="sa", password="123", database="DZVS")
        sql_get_table_id = "select ID from dbo.cur_model"
        table_id = connect_jitai.exec_one(sql_get_table_id)
        table_name = 'dbo.ModelMacroLog_' + str(table_id[0])
        sql_get_macro_name = "select top 1 MacroTitle as MacroName from " + table_name + " where MacroID= "\
                             + str(macro_id) + " order by ID DESC"
        macro_name = connect_jitai.exec_one(sql_get_macro_name)
        return macro_name[0]


def is_in_same_col(sheet1, sheet2):
    if sheet1 is not None and sheet2 is not None:
        for i in range(0, 5):
            if (7*i+1 <= sheet1 <= 7*i+7) and (7*i+1 <= sheet2 <= 7*i+7):
                return i+1
            i += 1
    return False


def each_day_wagon_fail():
    last_day = find_last_day()
    each_wagon_fail = {}
    this_day_wagon_fail = []
    connect_jitai = ConnectServer(server="127.0.0.1", user="sa", password="123", database="DZVS")
    sql_get_index = "select tablename,CreateTime from dbo.Indextable where convert(varchar(10), Createtime, 120) = '" +\
                    str(last_day) + "'"
    all_index = connect_jitai.exec_all(sql_get_index)
    for index in all_index:
        table_name = index[0]
        create_time_datetime_format = index[1]
        sql_total_fail = "select COUNT(1) as count from dbo." + table_name
        sql_ser_fail = "select COUNT(1) as count from dbo." + table_name + " where Reserve2=2"
        sql_psn = "select count(distinct PSN) as psnnum from dbo." + table_name + " where Reserve2=2"
        sql_max_k = "select top 1 FormatPos,COUNT(*) as count from dbo." + table_name +\
            " group by FormatPos order by count DESC"
        sql_max_m = "select top 1 MacroIndex as MacroId from dbo." + table_name +\
            " group by MacroIndex order by count(*) DESC"
        each_wagon_fail['WagonName'] = table_name[1:7]
        each_wagon_fail['CreateTime'] = create_time_datetime_format
        each_wagon_fail['TotalFail'] = connect_jitai.exec_one(sql_total_fail)[0]
        each_wagon_fail['SerFail'] = connect_jitai.exec_one(sql_ser_fail)[0]
        each_wagon_fail['Psn'] = connect_jitai.exec_one(sql_psn)[0]
        each_wagon_fail['MaxK'] = connect_jitai.exec_one(sql_max_k)[0]
        each_wagon_fail['MaxM'] = connect_jitai.exec_one(sql_max_m)[0]
        this_day_wagon_fail.append(copy.deepcopy(each_wagon_fail))

    return this_day_wagon_fail


def each_day_sum_fail():
    last_day_wagon_fail = each_day_wagon_fail()     #前一日的所有车次每车的作废信息
    last_day_sum_fail = {}      #用于保存前一日所有车次的合计作废信息
    last_day = find_last_day()  #前一日日期
    num_of_wagon = len(last_day_wagon_fail)
    connect_server = ConnectServer(server="127.0.0.1", user="sa", password="123", database="AnalyzedData")
    avg_total_fail = sum([total_fail.get('TotalFail') for total_fail in last_day_wagon_fail])//num_of_wagon
    avg_ser_fail = sum([ser_fail.get('SerFail') for ser_fail in last_day_wagon_fail])//num_of_wagon
    avg_psn = sum([psn.get('SerFail') for psn in last_day_wagon_fail])//num_of_wagon    #//为地板除法，不保留小数部分
    max_k_list = [max_k.get('MaxK') for max_k in last_day_wagon_fail]  #提取每一车的max_k保存为一个列表
    max_k = Counter(max_k_list).most_common(1)[0][0]    #使用most_common返回列表中最常出现的key-value，返回为包含单一元组的列表
    max_m_list = [max_m.get('MaxM') for max_m in last_day_wagon_fail]
    max_m = return_macro_name(Counter(max_m_list).most_common(1)[0][0])     #返回的出现次数最多的value对应的key为宏区域编号，再查询该编号对应的宏区域名称
    last_day_sum_fail['CreateTime'] = last_day
    last_day_sum_fail['AvgTotal'] = avg_total_fail
    last_day_sum_fail['AvgSer'] = avg_ser_fail
    last_day_sum_fail['AvgPsn'] = avg_psn
    last_day_sum_fail['MaxK'] = max_k
    last_day_sum_fail['MaxM'] = max_m
    sql = "insert into SumFail_"+machine_id()+"([CreateTime],[TotalFail],[SerFail],[PsnNum],[MaxK],[MaxM])\
    values('"+last_day.strftime('%Y%m%d')+"', "+str(avg_total_fail)+", "+str(avg_ser_fail)+", "+str(avg_psn)+", "+str(max_k)+", '"+max_m+"')"
    sql_check_if_exist = "select count(1) from dbo.SumFail_"+ machine_id() + " where CreateTime = '" + last_day.strftime('%Y-%m-%d') + "'"
    if connect_server.exec_one(sql_check_if_exist)[0] == 0:
        connect_server.exec(sql)
    return 0

each_day_sum_fail()

