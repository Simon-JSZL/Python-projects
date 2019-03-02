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
    #current_date = datetime.date.today()-datetime.timedelta(days=1)
    current_date = datetime.date(2019, 2, 10)
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
    connect_server = ConnectServer(server="127.0.0.1", user="sa", password="123", database="AnalyzedData")
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

    for value in this_day_wagon_fail:
        wagon_name = value['WagonName']
        create_time = value['CreateTime'].strftime("%Y%m%d %H:%M:%S")
        total_fail = str(value['TotalFail'])
        ser_fail = str(value['SerFail'])
        psn = str(value['Psn'])
        max_k = str(value['MaxK'])
        macro_name = return_macro_name(value['MaxM'])
        m_id = machine_id()
        m_procedure = machine_procedure()
        sql_insert_general_fail = "insert into dbo.GeneralFail_" + machine_id() +\
        "([WangonName],[CreateTime],[TotalFail],[SerFail],[PsnNum],[MaxK],[MaxM]) values\
        ('" + wagon_name + "', '" + create_time + "', '" + total_fail + "',\
         '" + ser_fail + "', '" + psn + "', '" + max_k + "', '" + macro_name + "')"

        sql_insert_all_index = "insert into dbo.AllIndex([WangonName],[CreateTime_" + m_procedure + "],\
        [MachineId_" + m_procedure + "])\
        values('" + wagon_name + "', '" + create_time + "', '" + m_id + "')"

        sql_update_all_index = "update dbo.AllIndex set [CreateTime_" + m_procedure + "] =\
        '" + create_time + "', [MachineId_" + m_procedure + "] = '" + m_id + "' where WangonName = '" + wagon_name + "'"

        sql_check_if_exist = "select MachineId_W1, MachineId_W2 from dbo.AllIndex where WangonName = '" + wagon_name+"'"

        check_if_exist = connect_server.exec_one(sql_check_if_exist)
        if check_if_exist is None:
            connect_server.exec(sql_insert_all_index)
            connect_server.exec(sql_insert_general_fail)
        elif machine_procedure() == 'W1' and check_if_exist[0] is None:
            connect_server.exec(sql_update_all_index)
            connect_server.exec(sql_insert_general_fail)
        elif machine_procedure() == 'W2' and check_if_exist[1] is None:
            connect_server.exec(sql_update_all_index)
            connect_server.exec(sql_insert_general_fail)

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
    connect_server.exec(sql)
    return last_day_sum_fail


def each_day_con_fail():
    last_day = find_last_day().strftime("%Y-%m-%d")     #前一天的日期
    each_wagon_con_fail = []    #保存前一日所有的连续废信息
    con_fail_images = []
    j = 0
    count_col = 0   #连续废发生的列
    max_con_psn = 10    #自己设定的连续废作废阈值
    sql_get_table_name = "select tablename from dbo.Indextable where convert(varchar(10), Createtime, 120) = '" + last_day + "'"
    connect_jitai = ConnectServer(server="127.0.0.1", user="sa", password="123", database="DZVS")
    connect_server = ConnectServer(server="127.0.0.1", user="sa", password="123", database="AnalyzedData")
    table_names = connect_jitai.exec_all(sql_get_table_name)
    for table_name in table_names:
        con_fail = [[0]*3 for _ in range(1)]    #用来保存单车的连续废的二维数组
        i = 0
        count = 1
        sql_confail = "select PSN as psn,FormatPos as pos,MacroIndex as area,[Index] as Id from dbo." + table_name[0][0:7] + " order by PSN"
        for row_of_wagon_fail in connect_jitai.exec_all(sql_confail):
            if con_fail[i][0] == 0 and con_fail[i][1] == 0 and con_fail[i][2] == 0: #给初始化的con_fail保存第一张的信息
                con_fail[i] = row_of_wagon_fail
            elif con_fail[i][0] == row_of_wagon_fail[0]:    #如果连着两条记录的psn相同表示他们是同一大张上的两条作废，直接跳过
                continue
            elif con_fail[i][0]+3 >= row_of_wagon_fail[0]:  #下一条记录的大张号和本条的差距不超过3，保证是连续废或者隔张废
                if (is_in_same_col(con_fail[i][1], row_of_wagon_fail[1]) > 0) and (con_fail[i][2] == row_of_wagon_fail[2]):     #下张的作废区域相同，并在同一列
                    count += 1  #作废计数加一
                    i += 1
                    con_fail.append(row_of_wagon_fail)  #将该张的作废信息加入到con_fail中
                    if count >= max_con_psn:  #如果连续作废数超过10即为连续废，记录作废发生的列
                        count_col = is_in_same_col(con_fail[i][1], row_of_wagon_fail[1])
                else:   #下一张大张号连续但是作废区域不同或不在同一列，作为新的起点赋给con_fail
                    con_fail[i] = row_of_wagon_fail
            elif con_fail[i][0]+3 < row_of_wagon_fail[0]:   #下一张的大张号不再连续
                if count < max_con_psn:     #计数不够，不算连续废，重置所有计数和信息
                    con_fail = []
                    i = 0
                    count = 1
                    con_fail.append(row_of_wagon_fail)
                elif count >= max_con_psn:    #符合连续废条件
                    each_wagon_con_fail.append({})    #插入confail表的作废信息
                    each_wagon_con_fail[j]['WagonName'] = table_name[0][1:7]
                    each_wagon_con_fail[j]['ConNumber'] = i+1
                    each_wagon_con_fail[j]['StartPsn'] = con_fail[0][0]
                    each_wagon_con_fail[j]['EndPsn'] = con_fail[i][0]
                    each_wagon_con_fail[j]['ConCol'] = count_col
                    each_wagon_con_fail[j]['ConArea'] = con_fail[0][2]
                    sql_get_image1 = "select ErrorImage from dbo." + table_name[0][0:7] + " where [index]=" + str(con_fail[0][3])
                    bin_image1 = connect_jitai.exec_one(sql_get_image1)[0]
                    sql_get_image2 = "select ErrorImage from dbo." + table_name[0][0:7] + " where [index]=" + str(con_fail[len(con_fail)-2][3])
                    bin_image2 = connect_jitai.exec_one(sql_get_image2)[0]
                    con_fail_images.append({})
                    con_fail_images[j]['Image1'] = bin_image1.hex()
                    con_fail_images[j]['Image2'] = bin_image2.hex()
                    j += 1
                    con_fail = []
                    i = 0
                    count = 1
                    con_fail.append(row_of_wagon_fail)
    for temp in range(len(each_wagon_con_fail)):
        sql_insert_con_fail = "insert into dbo.ConFail_"+machine_id() + \
                              "([WangonName],[ConNumber],[StartPsn],[EndPsn],[ConCol],[ConArea])values\
                              ('" + each_wagon_con_fail[temp]['WagonName'] + "',\
                               '" + str(each_wagon_con_fail[temp]['ConNumber']) + "',\
                               '" + str(each_wagon_con_fail[temp]['StartPsn']) + "',\
                               '" + str(each_wagon_con_fail[temp]['EndPsn']) + "',\
                               '" + str(each_wagon_con_fail[temp]['ConCol']) + "',\
                               '" + return_macro_name(each_wagon_con_fail[temp]['ConArea']) + "')\
                               insert into dbo.ConImage_" + machine_id() + \
                               "([ImageId],[ConImage1],[ConImage2]) values(SCOPE_IDENTITY(), '" +\
                               con_fail_images[temp]['Image1'] + "', '" + con_fail_images[temp]['Image2'] + "')"
        sql_checkifexist = "select count(1) as checksum from dbo.ConFail_" + machine_id() +\
                           " where WangonName ='" + each_wagon_con_fail[temp]['WagonName'] + \
                           "' and StartPsn = '" + str(each_wagon_con_fail[temp]['StartPsn']) + \
                           "' and EndPsn = '" + str(each_wagon_con_fail[temp]['EndPsn']) + "'"
        check_if_exist = connect_server.exec_one(sql_checkifexist)
        if check_if_exist[0] == 0:
            connect_server.exec(sql_insert_con_fail)


def each_day_typ_fail():
    connect_jitai = ConnectServer(server="127.0.0.1", user="sa", password="123", database="DZVS")
    connect_server = ConnectServer(server="127.0.0.1", user="sa", password="123", database="AnalyzedData")
    last_day = find_last_day().strftime("%Y-%m-%d")  # 前一天的日期
    each_wagon_typ_fail = []
    typ_fail_images = []
    i = 0
    sql_get_table_name = "select tablename from dbo.Indextable where convert(varchar(10), Createtime, 120) = '" + last_day + "'"
    table_names = connect_jitai.exec_all(sql_get_table_name)
    for table_name in table_names:
        j = 1  #设置一个增量用来标记每一车的三条典型废
        each_wagon_typ_fail.append({})
        typ_fail_images.append({})
        each_wagon_typ_fail[i]['WagonName'] = table_name[0][1:7]
        typ_fail_images[i]['WagonName'] = table_name[0][1:7]
        sql_typ_fail = "select top 3 count(1) as count,FormatPos as pos,MacroIndex as area,avg(Reserve3) as dimension\
        from  dbo." + table_name[0][0:7] + " where FormatPos != 15 and FormatPos != 8 and FormatPos != 22\
        group by FormatPos, MacroIndex order by count DESC"
        typ_fails = connect_jitai.exec_all(sql_typ_fail)
        for typ_fail in typ_fails:
            each_wagon_typ_fail[i]['Pos'+str(j)] = str(typ_fail[1])      #每一车的典型废共有三组，使用j来标记每一组，将三组合并为一条，
            each_wagon_typ_fail[i]['Area'+str(j)] = return_macro_name(typ_fail[2])     #这样可以使each_wagon_typ_fail中的一条表示一车的所有典型废
            each_wagon_typ_fail[i]['Num' + str(j)] = str(typ_fail[0])
            each_wagon_typ_fail[i]['Dim'+str(j)] = str(typ_fail[3])
            sql_get_image = "select top 1 ErrorImage as image from dbo." + table_name[0][0:7] + \
                            " where FormatPos = " + str(typ_fail[1]) + " and MacroIndex = " + str(typ_fail[2]) + \
                            " order by Reserve3 DESC"
            typ_fail_images[i]['Image'+str(j)] = connect_jitai.exec_one(sql_get_image)[0].hex()
            j += 1
        i += 1
    #print(each_wagon_typ_fail)
    for temp in range(len(each_wagon_typ_fail)):
        sql_insert_typ = "insert into dbo.TypicalFail_" + machine_id() + \
                         "([WangonName],[Max_Pos1],[Max_Area1],[Max_Num1],[Avg_Dim1],[Max_Pos2],\
                         [Max_Area2],[Max_Num2],[Avg_Dim2],[Max_Pos3],[Max_Area3],[Max_Num3],[Avg_Dim3])\
                         values('" + each_wagon_typ_fail[temp]['WagonName'] + "', '" + each_wagon_typ_fail[temp]['Pos1'] + "', '"\
                         + each_wagon_typ_fail[temp]['Area1'] + "', '" + each_wagon_typ_fail[temp]['Num1'] + "', '"\
                         + each_wagon_typ_fail[temp]['Dim1'] + "', '" + each_wagon_typ_fail[temp]['Pos2'] + "', '"\
                         + each_wagon_typ_fail[temp]['Area2'] + "', '" + each_wagon_typ_fail[temp]['Num2'] + "', '"\
                         + each_wagon_typ_fail[temp]['Dim2'] + "', '" + each_wagon_typ_fail[temp]['Pos3'] + "', '"\
                         + each_wagon_typ_fail[temp]['Area3'] + "', '" + each_wagon_typ_fail[temp]['Num3'] + "', '"\
                         + each_wagon_typ_fail[temp]['Dim3'] + "')"
        sql_insert_image = "insert into dbo.TypicalImage_" + machine_id() + \
                           "([WangonName],[TypImage1],[TypImage2],[TypImage3]) values('" + \
                           typ_fail_images[temp]['WagonName'] + "', '" + typ_fail_images[temp]['Image1'] + "', '"\
                           + typ_fail_images[temp]['Image2'] + "', '" + typ_fail_images[temp]['Image3'] + "')";

        connect_server.exec(sql_insert_typ)
        connect_server.exec(sql_insert_image)


each_day_wagon_fail()
each_day_sum_fail()
each_day_con_fail()
each_day_typ_fail()
