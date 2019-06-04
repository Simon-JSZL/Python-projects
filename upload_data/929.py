import sched
import time
import pymssql
import re
import decimal

decimal.__version__
'''将check_recent_table 的查询值修改正确，当前为1'''


class ConnectServer:
    def __init__(self):
        self.server = "10.17.84.22"
        self.user = "sa"
        self.password = "123"
        self.database = "AnalyzedData"

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


class ConnectJitai:
    def __init__(self):
        self.server = "192.168.50.13"
        self.user = "sa"
        self.password = "123"
        self.database = "DZVS"

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


class ConnectModel:
    def __init__(self):
        self.server = "192.168.50.13"
        self.user = "sa"
        self.password = "123"
        self.database = "MB"

    def __get_connect(self):
        if not self.database:
            raise (NameError, "没有设置数据库信息")
        self.conn = pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database)
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur

    def exec_one(self, sql):
        cur = self.__get_connect()
        cur.execute(sql)
        result = cur.fetchone()
        self.conn.close()
        return result


def machine_id():
    this_machine_id = '929'
    return this_machine_id


def machine_procedure():
    connect_server = ConnectServer()
    sql = "select SideId from dbo.MachineInfo where MachineId = '"+machine_id()+"'"
    side_id = connect_server.exec_one(sql)[0]
    if side_id == 0:
        machine_procedure = 'W1'
        return machine_procedure
    elif side_id == 1:
        machine_procedure = 'W2'
        return machine_procedure



def check_recent_table():
    rex = r"T[0-9][0-9]{0,1}[A-Z][A-Z][0-9][0-9][0-9]"     #定义一个正则表达式，检测返回的表为车号表
    table_num = 2   #该处要根据具体情况修改
    pattern = re.compile(rex)
    connect_jitai = ConnectJitai()
    connect_server = ConnectServer()
    while 1:
        sql_get_table_name = "SELECT name, create_date FROM (SELECT name, create_date, ROW_NUMBER() OVER (ORDER BY create_date DESC)\
                AS num FROM sys.tables) AS B WHERE B.num = "+str(table_num)   #返回最新创建的倒数第二个表名
        recent_table_name = connect_jitai.exec_one(sql_get_table_name)[0]
        recent_table_date = connect_jitai.exec_one(sql_get_table_name)[1]
        if pattern.fullmatch(recent_table_name) is not None:
            sql_check_if_exist = "SELECT COUNT(1) from dbo.AllIndex where WangonName = '" + recent_table_name[1:] + "' and MachineId_" + machine_procedure() + " = '" + machine_id() + "'"
            if_exist_in_server = connect_server.exec_one(sql_check_if_exist)[0]
            if if_exist_in_server != 0:     #如果allindex中已存在该车号，表示已经插入过了
                connect_server.conn.close()
                connect_jitai.conn.close()
                return 0
            elif if_exist_in_server == 0:   #如果allindex中不存在该车号，表示未执行插入操作，返回车号
                return [recent_table_name, recent_table_date]
        else:
            table_num += 1


def return_macro_name(macro_id):
    if macro_id == 0:
        return '走折废'
    else:
        connect_jitai = ConnectModel()
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


def insert_general_fail(table_name, crt_time):
    wagon_name = table_name[1:]
    create_time = crt_time.strftime("%Y%m%d %H:%M:%S")
    m_id = machine_id()
    m_procedure = machine_procedure()
    connect_jitai = ConnectJitai()
    connect_server = ConnectServer()
    sql_total_fail = "select COUNT(1) as count from dbo." + table_name
    sql_ser_fail = "select COUNT(1) as count from dbo." + table_name + " where Reserve2=2"
    sql_psn = "select count(distinct PSN) as psnnum from dbo." + table_name + " where Reserve2=2"
    sql_max_k = "select top 1 FormatPos,COUNT(*) as count from dbo." + table_name +\
        " group by FormatPos order by count DESC"
    sql_max_m = "select top 1 MacroIndex as MacroId from dbo." + table_name +\
        " group by MacroIndex order by count(*) DESC"
    total_fail = str(connect_jitai.exec_one(sql_total_fail)[0])
    ser_fail = str(connect_jitai.exec_one(sql_ser_fail)[0])
    psn = str(connect_jitai.exec_one(sql_psn)[0])
    max_k = str(connect_jitai.exec_one(sql_max_k)[0])
    macro_name = return_macro_name(connect_jitai.exec_one(sql_max_m)[0])

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
    elif m_procedure == 'W1' and check_if_exist[0] is None:
        connect_server.exec(sql_update_all_index)
        connect_server.exec(sql_insert_general_fail)
    elif m_procedure == 'W2' and check_if_exist[1] is None:
        connect_server.exec(sql_update_all_index)
        connect_server.exec(sql_insert_general_fail)


def insert_con_fail(table_name):
    wagon_name = table_name[1:]
    m_id = machine_id()
    connect_jitai = ConnectJitai()
    connect_server = ConnectServer()
    each_wagon_con_fail = []    #保存该车所有的连续废信息
    con_fail_images = []
    count_col = 0   #连续废发生的列
    max_con_psn = 10    #自己设定的连续废作废阈值
    con_fail = [[0]*3 for _ in range(1)]    #用来保存单车的连续废的二维数组
    i = 0
    j = 0
    count = 1
    sql_confail = "select PSN as psn,FormatPos as pos,MacroIndex as area,[Index] as Id from dbo." + table_name + " order by PSN"
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
                each_wagon_con_fail[j]['WagonName'] = wagon_name
                each_wagon_con_fail[j]['ConNumber'] = i+1
                each_wagon_con_fail[j]['StartPsn'] = con_fail[0][0]
                each_wagon_con_fail[j]['EndPsn'] = con_fail[i][0]
                each_wagon_con_fail[j]['ConCol'] = count_col
                each_wagon_con_fail[j]['ConArea'] = con_fail[0][2]
                sql_get_image1 = "select ErrorImage from dbo." + table_name + " where [index]=" + str(con_fail[0][3])
                bin_image1 = connect_jitai.exec_one(sql_get_image1)[0]
                sql_get_image2 = "select ErrorImage from dbo." + table_name + " where [index]=" + str(con_fail[len(con_fail)-2][3])
                bin_image2 = connect_jitai.exec_one(sql_get_image2)[0]
                con_fail_images.append({})
                con_fail_images[j]['Image1'] = ''.join(['%02x' % bytes for bytes in bin_image1])
                con_fail_images[j]['Image2'] = ''.join(['%02x' % bytes for bytes in bin_image2])
                j += 1
                con_fail = []
                i = 0
                count = 1
                con_fail.append(row_of_wagon_fail)
    for temp in range(len(each_wagon_con_fail)):
        sql_insert_con_fail = "insert into dbo.ConFail_" + m_id + \
                              "([WangonName],[ConNumber],[StartPsn],[EndPsn],[ConCol],[ConArea])values\
                              ('" + each_wagon_con_fail[temp]['WagonName'] + "',\
                               '" + str(each_wagon_con_fail[temp]['ConNumber']) + "',\
                               '" + str(each_wagon_con_fail[temp]['StartPsn']) + "',\
                               '" + str(each_wagon_con_fail[temp]['EndPsn']) + "',\
                               '" + str(each_wagon_con_fail[temp]['ConCol']) + "',\
                               '" + return_macro_name(each_wagon_con_fail[temp]['ConArea']) + "')\
                               insert into dbo.ConImage_" + m_id + \
                               "([ImageId],[ConImage1],[ConImage2]) values(SCOPE_IDENTITY(), '" +\
                               con_fail_images[temp]['Image1'] + "', '" + con_fail_images[temp]['Image2'] + "')"
        sql_checkifexist = "select count(1) as checksum from dbo.ConFail_" + m_id +\
                           " where WangonName ='" + each_wagon_con_fail[temp]['WagonName'] + \
                           "' and StartPsn = '" + str(each_wagon_con_fail[temp]['StartPsn']) + \
                           "' and EndPsn = '" + str(each_wagon_con_fail[temp]['EndPsn']) + "'"
        check_if_exist = connect_server.exec_one(sql_checkifexist)
        if check_if_exist[0] == 0:
            connect_server.exec(sql_insert_con_fail)


def insert_typ_fail(table_name):
    wagon_name = table_name[1:]
    m_id = machine_id()
    connect_jitai = ConnectJitai()
    connect_server = ConnectServer()
    each_wagon_typ_fail = {}
    typ_fail_images = {}
    j = 1  #设置一个增量用来标记每一车的三条典型废
    each_wagon_typ_fail['WagonName'] = typ_fail_images['WagonName'] = wagon_name
    sql_typ_fail = "select top 3 count(1) as count,FormatPos as pos,MacroIndex as area,avg(Reserve3) as dimension\
    from  dbo." + table_name + " where FormatPos != 15 and FormatPos != 8 and FormatPos != 22\
    group by FormatPos, MacroIndex order by count DESC"
    typ_fails = connect_jitai.exec_all(sql_typ_fail)
    for typ_fail in typ_fails:
        each_wagon_typ_fail['Pos'+str(j)] = str(typ_fail[1])      #每一车的典型废共有三组，使用j来标记每一组，将三组合并为一条，
        each_wagon_typ_fail['Area'+str(j)] = return_macro_name(typ_fail[2])     #这样可以使each_wagon_typ_fail中的一条表示一车的所有典型废
        each_wagon_typ_fail['Num' + str(j)] = str(typ_fail[0])
        each_wagon_typ_fail['Dim'+str(j)] = str(typ_fail[3])
        sql_get_image = "select top 1 ErrorImage as image from dbo." + table_name + \
                        " where FormatPos = " + str(typ_fail[1]) + " and MacroIndex = " + str(typ_fail[2]) + \
                        " order by Reserve3 DESC"
        bin_image = connect_jitai.exec_one(sql_get_image)[0]
        typ_fail_images['Image'+str(j)] = ''.join(['%02x' % bytes for bytes in bin_image])
        j += 1
    sql_insert_typ = "insert into dbo.TypicalFail_" + m_id + \
                     "([WangonName],[Max_Pos1],[Max_Area1],[Max_Num1],[Avg_Dim1],[Max_Pos2],\
                     [Max_Area2],[Max_Num2],[Avg_Dim2],[Max_Pos3],[Max_Area3],[Max_Num3],[Avg_Dim3])\
                     values('" + each_wagon_typ_fail['WagonName'] + "', '" + each_wagon_typ_fail['Pos1'] + "', '"\
                     + each_wagon_typ_fail['Area1'] + "', '" + each_wagon_typ_fail['Num1'] + "', '"\
                     + each_wagon_typ_fail['Dim1'] + "', '" + each_wagon_typ_fail['Pos2'] + "', '"\
                     + each_wagon_typ_fail['Area2'] + "', '" + each_wagon_typ_fail['Num2'] + "', '"\
                     + each_wagon_typ_fail['Dim2'] + "', '" + each_wagon_typ_fail['Pos3'] + "', '"\
                     + each_wagon_typ_fail['Area3'] + "', '" + each_wagon_typ_fail['Num3'] + "', '"\
                     + each_wagon_typ_fail['Dim3'] + "')"
    sql_insert_image = "insert into dbo.TypicalImage_" + m_id + \
                       "([WangonName],[TypImage1],[TypImage2],[TypImage3]) values('" + \
                       typ_fail_images['WagonName'] + "', '" + typ_fail_images['Image1'] + "', '"\
                       + typ_fail_images['Image2'] + "', '" + typ_fail_images['Image3'] + "')"

    connect_server.exec(sql_insert_typ)
    connect_server.exec(sql_insert_image)


s = sched.scheduler(time.time, time.sleep)
loop_time = 10


def loop_check(sc):
    if check_recent_table() != 0:   #判断得知最近创建的表不存在在服务器端，执行插入操作
        print("检测到新的车号生成，开始上传上一车数据，车号:"+str(check_recent_table()))
        table_name = check_recent_table()[0]
        create_time = check_recent_table()[1]
        insert_general_fail(table_name, create_time)
        insert_con_fail(table_name)
        insert_typ_fail(table_name)
        print("上传完成！")
    else:
        print("未检测到新的车号，等待下次检测")
    s.enter(loop_time, 1, loop_check, (sc,))


def run_program():
    s.enter(loop_time, 1, loop_check, (s,))
    s.run()


run_program()