# -*- coding: utf-8 -*-
# @Time    : 2019/1/30 9:31
# @Author  : 马飞
# @File    : sync_mysql2mongo.py
# @Software: PyCharm
# @Func    : optimizer bulk insert

import sys,time
import configparser
import warnings
import pymongo
from   bson.objectid import ObjectId
import phoenixdb
import phoenixdb.cursor
import json
import datetime
from   dateutil import parser

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


'''
   功能：通过IP，PORT获取phonenix连接对象
'''
def get_ds_phoenix(ip,port):
    url = 'http://{0}:{1}/'.format(ip,port)
    conn = phoenixdb.connect(url, autocommit=True)
    return conn

'''
   功能：非认证方式连接MongoDB
   入口：MongoDB连接串，格式:IP:PORT:DBNAME
   出口：MongoDB连接对象
'''
def get_ds_mongo(mongodb_str):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    conn          = pymongo.MongoClient(host=ip, port=int(port))
    db            = conn[service]
    return db

'''
   功能：认证方式连接MongoDB
   入口：MongoDB连接串，格式:IP:PORT:DBNAME:USER:PASS
   出口：MongoDB连接对象
'''
def get_ds_mongo_auth(mongodb_str):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    user          = mongodb_str.split(':')[3]
    password      = mongodb_str.split(':')[4]
    conn          = pymongo.MongoClient('mongodb://{0}:{1}/'.format(ip,int(port)))
    db            = conn[service]
    db.authenticate(user, password)
    return db

'''
   功能：格式化MongoDB连接字符串
   入口：MongoDB连接串，格式:IP:PORT:DBNAME:USER:PASS
   出口：MongoDB连接字符串
'''
def get_ds_mongo_str(mongodb_str):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    return 'MongoDB:{0}:{1}/{2}'.format(ip,port,service)

'''
   功能：获取全局配置字典
   入口：配置文件名
   出口：全局配置字典
'''
def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")

    #get sync parameter
    config['sync_table']               = cfg.get("sync", "sync_table")
    config['batch_size']               = cfg.get("sync", "batch_size")
    config['batch_size_incr']          = cfg.get("sync", "batch_size_incr")
    config['gather_rows']              = cfg.get("sync", "gather_rows")
    config['sync_time_type']           = cfg.get("sync", "sync_time_type")

    #get mongodb parameter
    db_mongo                           = cfg.get("sync", "db_mongo")
    db_mongo_ip                        = db_mongo.split(':')[0]
    db_mongo_port                      = db_mongo.split(':')[1]
    db_mongo_service                   = db_mongo.split(':')[2]
    db_mongo_user                      = db_mongo.split(':')[3]
    db_mongo_password                  = db_mongo.split(':')[4]
    config['db_mongo_ip']              = db_mongo_ip
    config['db_mongo_port']            = db_mongo_port
    config['db_mongo_service']         = db_mongo_service
    config['db_mongo_user']            = db_mongo_user
    config['db_mongo_password']        = db_mongo_password
    config['db_mongo']                 = get_ds_mongo(cfg.get("sync", "db_mongo"))
    config['db_mongo_str']             = get_ds_mongo_str(cfg.get("sync", "db_mongo"))

    #get phoenix parameter
    db_phoenix                         = cfg.get("sync", "db_phoenix")
    db_phoenix_ip                      = db_phoenix.split(':')[0]
    db_phoenix_port                    = db_phoenix.split(':')[1]
    db_phoenix_service                 = db_phoenix.split(':')[2]
    config['db_phoenix_ip']            = db_phoenix_ip
    config['db_phoenix_port']          = db_phoenix_port
    config['db_phoenix_service']       = db_phoenix_service
    config['db_phoenix']               = get_ds_phoenix(db_phoenix_ip, db_phoenix_port)
    return config

'''
   功能：获取当前时间和传递时间之间的秒数
'''
def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

'''
   功能：格式化字典输出
'''
def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

'''
   功能：初始化并输出字典信息
'''
def init(config,debug):
    config = get_config(config)
    #print dict
    if debug:
       print_dict(config)
    return config

'''
   功能：检测字符串是否是日期格式
   入口：日期字符串
   出口：True/日期格式,False/非日期格式
'''
def is_valid_date(strdate):
    try:
        if ":" in strdate and len(strdate)==26 and time.strptime(strdate[0:19], "%Y-%m-%d %H:%M:%S"):
           return True

        if time.strptime(strdate, "%Y-%m-%d %H:%M:%S"):
           return True

        if time.strptime(strdate, "%Y-%m-%d"):
           return True

        return False
    except:
        return False


'''
   功能：获取mongoDB中列的最大长度
   入口：db/数据库对象
        tab/表名
        col/列名
        gather_rows/采集行数
   出口：列的最大长度
'''
def get_col_len(db,tab,col,gather_rows):
    c1      = db[tab]
    results = c1.find({col:{"$exists":"true"}},{col:1}).limit(int(gather_rows))
    n_len   = 0
    for rec in results:
        if len(str(rec[col]))>n_len:
           n_len=len(str(rec[col]))*3+500
    return n_len

'''
   功能：评估mongoDB中列的数据类型
   入口：db/数据库对象
        tab/表名
        col/列名
        gather_rows/采集行数
   出口：列的数据类型字符串
'''
def get_col_type(db,tab,col,gather_rows):
    c1  = db[tab]
    if gather_rows==0:
       results  = c1.find({col: {"$exists": "true"}}, {col: 1})
       n_totals = results.count()
    else:
       results  = c1.find({col: {"$exists": "true"}}, {col: 1}).limit(int(gather_rows))
       n_totals = gather_rows

    i_int_counter = 0
    i_dec_counter = 0
    i_str_counter = 0
    i_rq_counter  = 0
    i_str_length  = 0

    for rec in results:
        if str(rec[col]).isdigit() and str(rec[col]).count(',')==0 :
            i_int_counter = i_int_counter+1
        elif is_number(str(rec[col])) and str(rec[col]).count(',') == 0 and str(rec[col]).count('_') == 0:
            i_dec_counter = i_dec_counter + 1
        elif is_valid_date(str(rec[col])):
            i_rq_counter  = i_rq_counter+1
        else:
            i_str_counter = i_str_counter+1
            if col!='_id':
               i_str_length  = i_str_length if i_str_length >= len(str(rec[col])) else 3*len(str(rec[col]))+100
            else:
               i_str_length = i_str_length if i_str_length >= len(str(rec[col])) else 3*len(str(rec[col]))

    if i_str_counter>0:
       if i_str_length+100<4000:
          return '{0}:{1}:{2}'.format('varchar',str(i_str_length+100),'0')
       elif i_str_length< 8000:
          return '{0}:{1}:{2}'.format('text',str(i_str_length),'0')
       else:
          return '{0}:{1}:{2}'.format('longtext',str(i_str_length),'0')

    if i_int_counter>0 and i_dec_counter==0 and  i_str_counter==0 and i_rq_counter==0:
       return '{0}:{1}:{2}'.format('bigint', '0', '0')

    if i_dec_counter>0 and i_int_counter==0 and i_str_counter==0 and i_rq_counter==0:
       return '{0}:{1}:{2}'.format('decimal', '40', '4')

    if i_rq_counter>0 and i_dec_counter==0 and i_int_counter==0 and i_str_counter==0:
       return '{0}:{1}:{2}'.format('datetime', '0', '0')

'''
   功能：检测字符串是否是数值类型
   入口：字符串
   出口：True/数值,False/非数值
'''
def is_number(str):
  try:
    if str=='NaN':
      return False
    float(str)
    return True
  except ValueError:
    return False

'''
   功能：将id字符串转为MongoDB可识别的object_id列表
   入口：id列表
   出口：object_id列表
'''
def get_mongo_where(p_ids):
    try:
        v_ids = []
        for dic in p_ids:
            v_ids.append(ObjectId(dic))
        return v_ids
    except:
        return p_ids

'''
   功能：根据配置获取MongoDB表的增量同步条件
   入口：config配置，MongoDB表名
   出口：mongodb增量同步条件，json格式
'''
def get_mongo_incr_where(config,tab):
    v_day = tab.split(':')[2]
    if v_day =='':
       return {}

    n_day = int(tab.split(':')[2])
    v_col=tab.split(':')[1]

    if config['sync_time_type']=='day':
       v_rq = (datetime.datetime.now() + datetime.timedelta(days=-n_day)).strftime('%Y-%m-%dT%H:%M:%S.00Z')
    elif config['sync_time_type']=='hour':
       v_rq = (datetime.datetime.now() + datetime.timedelta(hours=-n_day)).strftime('%Y-%m-%dT%H:%M:%S.00Z')
    elif config['sync_time_type']=='min':
       v_rq = (datetime.datetime.now() + datetime.timedelta(minutes=-n_day)).strftime('%Y-%m-%dT%H:%M:%S.00Z')
    else:
       v_rq =''

    if v_rq =='':
       return {}
    v_rq = parser.parse(v_rq)
    v_json = {v_col: {"$gte": v_rq}}
    return v_json

'''
   功能：格式化字符串，使其满足写入数据库需求
'''
def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")


'''
   功能：返回phoenix表行数
'''
def get_phoenix_tab_exists_rows(config,tab):
    db_phoenix = config['db_phoenix']
    cursor     = db_phoenix.cursor()
    v_new_tab  = '"{0}"."{1}"'.format(config['db_phoenix_service'], tab)
    cursor.execute('select count(0) from {0}'.format(v_new_tab))
    rs = cursor.fetchone()
    return rs[0]

'''
   功能：生成phoenix表定义
   入口：db/mongodb连接对象
        schema/phoenix模式名
        tab/mongodb表名
        gather_rows/采集mongodb表行数
        debug/是否输出调试信息
   出口：phoenix建表语句     
        
'''
def get_tab_ddl(db,schema,tab,gather_rows,debug):
    desc       = set()
    stats      = {}
    c1         = db[tab]
    start_time = datetime.datetime.now()
    r_counter  = 0
    results    = ''
    n_totals   = 0

    if gather_rows==0:
       results  = c1.find()
       n_totals = results.count()
    else:
       results  = c1.find().limit(gather_rows)
       n_totals = gather_rows

    print('starting analyze table:{0} column type and length...'.format(tab))
    for rec in results:
        r_counter = r_counter + 1
        for key in rec:
            #add key to set
            desc.add(key)
            #calc column type stats
            v_counter = 0
            v_max_len = 0
            i_counter = 0
            f_counter = 0
            f_prec    = 4
            d_counter = 0

            if str(rec[key]).isdigit() and str(rec[key]).count(',') == 0 and str(rec[key])[0] != '0':
                if int(rec[key])>9223372036854775807:
                    f_counter = 1
                    f_prec    = 4
                    v_max_len = 40
                else:
                    i_counter = 1
                    f_prec    = 0
                    v_max_len = 0
            elif is_number(str(rec[key])) and str(rec[key]).count(',') == 0 \
                    and str(rec[key]).count('_') == 0 and str(rec[key]).count('.') == 1:
                f_counter = 1
                f_prec    = 4
                v_max_len = 40

            elif is_valid_date(str(rec[key])):
                d_counter = 1
                f_prec    = 0
                v_max_len = 0
            else:
                v_counter = 1
                f_prec    = 0
                v_max_len = len(str(rec[key]))

            if key in stats:
               stats[key]={
                           'v_counter':stats[key]['v_counter']+v_counter,
                           'v_max_len':v_max_len if v_max_len>stats[key]['v_max_len'] else stats[key]['v_max_len'],
                           'd_counter':stats[key]['d_counter']+d_counter,
                           'i_counter':stats[key]['i_counter']+i_counter,
                           'f_counter':stats[key]['f_counter']+f_counter,
                           'f_prec'   :f_prec
                          }
            else:
               stats[key]={
                           'v_counter':v_counter,
                           'd_counter':d_counter,
                           'v_max_len':v_max_len,
                           'i_counter':i_counter,
                           'f_counter':f_counter,
                           'f_prec'   :f_prec
                           }
            if  r_counter % 10000 ==0 :
                print('\rComputing table:{0} column type and length,process:{1}/{2} ,Complete:{3}%,elapse:{4}s'.
                         format(tab,n_totals,r_counter,
                                round(r_counter / n_totals * 100, 2),
                                str(get_seconds(start_time))),end='')

    print('\rComputing table:{0} column type and len,process:{1}/{2} ,Complete:{3}%,elapse:{4}s'.
          format(tab, n_totals, r_counter,
                 round(r_counter / n_totals * 100, 2),
                 str(get_seconds(start_time))), end='')
    print('')

    #output dict stats
    if debug:
       print_dict(stats)
       print(desc)

    d_desc={}
    for key in stats:
        if stats[key]['v_counter']>0:
            if stats[key]['v_max_len']+100< 4000:
               d_desc[key]={'type':'varchar','length':stats[key]['v_max_len']+100,'scale':stats[key]['f_prec']}
            elif stats[key]['v_max_len']+100< 8000:
               d_desc[key] = {'type': 'text', 'length': 0,'scale':stats[key]['f_prec']}
            else:
               d_desc[key] = {'type': 'longtext','length': 0,'scale':stats[key]['f_prec']}
        elif stats[key]['i_counter']>0 and stats[key]['f_counter']==0 and stats[key]['v_counter']==0 and stats[key]['d_counter']==0:
             d_desc[key] = {'type': 'bigint', 'length': 0,'scale':stats[key]['f_prec']}
        elif stats[key]['f_counter']>0 and stats[key]['i_counter']==0 and stats[key]['v_counter']==0 and stats[key]['d_counter']==0:
             d_desc[key] = {'type': 'decimal', 'length': stats[key]['v_max_len'],'scale':stats[key]['f_prec']}
        elif stats[key]['d_counter'] > 0 and stats[key]['i_counter'] == 0 and stats[key]['v_counter'] == 0 and stats[key]['f_counter'] == 0:
             d_desc[key] = {'type': 'datetime', 'length': 0, 'scale': stats[key]['f_prec']}
        else:
             d_desc[key] = {'type': 'varchar', 'length': stats[key]['v_max_len'], 'scale': stats[key]['f_prec']}
    if debug:
       print('print dict d_desc...')
       print_dict(d_desc)

    v_pre = ' '.ljust(5, ' ')
    v_ddl = 'create table "{0}"."{1}" (\n'.format(schema,tab)
    for key in d_desc:
        vkey='"{0}"'.format(key)
        if d_desc[key]['type'] == 'varchar':
            v_ddl = v_ddl + v_pre + vkey.ljust(40, ' ') + '{0},\n'.format(d_desc[key]['type'])
        elif d_desc[key]['type'] =='decimal':
            v_ddl = v_ddl + v_pre + vkey.ljust(40, ' ') + '{0},\n'.format('double')
        elif d_desc[key]['type'] =='int':
            v_ddl = v_ddl + v_pre + vkey.ljust(40, ' ') + '{0},\n'.format('integer')
        elif d_desc[key]['type'] in('datetime','date','timestamp'):
            v_ddl = v_ddl + v_pre + vkey.ljust(40, ' ') + '{0},\n'.format('varchar')
        else:
            v_ddl = v_ddl + v_pre + vkey.ljust(40, ' ') + d_desc[key]['type'] + ',\n'
    v_ddl = v_ddl[0:-2] + '\n,CONSTRAINT pk PRIMARY KEY ("_id")\n' + ')'
    return v_ddl


'''
  功能：获取phoenix列类型
  入口：config  全局配置字典
       tab     phoenix表名
  出口：phoenix表列类型字典
'''
def get_phoenix_ctype(config,tab):
    db = config['db_phoenix']
    cr = db.cursor()
    d_ctype={}
    v_schema=config['db_phoenix_service']
    v_sql="""
            select COLUMN_NAME,DATA_TYPE 
             from SYSTEM.CATALOG 
              where table_schem='{0}' and table_name='{1}'
          """.format(v_schema,tab)
    cr.execute(v_sql)
    rs=cr.fetchall()
    for i in rs:
        if i[0] is not None:
            key=i[0]
            val=i[1]
            d_ctype[key]=val
    return d_ctype

'''
  功能：全量同步phoenix表并返回全量同步表字典
  入口：config  全局配置字典
  出口：全量同步表字典
'''
def full_sync(config):
    db_mongodb  = config['db_mongo']
    db_mongodb2 = config['db_mongo']
    db_hbase    = config['db_phoenix']
    config_init = {}
    #sync data
    for tabs in config['sync_table'].split(","):
      tab = tabs.split(':')[0]
      config_init[tab] = False

      if check_phoenix_table(config,tab) and get_phoenix_tab_exists_rows(config,tab)==0:
         config_init[tab] = True
         cur_mongo        = db_mongodb[tab]
         cur_mongo2       = db_mongodb2[tab]
         results          = cur_mongo.find({'_id': {"$exists": "true"}}, {'_id': 1},no_cursor_timeout = True)
         start_time       = datetime.datetime.now()
         n_batch_size     = int(config['batch_size'])
         v_ctype          = get_phoenix_ctype(config, tab)
         n_total_rows     = results.count()
         i_counter        = 0
         v_ids            = []
         v_results        = []

         print('Table:{0} total {1} rows!'.format(tab,str(n_total_rows)))
         for rec in results:
             i_counter = i_counter + 1
             v_ids.append(rec['_id'])
             v_results = []
             if i_counter % n_batch_size == 0:
                 v_mongo_where   = get_mongo_where(v_ids)
                 v_mongo_results = cur_mongo2.find({"_id": {'$in': v_mongo_where}})

                 for r in v_mongo_results:
                     v_results.append(r)

                 for x in v_results:
                    x['_id']=str(x['_id'])
                    for key in x:
                        x[key]={'value':x[key],'type':v_ctype[key]}

                 write_phoenix_rows(config,tab,v_results)
                 v_ids = []

             if  i_counter%10000==0:
                 print("\rTable:{0},Total :{1},Process :{2},Complete:{3}%,elapse:{4}s"
                       .format(tab, n_total_rows, i_counter,
                               round(i_counter / n_total_rows * 100, 2),
                               str(get_seconds(start_time))), end='')

         #last batch
         if len(v_ids)>0:
            v_results       = []
            v_mongo_where   = get_mongo_where(v_ids)
            v_mongo_results = cur_mongo2.find({"_id": {'$in': v_mongo_where}})

            for r in v_mongo_results:
                v_results.append(r)

            for x in v_results:
                x['_id'] = str(x['_id'])
                for key in x:
                    x[key] = {'value': x[key], 'type': v_ctype[key]}
            write_phoenix_rows(config, tab, v_results)

            print("\rTable:{0},Total :{1},Process :{2},Complete:{3}%,elapse:{4}s"
                  .format(tab, n_total_rows, i_counter,
                          round(i_counter / n_total_rows * 100, 2),
                          str(get_seconds(start_time))), end='')
            print('')

      else:
        print('{0} Table: {1} already exists date,skip full sync!'.format(get_time(),tab))
    return config_init

'''
  功能：增量同步phoenix表
  入口：config  全局配置字典
       config_init 全量同步表字典
'''
def incr_sync(config,config_init):
    db_mongodb  = config['db_mongo']
    db_mongodb2 = config['db_mongo']

    #增量同步每一天表数据
    for tabs in config['sync_table'].split(","):
        tab = tabs.split(':')[0]
        day = tabs.split(':')[2]
        if check_phoenix_table(config,tab):
            cur_mongo    = db_mongodb[tab]
            cur_mongo2   = db_mongodb2[tab]
            start_time   = datetime.datetime.now()
            n_batch_size = int(config['batch_size_incr'])
            v_ctype      = get_phoenix_ctype(config, tab)
            i_counter    = 0
            v_ids        = []

            #如果表不配置同步天数，则开启全量同步
            if day=='':
               print('{0} Table: {1} no config sync column,starting full sync!'.format(get_time(),tab))

            #获取表增量同步条件
            v_incr       = get_mongo_incr_where(config, tabs)
            results      = cur_mongo.find(v_incr, {'_id': 1}, no_cursor_timeout=True)
            n_total_rows = results.count()
            if n_total_rows>0:
                for rec in results:
                    i_counter = i_counter + 1
                    v_ids.append(rec['_id'])
                    v_results = []
                    if i_counter % n_batch_size == 0:
                        v_mongo_where   = get_mongo_where(v_ids)
                        v_mongo_results = cur_mongo2.find({"_id": {'$in': v_mongo_where}},no_cursor_timeout = True)

                        for r in v_mongo_results:
                            v_results.append(r)

                        for x in v_results:
                            x['_id'] = str(x['_id'])
                            for key in x:
                                x[key] = {'value': x[key], 'type': v_ctype[key]}

                        #写phoenix数据
                        write_phoenix_rows(config, tab, v_results)
                        v_ids = []

                    if i_counter % int(config['batch_size_incr']) == 0:
                        print("\r{0} Table:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                              .format(get_time(),tab, n_total_rows, i_counter,
                                      round(i_counter / n_total_rows * 100, 2),
                                      str(get_seconds(start_time))), end='')

                #处理最后一批
                if len(v_ids) > 0:
                    v_results =[]
                    v_mongo_where = get_mongo_where(v_ids)
                    v_mongo_results = cur_mongo2.find({"_id": {'$in': v_mongo_where}})

                    for r in v_mongo_results:
                        v_results.append(r)

                    for x in v_results:
                        x['_id'] = str(x['_id'])
                        for key in x:
                            x[key] = {'value': x[key], 'type': v_ctype[key]}

                    print("\r{0} Table:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                          .format(get_time(),
                                  tab, n_total_rows, i_counter,
                                  round(i_counter / n_total_rows * 100, 2),
                                  str(get_seconds(start_time))), end='')
                    print('')

            else:
               print('{0} Table:{1} recent {2} {3} no found data,skip increment sync!'.
                     format(get_time(), tab, day,config['sync_time_type']))
        else:
           if day=='':
              print('{0} Table:{1} no exists! skip sync!'.format(get_time(),tab))

'''
  功能：检测phoenix表是否存在
  入口：config  全局配置字典
       tname   phoenix表名
  出口：True/存在，False/不存在     
'''
def check_phoenix_table(config,tname):
    db    = config['db_phoenix']
    cr    = db.cursor()
    v_sql = """select count(0)  from SYSTEM.CATALOG  where table_schem='{0}' and table_name='{1}'
            """.format(config['db_phoenix_service'], tname)
    cr.execute(v_sql)
    rs = cr.fetchone()
    print('check_phoenix_table=',rs[0])
    if rs[0]==0:
       return False
    else:
       return True

'''
  功能：检测phoenix表是否存在
  入口：config  全局配置字典
       tname   phoenix表名
  出口：True/存在，False/不存在     
'''
def create_phoenix_tab(config,tname):
    db_mongo     = config['db_mongo']
    db_phoenix   = config['db_phoenix']
    cur_phoenix  = db_phoenix.cursor()
    v_new_tab    = '"{0}"."{1}"'.format(config['db_phoenix_service'], tname)
    v_cre_sql    ='create schema if not exists "{0}"'.format(config['db_mongo_service'])
    cur_phoenix.execute(v_cre_sql)
    print('phoenix create schema {0} ok!'.format(config['db_phoenix_service']))
    v_cre_sql   = get_tab_ddl(db_mongo,config['db_phoenix_service'],tname,int(config['gather_rows']),True)
    print('v_cre_sql=',v_cre_sql)
    cur_phoenix.execute(v_cre_sql)
    print('phoenix create table t_sync_log ok!')

'''
  功能：写phoenix数据
  入口：config   全局配置字典
       tab      phoenix表名
       row_data 行数据 
'''
def write_phoenix_rows(config,tab,row_data):
    v_rkey     = ''
    d_cols     = {}
    db_phoenix = config['db_phoenix']
    v_new_tab  = '"{0}"."{1}"'.format(config['db_phoenix_service'], tab)
    pk_name    = '_id'
    cursor     = db_phoenix.cursor()
    for r in row_data:
        v_header = 'UPSERT INTO {0} ('.format(v_new_tab)
        v_values = ''
        for key in r:
            if r[key]['value'] is not None:
                v_header = v_header + '"' + key + '",'
                if r[key]['type'] in(1,12):  # char,varchar
                    v_values = v_values+ "'" +format_sql(str(r[key]['value'])) + "',"
                elif r[key]['type']  in (-5,4,8):  # bigint,integer,double
                    v_values = v_values + str(r[key]['value']) + ","
                else:
                    v_values = v_values + "'" + format_sql(str(r[key]['value'])) + "',"

        v_upsert=v_header[0:-1]+') values ('+v_values[0:-1]+')'
        try:
          cursor.execute(v_upsert)
        except Exception as e:
          print('ERROR:',str(e))
          print(v_upsert)

'''
  功能：创建phoenix表
  入口：config  全局配置字典
'''
def cre_tab(config):
    db_mongo = config['db_mongo']
    for tabs in config['sync_table'].split(","):
        tab           = tabs.split(':')[0]
        cur_mongo     = db_mongo[tab]
        results       = cur_mongo.find()
        n_totals      = results.count()

        if n_totals == 0:
            print("{0} Table:{1} 0 rows,skip sync!".format(get_time(),tab))
            continue

        #check hbase table exists
        if not check_phoenix_table(config, tab):
            # create hbase_table
            create_phoenix_tab(config, tab)


'''
  功能：主函数
'''
def main():
    #命令行获取参数
    config = ""
    debug = False
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    #读取配置至字典
    config = init(config,debug)
    config['debug'] = debug

    #创建hbase表
    print("Creating table,please wait!".format(get_time()))
    cre_tab(config)

    #全量同步
    print("Full Sync data,please wait!".format(get_time()))
    config_init=full_sync(config)

    #增量同步
    print("Increment Sync data,please wait!".format(get_time()))
    incr_sync(config,config_init)


if __name__ == "__main__":
     main()
