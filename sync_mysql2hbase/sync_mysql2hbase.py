#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Software: PyCharm

import sys
import configparser
import warnings
import happybase
import pymysql
import datetime

'''
   功能：获取MySQL数据库连接对象
'''
def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip,
                           port=int(port),
                           user=user,
                           passwd=password,
                           db=service,
                           charset='utf8',
                           cursorclass = pymysql.cursors.DictCursor)
    return conn

'''
   功能：获取hbase数据库连接对象
'''
def get_ds_hbase(ip,port):
    conn = happybase.Connection(host=ip, port=int(port),
                                timeout=3600000,
                                autoconnect=True,
                                table_prefix=None,
                                table_prefix_separator=b'_',
                                compat='0.98',
                                transport='buffered',
                                protocol='binary')
    conn.open()
    return conn

'''
   功能：获取时间类型中文名称
'''
def get_sync_time_type_name(sync_time_type):
    if sync_time_type=="day":
       return '天'
    elif sync_time_type=="hour":
       return '小时'
    elif sync_time_type=="min":
       return '分'
    else:
       return ''

'''
   功能：生成全局字典对象，用于参数传递
'''
def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")

    #get mail parameter
    config['sync_table']               = cfg.get("sync", "sync_table")
    config['batch_size']               = cfg.get("sync", "batch_size")
    config['sync_time_type']           = cfg.get("sync", "sync_time_type")
    config['sync_time_type_name']      = get_sync_time_type_name(config['sync_time_type'])

    #get mysql parameter
    db_mysql                           = cfg.get("sync", "db_mysql")
    db_mysql_ip                        = db_mysql.split(':')[0]
    db_mysql_port                      = db_mysql.split(':')[1]
    db_mysql_service                   = db_mysql.split(':')[2]
    db_mysql_user                      = db_mysql.split(':')[3]
    db_mysql_pass                      = db_mysql.split(':')[4]
    config['db_mysql_ip']              = db_mysql_ip
    config['db_mysql_port']            = db_mysql_port
    config['db_mysql_service']         = db_mysql_service
    config['db_mysql_user']            = db_mysql_user
    config['db_mysql_pass']            = db_mysql_pass
    config['db_mysql_string']          = db_mysql_ip + ':' + db_mysql_port + '/' + db_mysql_service
    config['db_mysql']                 = get_ds_mysql(db_mysql_ip, db_mysql_port, db_mysql_service, db_mysql_user, db_mysql_pass)
    config['db_mysql2']                = get_ds_mysql(db_mysql_ip, db_mysql_port, db_mysql_service, db_mysql_user, db_mysql_pass)

    #get hbase parameter
    db_hbase                           = cfg.get("sync", "db_hbase")
    db_hbase_ip                        = db_hbase.split(':')[0]
    db_hbase_port                      = db_hbase.split(':')[1]
    db_hbase_service                   = db_hbase.split(':')[2]
    config['db_hbase_ip']              = db_hbase_ip
    config['db_hbase_port']            = db_hbase_port
    config['db_hbase_service']         = db_hbase_service
    config['db_hbase']                 = get_ds_hbase(db_hbase_ip, db_hbase_port)
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
   功能：格式化字符串，使其满足写入数据库需求
'''
def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

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
   功能：获取MySQL表名的主键名称
   入口：db/数据库连接对象,tab/表名
   出口：主键名称,复合主键列名以逗号分隔
'''
def get_sync_table_pk_names(db,tab):
    cr_source = db.cursor()
    v_col=''
    v_sql="""select column_name 
              from information_schema.columns
              where table_schema=database() 
                and table_name='{0}' and column_key='PRI' order by ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col=v_col+i['column_name']+','
    cr_source.close()
    return v_col[0:-1]

'''
   功能：获取MySQL表增量同步条件
   入口：tab/表名,config/全局配置
   出口：表增量同步where条件字符串
'''
def get_sync_where_incr_mysql(tab,config):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if config['sync_time_type']=='day':
       v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} DAY)".format(v_rq_col,v_expire_time)
    elif config['sync_time_type']=='hour':
       v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} HOUR)".format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'min':
       v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} MINUTE)".format(v_rq_col, v_expire_time)
    else:
       v = ''
    if tab.split(':')[1]=='':
       return ''
    else:
       return v

'''
   功能：根据条件获取MySQL表行数
   入口：db/数据库对象,tab/表名,where/查询条件
   出口：表增量同步where条件字符串
'''
def get_table_total_rows(db,tab,where):
    cr = db.cursor()
    v_sql="select count(0) from {0} {1} ".format(tab,where)
    print(v_sql)
    cr.execute(v_sql)
    rs=cr.fetchone()
    cr.close()
    return  rs['count(0)']

'''
   功能：检测hbase中是否存在表
   入口：config/全局配置,tname/表名
   出口：True/存在，False/不存在
'''
def exist_hbase_table(config,tname):
    db_hbase  = config['db_hbase']
    tab_list  = db_hbase.tables()
    v_new_tab = config['db_hbase_service']+':'+tname
    print('exist_hbase_table=',tab_list)
    for t in tab_list:
        tab=bytes.decode(t)
        if tab==v_new_tab:
           print('hbase table :{0} exists!'.format(tname))
           return True
    print('hbase table :{0} not exists!'.format(tname))
    return False

'''
   功能：创建hbase表
   入口：config/全局配置,tname/表名
   出口：无返回值
'''
def create_hbase_table(config,tname):
    db_hbase=config['db_hbase']
    families = {
       'info':dict(max_versions=3)
    }
    v_new_tab=config['db_hbase_service']+':'+tname
    db_hbase.create_table(v_new_tab, families)
    print('hbase create table {0}'.format(v_new_tab))

'''
   功能：写hbase一行数据
   入口：config/全局配置,tab/表名,row_data/行数据
   出口：无返回值
'''
def write_hbase_rows(config,tab,row_data):
    v_rkey    = ''
    d_cols    = {}
    db_hbase  = config['db_hbase']
    pk_name   = get_sync_table_pk_names(config['db_mysql'],tab)
    v_new_tab = config['db_hbase_service'] + ':' + tab
    table     = db_hbase.table(v_new_tab)
    for r in row_data:
        for key in r:
            if key==pk_name:
               v_rkey=r[key]
            d_cols['info:'+key]=str(r[key])
        table.put(v_rkey,d_cols)

'''
   功能：每批写config['batch_size']行数据至hbase中
   入口：config/全局配置,tab/表名,row_data/行数据
   出口：无返回值
'''
def write_hbase_rows_batch(config,tab,row_data):
    v_rkey   = ''
    d_cols   = {}
    db_hbase = config['db_hbase']
    pk_name  = get_sync_table_pk_names(config['db_mysql'],tab)
    v_new_tab = config['db_hbase_service'] + ':' + tab
    table    = db_hbase.table(v_new_tab)
    bat      = table.batch()
    for r in row_data:
        for key in r:
            if key==pk_name:
               v_rkey=r[key]
            d_cols['info:'+key]=str(r[key])
        bat.put(v_rkey,d_cols)
    bat.send()

'''
   功能：主调函数
'''
def main():

    #初始化参数
    config = ""
    debug = False
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    #将参数写入配置字典config
    config=init(config,debug)

    #数据同步
    db_mysql     = config['db_mysql']
    mysql_cur    = db_mysql.cursor()
    start_time   = datetime.datetime.now()
    n_batch_size = int(config['batch_size'])
    n_start      = 0
    for tname in config['sync_table'].split(","):
          tab=tname.split(':')[0]
          if not exist_hbase_table(config,tab):
             create_hbase_table(config,tab)

          i_counter    = 0
          v_where      = get_sync_where_incr_mysql(tname, config)
          n_total_rows = get_table_total_rows(db_mysql, tab,v_where)
          while n_start <= n_total_rows:
              v_sql      = 'select * from {0} {1} limit {2},{3}'.format(tab, v_where,n_start, n_batch_size)
              mysql_cur.execute(v_sql)
              mysql_rs   = mysql_cur.fetchall()
              mysql_ls   = []
              mysql_desc = mysql_cur.description
              for i in range(len(mysql_rs)):
                  for j in range(len(mysql_rs[i])):
                       col_name = str(mysql_desc[j][0])
                       col_type = str(mysql_desc[j][1])
                       if col_type in ('1','3','8','246','12'):
                           mysql_rs[i][col_name]=str(mysql_rs[i][col_name])
                  mysql_ls.append(mysql_rs[i])
              i_counter = i_counter + len(mysql_rs)

              write_hbase_rows_batch(config,tab,mysql_ls)

              print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%,elapse:{4}s"
                   .format(tab, n_total_rows, i_counter,
                           round(i_counter / n_total_rows * 100, 2),
                           str(get_seconds(start_time))), end='')
              print('')
              n_start  = n_start+n_batch_size
    db_mysql.close()

if __name__ == "__main__":
     main()
