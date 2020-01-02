#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Software: PyCharm
import sys,time,os
import traceback
import configparser
import warnings
import phoenixdb
import phoenixdb.cursor
import pymysql
import datetime
import hashlib
import smtplib
from email.mime.text import MIMEText

def send_mail465(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def send_mail(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_ds_mysql_dic(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port),
                           user=user, passwd=password, db=service,
                           charset='utf8',
                           cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port),
                           user=user, passwd=password, db=service,
                           charset='utf8'
                          )
    return conn

def get_ds_phoenix(ip,port):
    url = 'http://{0}:{1}/'.format(ip,port)
    conn = phoenixdb.connect(url, autocommit=True)
    return conn

def get_sync_time_type_name(sync_time_type):
    if sync_time_type=="day":
       return '天'
    elif sync_time_type=="hour":
       return '小时'
    elif sync_time_type=="min":
       return '分'
    else:
       return ''

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    #get mail parameter
    config['send_user']                = cfg.get("sync", "send_mail_user")
    config['send_pass']                = cfg.get("sync", "send_mail_pass")
    config['acpt_user']                = cfg.get("sync", "acpt_mail_user")
    config['mail_title']               = cfg.get("sync", "mail_title")
    config['sync_table']               = cfg.get("sync", "sync_table")
    config['batch_size']               = cfg.get("sync", "batch_size")
    config['batch_size_incr']          = cfg.get("sync", "batch_size_incr")
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
    config['db_mysql']                 = get_ds_mysql_dic(db_mysql_ip, db_mysql_port, db_mysql_service, db_mysql_user, db_mysql_pass)
    config['db_mysql2']                = get_ds_mysql_dic(db_mysql_ip, db_mysql_port, db_mysql_service, db_mysql_user, db_mysql_pass)

    #get phoenix  parameter
    db_phoenix                         = cfg.get("sync", "db_phoenix")
    db_phoenix_ip                      = db_phoenix.split(':')[0]
    db_phoenix_port                    = db_phoenix.split(':')[1]
    config['db_phoenix_ip']            = db_phoenix_ip
    config['db_phoenix_port']          = db_phoenix_port
    config['db_phoenix']               = get_ds_phoenix(db_phoenix_ip, db_phoenix_port)
    return config

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def init(config,debug):
    config = get_config(config)
    #print dict
    if debug:
       print_dict(config)
    return config

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

def get_table_total_rows(db,tab):
    cr = db.cursor()
    v_sql="select count(0) from {0}".format(tab)
    #print(v_sql)
    cr.execute(v_sql)
    rs=cr.fetchone()
    cr.close()
    return  rs['count(0)']

def check_phoenix_table(config,tname):
    db_phoenix = config['db_phoenix']
    cursor     = db_phoenix.cursor()
    v_new_tab  = '"{0}"."{1}"'.format(config['db_mysql_service'],tname)
    v_cre_tab  = """CREATE TABLE {0}("id"  VARCHAR PRIMARY KEY)""".format(v_new_tab)
    v_drp_tab  = """drop TABLE {0}""".format(v_new_tab)
    try:
        cursor.execute(v_cre_tab)
        cursor.execute(v_drp_tab)
        print('hbase table :{0} not exists!'.format(tname))
        return False
    except:
        print('hbase table :{0} exists!'.format(tname))
        return True


def get_phoenix_desc(config,tname):
    db    = config['db_mysql']
    cur   = db.cursor()
    v_tab_header ='create table "{0}"."{1}"(\n'.format(config['db_mysql_service'],tname)
    v_tab_body=''
    v_tab_pk='CONSTRAINT pk PRIMARY KEY ('
    v_sql = '''
            SELECT  CONCAT('"',column_name,'"') as col_name,
                    data_type,
                    character_maximum_length as col_len,
                    column_key
            FROM information_schema.columns
             WHERE table_schema='{0}' AND table_name='{1}'
             ORDER BY ordinal_position
        '''.format(config['db_mysql_service'],tname)
    cur.execute(v_sql)
    rs=cur.fetchall()
    for key in rs:
        #print(key)

        if key['column_key']== 'PRI':
           v_tab_pk=v_tab_pk+key['col_name']+','
           v_col_null = ' not null'
        else:
            v_col_null = ''

        if key['data_type'].lower()=='varchar':
          if key['column_key'] == 'PRI':
             v_col_type='char({0})'.format(key['col_len'])
          else:
             v_col_type =key['data_type'].lower()

        elif key['data_type'].lower()=='int':
           v_col_type='integer'
        elif  key['data_type'].lower()=='tinyint':
           v_col_type = 'integer'
        elif key['data_type'].lower()=='datetime':
           v_col_type = 'timestamp'
        else:
           v_col_type = key['data_type'].lower()

        v_tab_body=v_tab_body+key['col_name'].ljust(30,' ')+v_col_type+v_col_null+',\n'

    v_cre_tab=v_tab_header+v_tab_body+v_tab_pk[0:-1]+')\n)'
    print(v_cre_tab)
    return v_cre_tab

def create_phoenix_tab(config,tname):
    db_phoenix   = config['db_phoenix']
    cur_phoenix  = db_phoenix.cursor()
    v_new_tab    = '"{0}"."{1}"'.format(config['db_mysql_service'], tname)
    v_cre_sql    ='create schema if not exists "sync"'
    cur_phoenix.execute(v_cre_sql)
    print('phoenix create schema sync ok!')
    v_cre_sql   = get_phoenix_desc(config,tname)
    cur_phoenix.execute(v_cre_sql)
    print('phoenix create table t_sync_log ok!')


def write_phoenix_rows(config,tab,row_data):
    v_rkey     = ''
    d_cols     = {}
    db_phoenix = config['db_phoenix']
    v_new_tab  = '"{0}"."{1}"'.format(config['db_mysql_service'], tab)
    pk_name    = get_sync_table_pk_names(config['db_mysql'],tab)
    cursor     = db_phoenix.cursor()
    for r in row_data:
        #print('write_phoenix_rows=',r)
        v_header = 'UPSERT INTO {0} ('.format(v_new_tab)
        v_values = ''
        for key in r:

            if r[key]['value'] is not None:

                v_header = v_header + '"' + key + '",'

                if r[key]['type'] == '253':  # varchar,date
                    v_values = v_values+ "'" +format_sql(str(r[key]['value'])) + "',"

                elif r[key]['type']  in ('1', '3', '8', '246'):  # int,decimal
                    v_values = v_values + str(r[key]['value']) + ","

                elif r[key]['type'] in ('7','12'):  # datetime,timestamp
                    v_values = v_values + "TO_TIMESTAMP('" + str(r[key]['value']).split('.')[0] + "'),"
                else:
                    v_values = v_values + "'" + format_sql(str(r[key]['value'])) + "',"

        v_upsert=v_header[0:-1]+') values ('+v_values[0:-1]+')'
        try:
          cursor.execute(v_upsert)
        except Exception as e:
          print('ERROR:',str(e))
          print(v_upsert)

def main():
    #init variable
    config = ""
    debug = False
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    #初始化
    config=init(config,debug)

    #process
    db_mysql     = config['db_mysql']
    mysql_cur    = db_mysql.cursor()
    start_time   = datetime.datetime.now()
    n_batch_size = int(config['batch_size'])
    n_start      = 0
    for tname in config['sync_table'].split(","):
          tab=tname.split(':')[0]
          #check phoenix table exists
          if not check_phoenix_table(config,tab):
             #create phoenix table
             create_phoenix_tab(config,tab)

          i_counter    = 0
          n_total_rows = get_table_total_rows(db_mysql, tab)
          v_where      = get_sync_where_incr_mysql(tname, config)
          while n_start<=n_total_rows:
              v_sql = 'select * from {0} {1} limit {2},{3}'.format(tab, v_where,n_start, n_batch_size)
              mysql_cur.execute('select * from {0} limit {1},{2}'.format(tab, n_start, n_batch_size))
              mysql_rs   = mysql_cur.fetchall()
              mysql_ls   = []
              mysql_desc = mysql_cur.description
              for i in range(len(mysql_rs)):
                  for j in range(len(mysql_rs[i])):
                       col_name = str(mysql_desc[j][0])
                       col_type = str(mysql_desc[j][1])
                       mysql_rs[i][col_name] = {'value' : mysql_rs[i][col_name],'type' : col_type}
                  mysql_ls.append(mysql_rs[i])
              i_counter = i_counter + len(mysql_rs)

              #write hbase table rows data
              write_phoenix_rows(config,tab,mysql_ls)
              if __name__ == "__main__":
                  main()

              print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%,elapse:{4}s"
                   .format(tab, n_total_rows, i_counter,
                           round(i_counter / n_total_rows * 100, 2),
                           str(get_seconds(start_time))), end='')
              print('')
              n_start  = n_start+n_batch_size
    db_mysql.close()

