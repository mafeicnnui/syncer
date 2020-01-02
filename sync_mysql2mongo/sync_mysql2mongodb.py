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
import pymongo
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

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port),
                           user=user, passwd=password, db=service,
                           charset='utf8',
                           cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_ds_mongo(ip,port,service):
    conn = pymongo.MongoClient(host=ip, port=int(port))
    return conn[service]

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
    config['db_mysql2']                 = get_ds_mysql(db_mysql_ip, db_mysql_port, db_mysql_service, db_mysql_user, db_mysql_pass)

    #get mongodb parameter
    db_mongo                           = cfg.get("sync", "db_mongo")
    db_mongo_ip                        = db_mongo.split(':')[0]
    db_mongo_port                      = db_mongo.split(':')[1]
    db_mongo_service                   = db_mongo.split(':')[2]
    config['db_mongo_ip']              = db_mongo_ip
    config['db_mongo_port']            = db_mongo_port
    config['db_mongo_service']         = db_mongo_service
    config['db_mongo']                 = get_ds_mongo(db_mongo_ip, db_mongo_port, db_mongo_service)
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

def get_table_total_rows(db,tab):
    cr = db.cursor()
    v_sql="select count(0) from {0}".format(tab)
    cr.execute(v_sql)
    rs=cr.fetchone()
    cr.close()
    return  rs['count(0)']

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
    db_mysql  = config['db_mysql']
    db_mongo  = config['db_mongo']
    mysql_cur = db_mysql.cursor()
    #write mongo
    start_time   = datetime.datetime.now()
    n_batch_size = int(config['batch_size'])
    n_start      = 1
    for tab in config['sync_table'].split(","):
          i_counter    = 0
          n_total_rows = get_table_total_rows(db_mysql, tab)
          while n_start<=n_total_rows:
              v_sql='select * from {0} limit {1},{2}'.format(tab, n_start, n_batch_size)
              mysql_cur.execute('select * from {0} limit {1},{2}'.format(tab, n_start, n_batch_size))
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
              db_mongo[tab].insert_many(mysql_ls)
              print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%,elapse:{4}s"
                   .format(tab, n_total_rows, i_counter,
                           round(i_counter / n_total_rows * 100, 2),
                           str(get_seconds(start_time))), end='')
              n_start  = n_start+n_batch_size
    db_mysql.close()

if __name__ == "__main__":
     main()
