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
import pymssql
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

def exception_info():
    e_str=traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    db_sour                           = cfg.get("sync","db_mysql")
    config['send_user']               = cfg.get("sync", "send_mail_user")
    config['send_pass']               = cfg.get("sync", "send_mail_pass")
    config['acpt_user']               = cfg.get("sync", "acpt_mail_user")
    config['mail_title']              = cfg.get("sync", "mail_title")
    db_sour_ip                        = db_sour.split(':')[0]
    db_sour_port                      = db_sour.split(':')[1]
    db_sour_service                   = db_sour.split(':')[2]
    db_sour_user                      = db_sour.split(':')[3]
    db_sour_pass                      = db_sour.split(':')[4]
    config['db_mysql_ip']             = db_sour_ip
    config['db_mysql_port']           = db_sour_port
    config['db_mysql_service']        = db_sour_service
    config['db_mysql_user']           = db_sour_user
    config['db_mysql_pass']           = db_sour_pass
    config['db_mysql_string']         = db_sour_ip+':'+db_sour_port+'/'+db_sour_service
    config['db_mysql']                = get_ds_mysql(db_sour_ip, db_sour_port ,db_sour_service, db_sour_user, db_sour_pass)
    return config

def check_mysql_tab_exists(config,tab):
   db=config['db_mysql_desc']
   cr=db.cursor()
   sql="""select count(0) from information_schema.tables
            where table_schema=database() and table_name='{0}'""".format(tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_mysql_tab_rows(config,tab):
   db=config['db_mysql_desc3']
   cr=db.cursor()
   sql="""select count(0) from {0}""".format(tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def init(config,debug):
    config = get_config(config)
    #print dict
    if debug:
       print_dict(config)
    return config

def sync(config,debug):
     v_msg=''

     #run bj
     if synced(config,218):
        r=os.system("sh /home/hopson/apps/dingshirenwu/hopson_market_analysis/sqoop_info_bj.sh")
        if r==0:
            v_msg = v_msg+'loading bj shell...\n'
        else:
            v_msg = v_msg +'loading bj error...\n'

     #run sh
     if synced(config, 110):
        r= os.system("sh /home/hopson/apps/dingshirenwu/hopson_market_analysis/sqoop_info_sh.sh")
        if r==0:
            v_msg = v_msg+'loading sh shell...\n'
        else:
            v_msg = v_msg +'loading sh error...\n'

     # run cd
     if synced(config, 108):
        r= os.system("sh /home/hopson/apps/dingshirenwu/hopson_market_analysis/sqoop_info_cd.sh")
        if r==0:
            v_msg = v_msg+'loading cd shell...\n'
        else:
            v_msg = v_msg +'loading cd error...\n'

     # run gz
     if synced(config, 132):
        r= os.system("sh /home/hopson/apps/dingshirenwu/hopson_market_analysis/sqoop_info_gz.sh")
        if r==0:
            v_msg = v_msg+'loading gz shell...\n'
        else:
            v_msg = v_msg +'loading gz error...\n'

     # run sales
     if synced(config, 999):
        r=os.system("sh /home/hopson/apps/dingshirenwu/hopson_market_analysis/sqoop_sale_data.sh")
        if r==0:
            v_msg = v_msg+'loading sales shell...\n'
        else:
            v_msg = v_msg +'loading sales error...\n'

     # run qt
     if synced(config, 234):
        r=os.system("sh /home/hopson/apps/dingshirenwu/hopson_market_analysis/sqoop_info_tmp.sh")
        if r==0:
            v_msg = v_msg+'loading qt shell...\n'
        else:
            v_msg = v_msg +'loading qt error...\n'

     # sum
     r=os.system("sh /home/hopson/apps/dingshirenwu/hopson_market_analysis/hopson_market_analysis.sh")
     if r==0:
        v_msg = v_msg+'loading sum shell...\n'
     else:
        v_msg = v_msg +'loading sum error...\n'

     send_mail465(config['send_user'], config['send_pass'], config['acpt_user'], config['mail_title'],v_msg)
     print('send mail success!')
	
#检测是否可以开始同步
def synced(config,p_market_id):
    db = config['db_mysql']
    cr=db.cursor()
    cr.execute('SELECT COUNT(0) FROM sync.t_mysql2mysql_sync_log WHERE STATUS=0 and market_id={0}'.format(p_market_id))
    rs=cr.fetchone()
    if rs[0]==0:
       return True
    else:
       return False

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
    sync(config, debug)

if __name__ == "__main__":
     main()
