#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/4/4 13:59
# @Author : 马飞
# @File : mysql_schedule_task.py
# @Software: PyCharm
import pymysql
import pymssql
import sys,datetime
import warnings
import configparser
import time
import smtplib
import os
from email.mime.text import MIMEText

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

def get_ds_sqlserver(ip,port,service,user,password):
    conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service,charset='utf8')
    return conn

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    sync_server_sour                  = cfg.get("sync","sync_db_server")
    sync_server_dest                  = cfg.get("sync","sync_db_mysql")
    db_sour_ip                        = sync_server_sour.split(':')[0]
    db_sour_port                      = sync_server_sour.split(':')[1]
    db_sour_service                   = sync_server_sour.split(':')[2]
    db_sour_user                      = sync_server_sour.split(':')[3]
    db_sour_pass                      = sync_server_sour.split(':')[4]
    db_dest_ip                        = sync_server_dest.split(':')[0]
    db_dest_port                      = sync_server_dest.split(':')[1]
    db_dest_service                   = sync_server_dest.split(':')[2]
    db_dest_user                      = sync_server_dest.split(':')[3]
    db_dest_pass                      = sync_server_dest.split(':')[4]
    market_id                         = cfg.get("sync","market_id")
    run_time                          = cfg.get("sync", "run_time")
    logfile                           = cfg.get("sync", "logfile")
    config['db_sqlserver_ip']         = db_sour_ip
    config['db_sqlserver_port']       = db_sour_port
    config['db_sqlserver_service']    = db_sour_service
    config['db_sqlserver_user']       = db_sour_user
    config['db_sqlserver_pass']       = db_sour_pass
    config['db_mysql_ip']             = db_dest_ip
    config['db_mysql_port']           = db_dest_port
    config['db_mysql_service']        = db_dest_service
    config['db_mysql_user']           = db_dest_user
    config['db_mysql_pass']           = db_dest_pass
    config['db_sqlserver_string']     = db_sour_ip+':'+db_sour_port+'/'+db_sour_service
    config['db_mysql_string']         = db_dest_ip+':'+db_dest_port+'/'+db_dest_service
    config['market_id']               = market_id
    config['run_time']                = run_time
    config['logfile']                 = logfile
    config['mail_title']              = cfg.get("sync","mail_title")
    config['today']                   = get_today()
    config['yesterday']               = get_yesterday()
    config['begin_time']              = get_begin_time()
    config['end_time']                = get_end_time()
    config['yesterday_begin_time']    = get_yesterday_begin_time()
    config['yesterday_end_time']      = get_yesterday_end_time()
    return config

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_now():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_yesterday_begin_time():
    return (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")+' 0:0:0'

def get_yesterday_end_time():
    return (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")+' 23:59:59'

def get_begin_time():
    return datetime.datetime.now().strftime("%Y-%m-%d")+' 0:0:0'

def get_end_time():
    return datetime.datetime.now().strftime("%Y-%m-%d")+' 23:59:59'

def get_today():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def get_yesterday():
    return (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

def log(message):
    return get_time()+':'+str(message)+'\n'


def check_mysql_yesterday(config):
    db_mysql     = get_ds_mysql(config['db_mysql_ip'],config['db_mysql_port'],config['db_mysql_service'],\
                                config['db_mysql_user'],config['db_mysql_pass'])
    cr_mysql     = db_mysql.cursor()
    v_sql        ="select count(0) from traffic where park_date='{0}' and market_id={1}".format(config['yesterday'],config['market_id'])
    #print("check_mysql_yesterday.sql=",v_sql)
    cr_mysql.execute(v_sql)
    rs_mysql=cr_mysql.fetchone()
    cr_mysql.close()
    return rs_mysql[0]
    

def calc_yesterday(config):
    db_sqlserver = get_ds_sqlserver(config['db_sqlserver_ip'],config['db_sqlserver_port'],\
                                    config['db_sqlserver_service'],config['db_sqlserver_user'],config['db_sqlserver_pass'])
    db_mysql     = get_ds_mysql(config['db_mysql_ip'],config['db_mysql_port'],config['db_mysql_service'],\
                                config['db_mysql_user'],config['db_mysql_pass'])
    cr_sqlserver = db_sqlserver.cursor()
    cr_mysql     = db_mysql.cursor()
    file_handle = open(config['logfile'], 'a')
    file_handle.write(log('sqlserver calc start....'))
    #1.sqlserver calc value
    v_sql_sqlserver1 ="""
select count(t.IDNO) from (
  SELECT IDNO
   FROM [TC].[RecordArchive]
  where InTime > '{0}' and InTime < '{1}'
  group by IDNO
) t""".format(config['yesterday_begin_time'],config['yesterday_end_time'])

    v_sql_sqlserver2="""
select count(t.IDNO) from (
 SELECT IDNO
  FROM [TC].[Record]
 where InTime > '{0}' and InTime < '{1}'
 group by IDNO
 ) t""".format(config['yesterday_begin_time'],config['yesterday_end_time'])
    cr_sqlserver.execute(v_sql_sqlserver1)
    rs_sqlserver=cr_sqlserver.fetchone()
    val1=rs_sqlserver[0]
    file_handle.write(log("""
sqlserver execute sql1={0}
-------------------------------------------------------------------------{1}
""".format(str(val1),v_sql_sqlserver1)))
    cr_sqlserver.execute(v_sql_sqlserver2)
    rs_sqlserver = cr_sqlserver.fetchone()
    val2 = rs_sqlserver[0]
    file_handle.write(log("""
sqlserver execute sql2={0}
-------------------------------------------------------------------------{1}
""".format(str(val2),v_sql_sqlserver2)))
    db_sqlserver.commit()
    hj=val1+val2
    file_handle.write(log('sqlserver calc sum='+str(hj)))
    #2.write mysql
    p_mysql_ins="""insert into traffic(park_flow,park_date,create_time,market_id)
                        values({0},'{1}','{2}','{3}')""".format(hj,config['yesterday'],get_time(),config['market_id'])
    p_rq=config['yesterday_begin_time']+'~'+config['yesterday_end_time']
    cr_mysql.execute(p_mysql_ins)
    db_mysql.commit()
    file_handle.write(log('mysql insert ok!'))
    v_content=get_html_calc(config,p_rq,str(hj))
    v_mail="""/home/hopson/apps/usr/webserver/schedule_task/sendmail.py '{0}' '{1}'""".\
           format(config['mail_title'],v_content)
    os.system(v_mail)
    file_handle.write(log('mail send success!'))
    print('mail send success!');
    file_handle.close()

def calc(config):
    db_sqlserver = get_ds_sqlserver(config['db_sqlserver_ip'],config['db_sqlserver_port'],\
                                    config['db_sqlserver_service'],config['db_sqlserver_user'],config['db_sqlserver_pass'])
    db_mysql     = get_ds_mysql(config['db_mysql_ip'],config['db_mysql_port'],config['db_mysql_service'],\
                                config['db_mysql_user'],config['db_mysql_pass'])
    cr_sqlserver = db_sqlserver.cursor()
    cr_mysql     = db_mysql.cursor()
    file_handle = open(config['logfile'], 'a')
    file_handle.write(log('sqlserver calc start....'))
    #1.sqlserver calc value
    v_sql_sqlserver1 ="""
select count(t.IDNO) from (
  SELECT IDNO
   FROM [TC].[RecordArchive]
  where InTime > '{0}' and InTime < '{1}'
  group by IDNO
) t""".format(config['begin_time'],config['end_time'])

    v_sql_sqlserver2="""
select count(t.IDNO) from (
 SELECT IDNO
  FROM [TC].[Record]
 where InTime > '{0}' and InTime < '{1}'
 group by IDNO
 ) t""".format(config['begin_time'],config['end_time'])
    cr_sqlserver.execute(v_sql_sqlserver1)
    rs_sqlserver=cr_sqlserver.fetchone()
    val1=rs_sqlserver[0]
    file_handle.write(log("""
sqlserver execute sql1={0}
-------------------------------------------------------------------------{1}
""".format(str(val1),v_sql_sqlserver1)))
    cr_sqlserver.execute(v_sql_sqlserver2)
    rs_sqlserver = cr_sqlserver.fetchone()
    val2 = rs_sqlserver[0]
    file_handle.write(log("""
sqlserver execute sql2={0}
-------------------------------------------------------------------------{1}
""".format(str(val2),v_sql_sqlserver2)))
    db_sqlserver.commit()
    hj=val1+val2
    file_handle.write(log('sqlserver calc sum='+str(hj)))
    #2.write mysql
    p_mysql_ins="""insert into traffic(park_flow,park_date,create_time,market_id)
                        values({0},'{1}','{2}','{3}')""".format(hj,config['today'],get_time(),config['market_id'])
    p_rq=config['begin_time']+'~'+config['end_time']
    cr_mysql.execute(p_mysql_ins)
    db_mysql.commit()
    file_handle.write(log('mysql insert ok!'))
    v_content=get_html_calc(config,p_rq,str(hj))
    v_mail="""/home/hopson/apps/usr/webserver/schedule_task/sendmail.py '{0}' '{1}'""".\
           format(config['mail_title'],v_content)
    os.system(v_mail)
    file_handle.write(log('mail send success!'))
    print('mail send success!');
    file_handle.close()

def db_to_html(str):
    return str.replace('\t','&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;').replace('\n','<br>')

def get_html_calc(config,p_rq,p_val):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tbody ='''<tr><td width=10%><b>源库</b></td> <td width=40%>{0}</td></tr>
              <tr><td width=10%><b>目标库</b></td> <td width=40%>{1}</td></tr>
              <tr><td width=10%><b>统计时间范围</b></td> <td width=40%>{2}</td></tr>
              <tr><td width=10%><b>停车记录</b></td> <td width=40%>{3}</td></tr>
           '''.format(config['db_sqlserver_string'],
                      config['db_mysql_string'],
                      p_rq,
                      p_val)

    v_html='''<html>
                <head>
                   <style type="text/css">
                       .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
                       .xwtable thead td {font-size: 12px;color: #333333;
                                  text-align: center;background: url(table_top.jpg) repeat-x top center;
                                      border: 1px solid #ccc; font-weight:bold;}
                       .xwtable thead th {font-size: 12px;color: #333333;
                                      text-align: center;background: url(table_top.jpg) repeat-x top center;
                                  border: 1px solid #ccc; font-weight:bold;}
                       .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
                       .xwtable tbody tr.alt-row {background: #f2f7fc;}
                       .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
                   </style>
                </head>
                <body>
                  <h4>发送时间：'''+nowTime+'''</h4>
                  <table class="xwtable">
                    <tbody>\n'''+tbody+'\n</tbody>\n'+'''
                  </table>
                </body>
                </html>
           '''.format(tbody)
    #print(v_html)
    return v_html

def check_full_sync_finish(config):
    try:
        if config['full_sync_' + get_today()]:
            return True
        else:
            return False
    except:
        return False

def main():
    #init variable
    cfg = ""
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            cfg = sys.argv[p + 1]
    #process
    config=get_config(cfg)
    print(config)
    start_time = datetime.datetime.now()
    while True:
        if get_now()>=config['run_time'] and not check_full_sync_finish(config):
           config=get_config(cfg)
           calc(config)
           config['full_sync_'+get_today()]=True
        #repair failure dataa
        if check_mysql_yesterday(config)==0:
           print("check_mysql_yesterday!")
           calc_yesterday(config) 
     
        time.sleep(10)


if __name__ == "__main__":
     main()
