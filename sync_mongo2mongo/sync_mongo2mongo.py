#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/30 9:31
# @Author  : 马飞
# @File    : sync_mysql2mongo.py
# @Software: PyCharm
# #Function：单线程同步

import sys,time
import traceback
import configparser
import warnings
import pymongo
from bson.objectid import ObjectId
import json
import datetime
import smtplib
from email.mime.text import MIMEText
from dateutil import parser

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

def get_ds_mongo(mongodb_str):
    print('get_ds_mongo=A')
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    user          = mongodb_str.split(':')[3]
    password      = mongodb_str.split(':')[4]
    conn          = pymongo.MongoClient(host=ip, port=int(port))
    db            = conn[service]
    db.authenticate(user, password)
    return db

def get_ds_mongo_str(mongodb_str):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    return 'MongoDB:{0}:{1}/{2}'.format(ip,port,service)

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
    config['sync_time_type']           = cfg.get("sync", "sync_time_type")

    #get mongodb db
    config['db_mongo_from']            = get_ds_mongo_auth(cfg.get("sync", "db_mongo_from"))
    config['db_mongo_to']              = get_ds_mongo_auth(cfg.get("sync", "db_mongo_to"))
    config['db_mongo_from_str']        = get_ds_mongo_str(cfg.get("sync", "db_mongo_from"))
    config['db_mongo_to_str']          = get_ds_mongo_str(cfg.get("sync", "db_mongo_to"))
    return config

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def format_table(sync_tab):
    v_line     = ''
    v_lines    = ''
    i_counter  = 0
    v_space    = ' '.ljust(25,' ')
    for tab in  sync_tab.split(","):
        v_line    = v_line+tab+','
        i_counter = i_counter+1
        if i_counter%5==0:
           if i_counter==5:
              v_lines = v_lines + v_line[0:-1] + '\n'
              v_line  = ''
           else:
              v_lines = v_lines +v_space+ v_line[0:-1] + '\n'
              v_line  = ''
    v_lines = v_lines + v_space+v_line[0:-1]
    return v_lines

def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        if key=='sync_table':
           print(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=', format_table(config[key]))
        else:
           print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))


def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def init(config,debug):
    config = get_config(config)
    #print dict
    #if debug:
    print_dict(config)
    return config

def check_mongo_tab_exists(db,tab):
    try:
      c1 = db[tab]
      if c1.count()==0:
         return False
      else:
         return True
    except:
      return False

def get_mongo_incr_where(config,tab):
    v_rq  = ''
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



def get_mongo_incr_where_pk(tab):
    v_day = tab.split(':')[2]
    if v_day =='':
       return {}
    n_day = int(tab.split(':')[2])
    v_col=tab.split(':')[1]
    v_rq = (datetime.datetime.now() + datetime.timedelta(days=-n_day)).strftime('%Y-%m-%dT%H:%M:%S.00Z')
    v_rq = parser.parse(v_rq)
    v_json = {v_col: {"$gt": v_rq}}
    #v_json = json.dumps(v_json)
    return v_json

def write_pk(config,tabs):
    tab             = tabs.split(':')[0]
    db_mongodb_from = config['db_mongo_from']
    db_mongodb_to   = config['db_mongo_to']
    cur_mongo_from  = db_mongodb_from[tab]
    cur_mongo_to    = db_mongodb_to['t_pk']
    v_where         = get_mongo_incr_where_pk(tabs)

    print('v_where.write_pk=',v_where)
    results         = cur_mongo_from.find(v_where,{"_id":1})
    n_batch         = int(config['batch_size'])
    n_totals        = results.count()
    i_counter       = 1
    start_time      = datetime.datetime.now()

    #init t_pk
    db_mongodb_to.drop_collection('t_pk')
    db_mongodb_to.create_collection('t_pk')
    print('{0} create table {1} complete!'.format(config['db_mongo_to_str'], 't_pk'))

    # write t_pk
    print('{0} insert table {1} ,please wait!'.format(config['db_mongo_to_str'], 't_pk'))
    for dic in results:
        #print("""db.t_pk.insert("id":'{0}',"counter": {1})""".format(dic['_id'], i_counter))
        i_counter = i_counter + 1
        cur_mongo_to.insert({"id": dic['_id'], "counter": i_counter})
        if i_counter % n_batch == 0:
            print('\rMongoDB:t_pk insert {0}/{1} ,Complete:{2}%,elapse:{3}s'.
                  format(n_totals, i_counter, round(i_counter / n_totals * 100, 2), str(get_seconds(start_time))),
                  end='')
            #sys.exit(0)
    print('{0} {1} insert {2} rows ok!'.format(config['db_mongo_to_str'], 't_pk',str(n_totals)))

def full_sync(config):
    #process
    db_mongodb_from      = config['db_mongo_from']
    db_mongodb_to        = config['db_mongo_to']
    for tabs in config['sync_table'].split(","):
        start_time       = datetime.datetime.now()
        n_batch          = int(config['batch_size'])
        tab              = tabs.split(':')[0]
        cur_mongo_from   = db_mongodb_from[tab]
        cur_mongo_to     = db_mongodb_to[tab]
        results          = cur_mongo_from.find()
        n_totals         = results.count()

        if n_totals > 0 :
           if not check_mongo_tab_exists(db_mongodb_to,tab):
               print('{0} Full sync table:{1},please wait...'.format(get_time(),tab))
               i_counter = 0
               mylist    = []
               mylist_id = []
               cur_mongo_to.drop()
               for r in results:
                   mylist.append(r)
                   mylist_id.append(r['_id'])
                   cur_mongo_to.insert(r)
                   i_counter = i_counter + 1
                   if i_counter % n_batch == 0:
                       cur_mongo_to.remove({"_id": {'$in': mylist_id}})
                       cur_mongo_to.insert(mylist)
                       mylist = []
                       mylist_id = []
                       print("\r{0} Full sync Table:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                             .format(get_time(),tab, n_totals, i_counter,
                                     round(i_counter / n_totals * 100, 2),
                                     str(get_seconds(start_time))), end='')

               cur_mongo_to.remove({"_id": {'$in': mylist_id}})
               cur_mongo_to.insert(mylist)
               print("\r{0} Full sync Table:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                     .format(get_time(),tab, n_totals, i_counter,
                             round(i_counter / n_totals * 100, 2),
                             str(get_seconds(start_time))), end='')
               print('')
           else:
              print('{0} Full sync Table: {1} already exists date,skip full sync!'.format(get_time(), tab))
        else:
           db_mongodb_to.drop_collection(tab)
           db_mongodb_to.create_collection(tab)
           print('{0} Full sync Table {1} sync 0 records!'.format(get_time(),tab))


def increment_sync(config):
    db_mongodb_from    = config['db_mongo_from']
    db_mongodb_to      = config['db_mongo_to']
    for tabs in config['sync_table'].split(","):
        start_time     = datetime.datetime.now()
        n_batch        = int(config['batch_size'])
        tab            = tabs.split(':')[0]
        day            = tabs.split(':')[2]
        cur_mongo_from = db_mongodb_from[tab]
        cur_mongo_to   = db_mongodb_to[tab]
        v_where        = get_mongo_incr_where(config,tabs)
        #print('increment_sync=',increment_sync)
        results        = cur_mongo_from.find(v_where)
        n_totals       = results.count()
        if n_totals > 0 :
            print('{0} Increment sync table:{1},please wait...'.format(get_time(),tab))
            #if check_mongo_tab_exists(db_mongodb_to, tab):
            i_counter = 0
            mylist    = []
            mylist_id = []
            for r in results:
                mylist.append(r)
                mylist_id.append(r['_id'])
                i_counter = i_counter + 1
                if i_counter % n_batch == 0:
                    cur_mongo_to.remove({"_id": {'$in': mylist_id}})
                    cur_mongo_to.insert(mylist)
                    mylist = []
                    mylist_id = []
                    print("\r{0} Increment sync Table:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                          .format(get_time(),tab, n_totals, i_counter,
                                  round(i_counter / n_totals * 100, 2),
                                  str(get_seconds(start_time))), end='')

            cur_mongo_to.remove({"_id": {'$in': mylist_id}})
            cur_mongo_to.insert(mylist)
            print("\r{0} Increment sync Table:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                  .format(get_time(),tab, n_totals, i_counter,
                          round(i_counter / n_totals * 100, 2),
                          str(get_seconds(start_time))), end='')
            print('')
        else:
            if day != '':
               print('{0} Increment sync Table:{1} recent {2} {3} no found data,skip increment sync!'.
                      format(get_time(), tab, day,config['sync_time_type']))
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

    #read config
    config=init(config,debug)

    #full sync
    print("\033[1;32;40m{0} Full Sync data,please wait!\033[0m".format(get_time()))
    full_sync(config)

    #incr sync
    print("\033[1;32;40m{0} Increment Sync data,please wait!\033[0m".format(get_time()))
    increment_sync(config)

if __name__ == "__main__":
     main()

