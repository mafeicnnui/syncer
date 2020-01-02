#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Software: PyCharm
# @flunc   :多线程程同步

import sys,time
import traceback
import configparser
import warnings
import pymongo
from bson.objectid import ObjectId
import json
import datetime
import smtplib
from   email.mime.text import MIMEText
from   dateutil import parser
import threading

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
    config['sync_thread_number']       = cfg.get("sync", "sync_thread_number")

    #get mongodb db
    config['db_mongo_from']            = get_ds_mongo_auth(cfg.get("sync", "db_mongo_from"))
    config['db_mongo_to']              = get_ds_mongo_auth(cfg.get("sync", "db_mongo_to"))
    config['db_mongo_from_str']        = get_ds_mongo_str(cfg.get("sync", "db_mongo_from"))
    config['db_mongo_to_str']          = get_ds_mongo_str(cfg.get("sync", "db_mongo_to"))
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


def print_dict_threads(config):
    v=''
    print('\033[1;32;40mTime:{0}\033[0m'.format(get_time()))
    print('-'.ljust(125, '-'))
    print(' '.ljust(3, ' ') + 'thread_id'.ljust(12, ' ')+ 'status'.ljust(14, ' ')+'message'.ljust(150, ' '))

    print('-'.ljust(125, '-'))
    for key in config:
      if config[key]['status']=='running':
         v=v+' '.ljust(3, ' ') \
            +config[key]['id'].ljust(12,' ')\
            +config[key]['status'].ljust(14,' ')\
            +config[key]['msg']+'\n'
    print("\r{0}\n".format(v))


def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def init(config,debug):
    config = get_config(config)
    #print dict
    if debug:
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

def full_sync(config,tabs,threads_env,thread_id):
    threads_env[thread_id]['status'] = 'running'
    #process
    db_mongodb_from      = config['db_mongo_from']
    db_mongodb_to        = config['db_mongo_to']
    config_init          = {}
    start_time           = datetime.datetime.now()
    n_batch              = int(config['batch_size'])
    tab                  = tabs.split(':')[0]
    config_init[tab]     = False
    cur_mongo_from       = db_mongodb_from[tab]
    cur_mongo_to         = db_mongodb_to[tab]
    results              = cur_mongo_from.find()
    n_totals             = results.count()
    threads_env[thread_id]['msg']=thread_id
    if n_totals > 0 :
       if not check_mongo_tab_exists(db_mongodb_to,tab):
           threads_env[thread_id]['msg'] = 'Full sync table:{0},please wait...'.format(tab)
           config_init[tab] = True
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
                   threads_env[thread_id]['msg'] ="Full sync Table:{0},Total :{1},Process :{2},Complete:{3}%,elapse:{4}s".\
                              format(tab, n_totals, i_counter,round(i_counter / n_totals * 100, 2),str(get_seconds(start_time)))
           cur_mongo_to.remove({"_id": {'$in': mylist_id}})
           cur_mongo_to.insert(mylist)
           threads_env[thread_id]['msg'] = "Full sync Table:{0},Total :{1},Process :{2},Complete:{3}%,elapse:{4}s".\
                       format(tab, n_totals, i_counter,round(i_counter / n_totals * 100, 2),str(get_seconds(start_time)))
       else:
          threads_env[thread_id]['msg'] = 'Full sync Table {0} already exists,skip full sync!'.format(tab)
    else:
       db_mongodb_to.drop_collection(tab)
       db_mongodb_to.create_collection(tab)
       threads_env[thread_id]['msg'] ='Table {0} sync 0 records!'.format(tab)
    threads_env[thread_id]['status'] = 'stopped'

def increment_sync(config,tabs,threads_env,thread_id):
    threads_env[thread_id]['status'] = 'running'
    #process
    db_mongodb_from    = config['db_mongo_from']
    db_mongodb_to      = config['db_mongo_to']
    start_time         = datetime.datetime.now()
    n_batch            = int(config['batch_size'])
    tab                = tabs.split(':')[0]
    day                = tabs.split(':')[2]
    cur_mongo_from     = db_mongodb_from[tab]
    cur_mongo_to       = db_mongodb_to[tab]
    v_where            = get_mongo_incr_where(config,tabs)
    results            = cur_mongo_from.find(v_where)
    n_totals           = results.count()

    if n_totals > 0  :
        threads_env[thread_id]['msg']='Increment sync table:{0},please wait...'.format(tab)
        i_counter = 0
        mylist = []
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
                threads_env[thread_id]['msg']="Increment sync Table:{0},Total :{1},Process :{2},Complete:{3}%,elapse:{4}s".\
                            format(tab, n_totals, i_counter,round(i_counter / n_totals * 100, 2),str(get_seconds(start_time)))
        cur_mongo_to.remove({"_id": {'$in': mylist_id}})
        cur_mongo_to.insert(mylist)
        threads_env[thread_id]['msg'] ="Increment sync Table:{0},Total :{1},Process :{2},Complete:{3}%,elapse:{4}s".\
                    format(tab, n_totals, i_counter,round(i_counter / n_totals * 100, 2),str(get_seconds(start_time)))
    else:
        if day != '':
           threads_env[thread_id]['msg'] ='Increment sync Table:{0} recent {1} {2} no found data,skip increment sync!'.\
                      format(tab, day, config['sync_time_type'])
        else:
           threads_env[thread_id]['msg'] = 'Increment sync Table:{0}  no found data,skip increment sync!'.format(tab)
    threads_env[thread_id]['status'] = 'stopped'

def monitor_thread(config):
    print('print thread running status...')
    time.sleep(1)
    while True:
       i_counter = 0
       for key in list(config.keys()):
           if config[key]['status'] == 'completed':
              i_counter=i_counter+1

           if config[key]['status']=='running':
               print_dict_threads(config)
               time.sleep(10)
               break

       if i_counter==len(config):
          print('all thread end,exit!')
          break

def sync_multi_thread_full(config):
    #init threads variables
    threads_env = {}
    threads = []
    i_thread_counter = 0

    #add full sync to thread
    for tabs in config['sync_table'].split(","):
        tab = tabs.split(':')[0]
        thread_id = 'thread' + str(i_thread_counter)
        threads_env[thread_id] = {'id': thread_id, 'name': 'full_sync_' + tab, 'status': 'init', 'msg': ''}
        thread = threading.Thread(target=full_sync, args=(config, tabs, threads_env, thread_id,))
        threads.append(thread)
        i_thread_counter = i_thread_counter + 1

    #for i in range(len(threads)):
    #    print(threads[i],threads_env['thread'+str(i)])

    #set thread number
    if int(config['sync_thread_number'])> len(config['sync_table'].split(",")):
        config['sync_thread_number']=len(config['sync_table'].split(","))

    #monitor thread
    if int(config['sync_thread_number'])!=0:
        t = threading.Thread(target=monitor_thread, args=(threads_env,))
        t.start()

    for i in range(0, int(config['sync_thread_number'])):
        threads[i].start()

    #runnint others threads,max three
    i = int(config['sync_thread_number'])
    if i!=0:
        while True:
            for key in list(threads_env.keys()):
                if threads_env[key]['status'] == 'stopped':
                   threads_env[key]['status'] = 'completed'
                   if i != i_thread_counter:
                        threads[i].start()
                        i = i + 1
                        break
            time.sleep(1)
            if i == i_thread_counter:
                break

        for i in range(0, i_thread_counter):
            threads[i].join()
        print('all Done at :', get_time())

    #print('print threads_env...')
    #print_dict(threads_env)


def sync_multi_thread_incr(config):
    #init threads variables
    threads_env = {}
    threads = []
    i_threads_running = 0
    i_thread_counter  = 0

    #add incr sync to thread
    for tabs in config['sync_table'].split(","):
        tab = tabs.split(':')[0]
        thread_id = 'thread' + str(i_thread_counter)
        threads_env[thread_id] = {'id':thread_id,'name':'incr_sync_'+tab,'status': 'init','msg':''}
        thread = threading.Thread(target=increment_sync, args=(config, tabs,threads_env,thread_id,))
        i_thread_counter = i_thread_counter + 1
        threads.append(thread)

    #for i in range(len(threads)):
    #    print(threads[i],threads_env['thread'+str(i)])

    #set thread number
    if int(config['sync_thread_number']) > len(config['sync_table'].split(",")):
        config['sync_thread_number'] = len(config['sync_table'].split(","))

    #monitor thread
    if int(config['sync_thread_number']) != 0:
        t = threading.Thread(target=monitor_thread, args=(threads_env,))
        t.start()

    #run thread for sync_thread_number
    for i in range(0, int(config['sync_thread_number'])):
        threads[i].start()
        i_threads_running=i_threads_running+1

    #runnint others threads,max three
    i = int(config['sync_thread_number'])
    if i != 0:
        while True:
            for key in list(threads_env.keys()):
                if threads_env[key]['status'] == 'stopped':
                    threads_env[key]['status'] = 'completed'
                    if i != i_thread_counter:
                        threads[i].start()
                        i = i + 1
                        break
            time.sleep(1)
            if i == i_thread_counter:
                break

        for i in range(0, i_thread_counter):
            threads[i].join()
        print('all Done at :', get_time())

    #print('print threads_env...')
    #print_dict(threads_env)

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

    #sync_multi_thread_full
    sync_multi_thread_full(config)

    #sync_multi_thread_incr
    sync_multi_thread_incr(config)

if __name__ == "__main__":
     main()

