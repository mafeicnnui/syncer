#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/30 9:31
# @Author  : 马飞
# @File    : sync_mysql2mongo.py
# @Software: PyCharm
# @Func    : MongoDB->MOngoDB_Replia_Set

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
from elasticsearch import Elasticsearch
from elasticsearch import helpers


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
    if user != '' and password != '':
        db.authenticate(user, password)
    return db

def get_ds_mongo_auth_replset(mongodb_str,replset_name):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    user          = mongodb_str.split(':')[3]
    password      = mongodb_str.split(':')[4]
    replstr       = ''
    for v in range(len(ip.split(','))):
        replstr=replstr+'{0}:{1},'.format(ip.split(',')[v],port.split(',')[v])
        print('replstr=',replstr)
    conn          = pymongo.MongoClient('mongodb://{0},replicaSet={1}'.format(replstr[0:-1],replset_name))
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

def get_ds_es_str(es_str):
    ip            = es_str.split(':')[0]
    port          = es_str.split(':')[1]
    return 'ElasticSearch:{0}:{1}'.format(ip,port)

'''
    功能：获取ES连接
'''
def get_ds_es(es_str):
    conn = Elasticsearch([es_str.split(':')[0]],port=int(es_str.split(':')[1]))
    return conn


'''
    功能：datatime对象增加8小时   
'''
def preprocessor(result):
    for key in result:
        if is_valid_datetime(result[key]):
            result[key] = (result[key] + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
        else:
            result[key]=str(result[key])
    #print('preprocessor=',result)
    return result

'''
    功能：判断对象是否为datetime类型  
'''
def is_valid_datetime(strdate):
    if isinstance(strdate,datetime.datetime):
        return True
    else:
        return False

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    config['sync_table']               = cfg.get("sync", "sync_table")
    config['batch_size']               = cfg.get("sync", "batch_size")
    config['sync_time_type']           = cfg.get("sync", "sync_time_type")
    config['db_mongo']                 = get_ds_mongo_auth(cfg.get("sync", "db_mongo"))
    config['db_mongo_db']              = cfg.get("sync", "db_mongo").split(':')[2]
    config['db_es']                    = get_ds_es(cfg.get("sync", "db_es"))
    config['db_mongo_str']             = get_ds_mongo_str(cfg.get("sync", "db_mongo"))
    config['db_es_str']                = get_ds_es_str(cfg.get("sync", "db_es"))
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
           print(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=', config[key])
        else:
           print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))


def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def init(config,debug):
    config = get_config(config)
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
    return v_json

'''
    功能：将datatime类型序列化json可识别类型
'''

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        else:
            return json.JSONEncoder.default(self, obj)


def full_sync(config):
    #process
    db_mongodb           = config['db_mongo']
    db_es                = config['db_es']
    for tabs in config['sync_table'].split(","):
        start_time       = datetime.datetime.now()
        n_batch          = int(config['batch_size'])
        tab              = tabs.split(':')[0]
        cur_mongo_from   = db_mongodb[tab]
        results          = cur_mongo_from.find()
        n_totals         = results.count()

        if n_totals > 0 :
           print('{0} Full sync table:{1},please wait...'.format(get_time(),tab))
           i_counter = 0
           mylist    = []
           for r in results:
               id=str(r['_id'])
               del r['_id']
               mylist.append({
                   "_index" : config['db_mongo_db'].lower(),
                   "_type"  : tab,
                   "_id"    : id,
                   "_source": r
               })
               i_counter = i_counter + 1
               if i_counter % n_batch == 0:
                   helpers.bulk(db_es, mylist)
                   mylist = []
                   print("\r{0} Full sync Table~:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                         .format(get_time(),tab, n_totals, i_counter,
                                 round(i_counter / n_totals * 100, 2),
                                 str(get_seconds(start_time))), end='')

           helpers.bulk(db_es, mylist)
           print("\r{0} Full sync Table:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                 .format(get_time(),tab, n_totals, i_counter,
                         round(i_counter / n_totals * 100, 2),
                         str(get_seconds(start_time))), end='')
           print('')


'''
 mongodb demo:
    db.t1.insert({'name':'zhangsan','create_time':ISODate('2020-07-02 17:20:20')})
    db.t2.insert({'name':'wangwu','create_time':ISODate('2020-07-01 17:20:20')})
'''
def start_sync(config):
    db_mongodb    = config['db_mongo']
    db_es         = config['db_es']
    for tabs in config['sync_table'].split(","):
        start_time     = datetime.datetime.now()
        n_batch        = int(config['batch_size'])
        tab            = tabs.split(':')[0]
        day            = tabs.split(':')[2]
        cur_mongo      = db_mongodb[tab]
        v_where        = get_mongo_incr_where(config,tabs)
        results        = cur_mongo.find(v_where)
        n_totals       = results.count()
        if n_totals > 0 :
            print('{0} Increment sync table:{1},please wait...'.format(get_time(),tab))
            i_counter = 0
            mylist    = []
            for r in results:
                r=preprocessor(r)
                id = str(r['_id'])
                del r['_id']
                mylist.append({
                    "_index": config['db_mongo_db'].lower() ,
                    "_type": tab,
                    "_id": id,
                    "_source": r
                })

                i_counter = i_counter + 1
                if i_counter % n_batch == 0:
                    helpers.bulk(db_es, mylist)
                    mylist = []
                    print("\r{0} Increment sync Table:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                          .format(get_time(),tab, n_totals, i_counter,
                                  round(i_counter / n_totals * 100, 2),
                                  str(get_seconds(start_time))), end='')

            helpers.bulk(db_es, mylist)
            print("\r{0} Increment sync Table:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                  .format(get_time(),tab, n_totals, i_counter,
                          round(i_counter / n_totals * 100, 2),
                          str(get_seconds(start_time))), end='')
            print('')
        else:
            if day != '':
               print('{0} Increment sync Table:{1} recent {2} {3} no found data,skip increment sync!'.
                      format(get_time(), tab, day,config['sync_time_type']))



'''
    功能：首次同步时在ES上创建索引
'''
def init_es(config):
    db_mongodb = config['db_mongo']
    db_name = config['db_mongo_db']
    es=config['db_es']
    d_mappings= {
        "mappings": {
            "properties": {
            }
        }
    }
    for e in config['sync_table'].split(","):
        tab = e.split(':')[0]
        cur_mongo = db_mongodb[tab]
        results = cur_mongo.find().limit(1)
        col={}
        for cur in results:
            del cur['_id']
            for key in cur:
                if key not in('_id'):
                    col.update({
                        key: {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    })
        #print('col=',json.dumps(col, cls=DateEncoder, ensure_ascii=False, indent=4, separators=(',', ':')) + '\n')
        d_mappings['mappings']['properties'].update({tab:col})
        print('>>>mappings=',
              json.dumps(d_mappings, cls=DateEncoder, ensure_ascii=False, indent=4, separators=(',', ':')) + '\n')

    print('mappings=',json.dumps(d_mappings, cls=DateEncoder, ensure_ascii=False, indent=4, separators=(',', ':')) + '\n')
    try:
      es.indices.create(index=db_name,body =d_mappings)
      print('ElasticSearch index {} created!'.format(db_name))
    except:
      print('{} index already exist'.format(db_name))





def main():
    # init variable
    config = ""
    debug = False
    warnings.filterwarnings("ignore")

    # get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    # read config
    config=init(config,debug)

    # init index
    init_es(config)

    # start sync
    print("\033[1;32;40m{0} Increment Sync data,please wait!\033[0m".format(get_time()))
    start_sync(config)

if __name__ == "__main__":
     main()

