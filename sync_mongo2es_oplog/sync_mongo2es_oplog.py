#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/12/14 11:02
# @Author : 马飞
# @File : sync_mongo_oplog.py
# @Software: PyCharm

import pymongo
import json
import datetime
import sys
import traceback
from pymongo.cursor import CursorType
from elasticsearch import Elasticsearch
from elasticsearch import helpers


''' ElasticSearch 查询

    1、查询索引下的类型
    curl 10.2.39.129:9200/test?pretty

    2、查询某个文档
    curl '172.17.194.79:9200/test/saleDetail/_search?pretty' -d '
    {
      "query": {
        "bool": {
          "must": [
            {"match":{"_id":"5efc45fa2eec550e40eef153"}}
          ]
        }
      }
    }'
    
    3、查询文档数量
    curl 172.17.194.79:9200/_cat/indices?v
    
    4、查询同步日志
    tail -f sync_mongo2es_oplog.log
'''


'''
    功能：Mongo 单实例连接配置
'''
MONGO_SETTINGS = {
    "host"     : '10.2.39.130',
    "port"     : '27017',
    "db"       : 'local',
    "user"     : '',
    "passwd"   : '',
    "table"    : 'test.t1,test.t2',  #db:table
    "isSync"   :  True,
    "logfile"  :  "sync_mongo2es_oplog.log",
    "debug"    :  True
}

'''
    功能：ElasticSearch连接配置
'''
ES_SETTINGS = {
    "host"  : '10.2.39.130',
    "port"  :  9200,
}

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


'''
    功能：mongo 单实例认证
    入口：mongo 连接串，格式：IP:PORT:DB:USER:PASSWD
    出口：mongo 数据库连接对象
'''
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
        db = conn['local']
    return db

'''
    功能：mongo 副本集认证
    入口：mongo 连接串，格式：IP1,IP2,IP3:PORT1,PORT2,PORT3:DB:USER:PASSWD
    出口：mongo 数据库连接对象
'''
def get_ds_mongo_auth_replset(mongodb_str,replset_name):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    user          = mongodb_str.split(':')[3]
    password      = mongodb_str.split(':')[4]
    replstr       = ''

    for v in range(len(ip.split(','))):
        replstr=replstr+'{0}:{1},'.format(ip.split(',')[v],port.split(',')[v])
    conn          = pymongo.MongoClient('mongodb://{0},replicaSet={1}'.format(replstr[0:-1],replset_name))
    db            = conn[service]

    if user!='' and password !='' :
       db.authenticate(user, password)
       db = conn['local']
    return db

'''
    功能：获取ES连接
'''
def get_ds_es(p_ip,p_port):
    conn = Elasticsearch([p_ip],port=p_port)
    return conn


'''
    功能：格式化输出字典对象   
'''
def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

'''
    功能：datatime对象增加8小时   
'''
def preprocessor(result):
    for key in result['o']:
        if is_valid_datetime(result['o'][key]):
            result['o'][key] = result['o'][key] + datetime.timedelta(hours=8)
    return result

'''
    功能：判断对象是否为datetime类型  
'''
def is_valid_datetime(strdate):
    if isinstance(strdate,datetime.datetime):
        return True
    else:
        return False

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

'''
    功能：写同步日志 
'''
def write_log(doc):
    try:
        if doc['op'] =='i' or doc['op'] =='d' :
           doc['o']['_id'] = str(doc['o']['_id'])

        if doc['op']=='u':
           doc['o']['_id'] = str(doc['o']['_id'])
           doc['o2']['_id'] = str(doc['o2']['_id'])

    except:
        print('write_log error:',traceback.print_exc())

    doc=preprocessor(doc)

    with open(MONGO_SETTINGS['logfile'], 'a') as f:
        f.write('Sync time:{},ElasticSearch:index:{},type:{},op:{}\n'.format(get_time(),doc['ns'].split('.')[0],doc['ns'].split('.')[1],doc['op']))
        f.write('_source='+json.dumps(doc, cls=DateEncoder,ensure_ascii=False, indent=4, separators=(',', ':'))+'\n')

    if MONGO_SETTINGS['debug']:
        print('Sync time:{},ElasticSearch:index:{},type:{},op:{}'
              .format(get_time(), doc['ns'].split('.')[0],doc['ns'].split('.')[1], doc['op']))
        print('_source=' + json.dumps(doc, cls=DateEncoder, ensure_ascii=False, indent=4, separators=(',', ':')) + '\n')


'''
    功能：同步所有库数据
'''
def full_sync(config):
    cr = config['mongo']['oplog.rs']
    first = next(cr.find().sort('$natural', pymongo.DESCENDING).limit(-1))
    ts = first['ts']
    actions = []
    while True:
        cursor = cr.find({'ts': {'$gt': ts}}, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True)
        while cursor.alive:
            for doc in cursor:
                doc['ts'] = str(doc['ts'])
                v_json = {}
                if MONGO_SETTINGS['table'] == '' or MONGO_SETTINGS['table'] is None:
                    if doc['op'] == 'i':
                        v_json['op'] = 'insert'
                        v_json['obj'] = doc['ns']
                        v_json['val'] = doc['o']
                        v_json['val']['_id'] = str(v_json['val']['_id'])
                        es_doc = {
                            "_index": doc['ns'].split('.')[0],
                            "_type" : doc['ns'].split('.')[1],
                            "_id"   : v_json['val']['_id'],
                            "_source": doc
                        }
                        actions.append(es_doc)
                        helpers.bulk(config['es'], actions)
                        write_log(doc)
                    elif doc['op'] == 'd':
                        v_json['op'] = 'delete'
                        v_json['obj'] = doc['ns']
                        v_json['val'] = str(doc['o']['_id'])
                        es_doc = {
                            "_index": doc['ns'].split('.')[0],
                            "_type": doc['ns'].split('.')[1],
                            "_id": v_json['val'],
                            "_source": doc
                        }
                        actions.append(es_doc)
                        helpers.bulk(config['es'], actions)
                        write_log(doc)
                    elif doc['op'] == 'u':
                        v_json['op'] = 'update'
                        v_json['obj'] = doc['ns']
                        if doc['o'].get('$set', None) is not None:
                            v_json['val'] = doc['o']['$set']
                        else:
                            v_json['val'] = doc['o']
                            v_json['val']['_id'] = str(v_json['val']['_id'])
                        v_json['where'] = doc['o2']
                        v_json['where']['_id'] = str(v_json['where']['_id'])

                        es_doc = {
                            "_index": doc['ns'].split('.')[0],
                            "_type": doc['ns'].split('.')[1],
                            "_id": v_json['val']['_id'],
                            "_source": doc
                        }
                        actions.append(es_doc)
                        helpers.bulk(config['es'], actions)
                        write_log(doc)
                    else:
                        pass

                    actions = []

'''
    功能：同步某个库下某张表数据
'''
def incr_sync(config):
    cr = config['mongo']['oplog.rs']
    first = next(cr.find().sort('$natural', pymongo.DESCENDING).limit(-1))
    ts = first['ts']
    actions =[]
    while True:
        cursor = cr.find({'ts': {'$gt': ts}}, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True)
        while cursor.alive:
            for doc in cursor:
                doc['ts'] = str(doc['ts'])
                v_json = {}
                if len([i for i in MONGO_SETTINGS['table'].split(',') if i in doc['ns']])>0:
                    if doc['op'] == 'i':
                        v_json['op'] = 'insert'
                        v_json['obj'] = doc['ns']
                        v_json['val'] = doc['o']
                        v_json['val']['_id'] = str(v_json['val']['_id'])
                        es_doc = {
                            "_index" : doc['ns'].split('.')[0],
                            "_type"  : doc['ns'].split('.')[1],
                            "_id"    : v_json['val']['_id'],
                            "_source": doc
                        }
                        actions.append(es_doc)
                        helpers.bulk(config['es'], actions)
                        write_log(doc)
                    elif doc['op'] == 'd':
                        v_json['op']  = 'delete'
                        v_json['obj'] = doc['ns']
                        v_json['val'] = str(doc['o']['_id'])
                        es_doc = {
                            "_index": doc['ns'].split('.')[0],
                            "_type": doc['ns'].split('.')[1],
                            "_id": v_json['val'],
                            "_source": doc
                        }
                        actions.append(es_doc)
                        helpers.bulk(config['es'], actions)
                        write_log(es_doc)
                    elif doc['op'] == 'u':
                        v_json['op'] = 'update'
                        v_json['obj'] = doc['ns']
                        if doc['o'].get('$set', None) is not None:
                            v_json['val'] = doc['o']['$set']
                        else:
                            v_json['val'] = doc['o']
                            v_json['val']['_id'] = str(v_json['val']['_id'])
                        v_json['where'] = doc['o2']
                        v_json['where']['_id'] = str(v_json['where']['_id'])

                        es_doc = {
                            "_index": doc['ns'].split('.')[0],
                            "_type": doc['ns'].split('.')[1],
                            "_id": v_json['val']['_id'],
                            "_source": doc
                        }
                        actions.append(es_doc)
                        helpers.bulk(config['es'], actions)
                        write_log(es_doc)
                    else:
                        pass
                    actions = []

'''
    功能：首次同步时在ES上创建索引
'''
def init_es(config):
    es=config['es']
    for e in MONGO_SETTINGS['table'].split(','):
        es.indices.create(index=e.split('.')[0], ignore=400)
        print('ElasticSearch index {} created!'.format(e.split('.')[0]))

'''
    功能：获取mongo,es连接对象
'''
def get_config():
    config={}
    # 获取mongo数据库对象
    if MONGO_SETTINGS.get('replset') is None or MONGO_SETTINGS.get('replset') == '':
        mongo = get_ds_mongo_auth('{0}:{1}:{2}:{3}:{4}'.
                                  format(MONGO_SETTINGS['host'],
                                         MONGO_SETTINGS['port'],
                                         MONGO_SETTINGS['db'],
                                         MONGO_SETTINGS['user'],
                                         MONGO_SETTINGS['passwd']))
    else:
        mongo = get_ds_mongo_auth_replset('{0}:{1}:{2}:{3}:{4}'.
                                  format(MONGO_SETTINGS['host'],
                                         MONGO_SETTINGS['port'],
                                         MONGO_SETTINGS['db'],
                                         MONGO_SETTINGS['user'],
                                         MONGO_SETTINGS['passwd']),
                                         MONGO_SETTINGS['replset'])
    config['mongo'] = mongo
    MONGO_SETTINGS['mongo'] = mongo

    # 获取ES对象
    es = get_ds_es(ES_SETTINGS['host'], ES_SETTINGS['port'])
    config['es']=es
    ES_SETTINGS['es'] = es
    return config


'''
    功能：打印配置
'''
def print_cfg():
    print('Mongodb config:')
    print_dict(MONGO_SETTINGS)
    print('\n')
    print('ElasticSearch config:')
    print_dict(ES_SETTINGS)


'''
    功能：主函数   
'''
def main():

   if not MONGO_SETTINGS['isSync']:
      print('Sync is disabled!' )
      sys.exit(0)

   # 获取对象
   config = get_config()

   # 输出配置
   print_cfg()

   # 初始化ES
   init_es(config)

   # 数据同步
   if MONGO_SETTINGS['table'] == '' or MONGO_SETTINGS['table'] is None:
       full_sync(config)
   else:
       incr_sync(config)


if __name__ == "__main__":
     main()

