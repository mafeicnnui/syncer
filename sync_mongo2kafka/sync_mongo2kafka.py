#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/12/14 11:02
# @Author : 马飞
# @File : sync_mongo_oplog.py
# @Software: PyCharm

import pymongo
import json
import datetime
from pymongo.cursor import CursorType
from kafka  import KafkaProducer
from kafka.errors import KafkaError

# 演示环境 MongoDB 副本集
# MONGO_SETTINGS = {
#     "host"     : '10.2.39.171,10.2.39.170,10.2.39.169',
#     "port"     : '27016,27016,27016',
#     "db"       : 'local',
#     "user"     : '',
#     "passwd"   : '',
#     "replset"  : 'hopsondemo'
# }
#
# KAFKA_SETTINGS = {
#     "host"  : '10.2.39.18',
#     "port"  :  9092,
#     "topic" : 'sync_mongo2kafka'
# }

# Mongo 副本集连接配置
# MONGO_SETTINGS = {
#     "host"     : '172.17.194.79,172.17.129.195,172.17.129.194',
#     "port"     : '27016,27017,27018',
#     "db"       : 'admin',
#     "user"     : 'root',
#     "passwd"   : 'YxBfV0Q3ne6z6rny',
#     "replset"  : 'posb',
#     "db_name"  : 'posB',
#     "tab_name" : 'saleDetail',
# }


'''
    功能：Mongo 单实例连接配置
'''
MONGO_SETTINGS = {
    "host"     : 'dds-2ze8179ceab498d41.mongodb.rds.aliyuncs.com',
    "port"     : '3717',
    "db"       : 'admin',
    "user"     : 'root',
    "passwd"   : 'Mongo-kkm!2019',
    "replset"  : '',
    "db_name"  : 'posB',
    "tab_name" : 'saleDetail'
}

'''
    功能：Kafka连接配置
'''
KAFKA_SETTINGS = {
    "host"  : '172.17.194.79',
    "port"  :  9092,
    "topic" : 'sync_mongo2kafka_hopson'
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
    功能：kafka 生产对象类   
'''
class Kafka_producer():
    '''
    使用kafka的生产模块
    '''
    def __init__(self, kafkahost, kafkaport, kafkatopic):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.producer = KafkaProducer(bootstrap_servers='{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafkaHost,
            kafka_port=self.kafkaPort
        ))

    def sendjsondata(self, params):
        try:
            parmas_message = json.dumps(params, cls=DateEncoder)
            producer = self.producer
            producer.send(self.kafkatopic, parmas_message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print(str(e))

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
    功能：主函数   
'''
def main():

   #获取kafka生产者对象
   producer = Kafka_producer(KAFKA_SETTINGS['host'], KAFKA_SETTINGS['port'], KAFKA_SETTINGS['topic'])

   #获取mongo数据库对象
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
   #输出配置
   print_dict(MONGO_SETTINGS)

   #获取mongo游标对象
   cr       = mongo['oplog.rs']
   first    = next(cr.find().sort('$natural', pymongo.DESCENDING).limit(-1))
   ts       = first['ts']

   #以自然排序方式遍历oplog日志，处理oplog日志类型(op:i,u,d)
   while True:
       cursor = cr.find({'ts': {'$gt': ts}}, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True)
       while cursor.alive:
           for doc in cursor:
               ts   = doc['ts']
               v_json = {}
               if MONGO_SETTINGS['db_name'] == '' or MONGO_SETTINGS['db_name'] is None:
                   if doc['op'] == 'i':
                       v_json['op'] = 'insert'
                       v_json['obj'] = doc['ns']
                       v_json['val'] = doc['o']
                       v_json['val']['_id'] = str(v_json['val']['_id'])
                       producer.sendjsondata(v_json)
                   elif doc['op'] == 'd':
                       v_json['op'] = 'delete'
                       v_json['obj'] = doc['ns']
                       v_json['val'] = str(doc['o']['_id'])
                       producer.sendjsondata(v_json)
                   elif doc['op'] == 'u':
                       v_json['op'] = 'update'
                       v_json['obj'] = doc['ns']
                       v_json['val'] = doc['o']['$set']
                       v_json['where'] = doc['o2']
                       v_json['where']['_id'] = str(v_json['where']['_id'])
                       producer.sendjsondata(v_json)
                   elif doc['op'] == 'c':
                       print('v_json=', v_json)
                   else:
                       print('v_json=', v_json)

               else:
                   if "{0}.{1}".format(MONGO_SETTINGS['db_name'],MONGO_SETTINGS['tab_name']) in doc['ns']:
                        print('doc=',doc)
                        if doc['op'] == 'i':
                           v_json['op']  = 'insert'
                           v_json['obj'] = doc['ns']
                           v_json['val'] = doc['o']
                           v_json['val']['_id'] = str(v_json['val']['_id'])
                           producer.sendjsondata(v_json)
                        elif  doc['op'] == 'd':
                           v_json['op']  = 'delete'
                           v_json['obj'] = doc['ns']
                           v_json['val'] = str(doc['o']['_id'])
                           producer.sendjsondata(v_json)
                        elif doc['op'] == 'u':
                           v_json['op']  = 'update'
                           v_json['obj'] = doc['ns']
                           if doc['o'].get('$set',None) is not None :
                               v_json['val'] = doc['o']['$set']
                           else:
                               v_json['val'] = doc['o']
                               v_json['val']['_id'] = str(v_json['val']['_id'])
                           v_json['where'] = doc['o2']
                           v_json['where']['_id'] = str(v_json['where']['_id'])
                           producer.sendjsondata(v_json)
                        elif doc['op'] == 'c':
                            print('v_json=', v_json)
                        else:
                            print('v_json=', v_json)

if __name__ == "__main__":
     main()

