#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/29 9:06
# @Author : 马飞
# @File : mysql2kafka_sync.py.py
# @Software: PyCharm

import json,datetime,time,sys
import pymysql
from kafka  import KafkaProducer
from kafka.errors import KafkaError
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent)

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
            parmas_message = json.dumps(params,cls=DateEncoder)
            producer = self.producer
            producer.send(self.kafkatopic, parmas_message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print(str(e))

class DateEncoder(json.JSONEncoder):
    '''
      自定义类，解决报错：
      TypeError: Object of type 'datetime' is not JSON serializable
    '''
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        else:
            return json.JSONEncoder.default(self, obj)

def get_event_name(event):
    if event==2:
       return 'QueryEvent'.ljust(20,' ')+':'
    elif event==30:
       return 'WriteRowsEvent'.ljust(20,' ')+':'
    elif event==31:
       return 'UpdateRowsEvent'.ljust(20,' ')+':'
    elif event==32:
       return 'DeleteRowsEvent'.ljust(20,' ')+':'
    else:
       return ''.ljust(30,' ')

MYSQL_SETTINGS = {
    "host"  : "10.2.39.18",
    "port"  : 3307,
    "user"  : "puppet",
    "passwd": "Puppet@123",
    "db"    : "test"
}

KAFKA_SETTINGS = {
    "host"  : "10.2.39.18",
    "port"  :  9092,
    "topic" : 'mysql2kafka'
}

def get_master_pos(file=None,pos=None):
    db = get_db(MYSQL_SETTINGS)
    cr = db.cursor()
    cr.execute('show master status')
    rs=cr.fetchone()
    if file is not None and pos is not None:
        return file,pos
    else:
        return rs[0],rs[1]

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',autocommit=True)
    return conn

def get_db(config):
    return get_ds_mysql(config['host'],config['port'],config['db'],config['user'],config['passwd'])


def main():
    while True:
        try:
            file,pos=get_master_pos()
            stream = BinLogStreamReader( connection_settings = MYSQL_SETTINGS,
                                         server_id     = 8,
                                         blocking      = True,
                                         resume_stream = True,
                                         log_file = file,
                                         log_pos  = int(pos)
                                        )

            producer = Kafka_producer(KAFKA_SETTINGS['host'], KAFKA_SETTINGS['port'], KAFKA_SETTINGS['topic'])

            schema = MYSQL_SETTINGS['db']

            for binlogevent in stream:

                if binlogevent.event_type in (2,):
                    event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                    #print('event1=>',event)
                    if 'create' in event['query'] or 'drop' in event['query'] \
                            or 'alter' in event['query'] or 'truncate' in event['query']:

                        if event['schema'] == schema:
                            #print('event2=>',get_event_name(binlogevent.event_type), event)
                            #print(binlogevent.query.lower())
                            producer.sendjsondata(event)


                if binlogevent.event_type in (30, 31, 32):
                    for row in binlogevent.rows:
                        event = {"schema": binlogevent.schema, "table": binlogevent.table}
                        if event['schema'] == schema:
                            if isinstance(binlogevent, DeleteRowsEvent):
                                event["action"] = "delete"
                                event["data"] = row["values"]
                                #print('delete=',event)
                                producer.sendjsondata(event)
                            elif isinstance(binlogevent, UpdateRowsEvent):
                                event["action"] = "update"
                                event["after_values"] = row["after_values"]
                                event["before_values"] = row["before_values"]
                                #print('update=',event)
                                producer.sendjsondata(event)
                            elif isinstance(binlogevent, WriteRowsEvent):
                                event["action"] = "insert"
                                event["data"] = row["values"]
                                #print('insert=', event)
                                producer.sendjsondata(event)


        except Exception as e:
            print('Exception=',str(e))


if __name__ == "__main__":
    main()