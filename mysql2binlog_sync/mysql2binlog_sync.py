#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/29 9:06
# @Author : 马飞
# @File : mysql2kafka_sync.py.py
# @Software: PyCharm

import json,datetime,time,sys
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
     DeleteRowsEvent,
     UpdateRowsEvent,
     WriteRowsEvent
)

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

MYSQL_SYNC_SETTINGS = {
    "host"  : "10.2.39.18",
    "port"  : 3307,
    "user"  : "puppet",
    "passwd": "Puppet@123",
    "db"    : "test_repl"
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

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_ins_header(event):
    v_ddl = 'insert into {0}.{1} ('.format(MYSQL_SYNC_SETTINGS['db'],event['table'])
    for key in event['data']:
        v_ddl = v_ddl + '`{0}`'.format(key) + ','
    v_ddl = v_ddl[0:-1] + ')'
    return v_ddl

def get_ins_values(event):
    v_tmp=''
    for key in event['data']:
        if event['data'][key]==None:
           v_tmp=v_tmp+"null,"
        else:
           v_tmp = v_tmp + "'" + format_sql(str(event['data'][key])) + "',"
    return v_tmp[0:-1]

def get_where(p_where):
    v_where = ' where '
    for key in p_where:
        v_where = v_where+ key+' = '+str(p_where[key]) + ' and '
    return v_where[0:-5]


def set_column(p_data):
    v_set = ' set '
    for key in p_data:
        v_set = v_set + key + '=' + str(p_data[key]) + ','
    return v_set[0:-1]


def gen_sql(event):
    if event['action']=='insert':
        sql = get_ins_header(event)+ ' values ('+get_ins_values(event)+')'
    elif event['action']=='update':
        sql = 'update {0}.{1} {2} {3}'.\
             format(MYSQL_SYNC_SETTINGS['db'],event['table'],set_column(event['after_values']),get_where(event['before_values']))
    elif event['action']=='delete':
        sql = 'delete from {0}.{1} {2}'.\
             format(MYSQL_SYNC_SETTINGS['db'],event['table'],get_where(event['data']))
    else:
       pass
    return sql

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

            schema = MYSQL_SETTINGS['db']
            db     = get_db(MYSQL_SYNC_SETTINGS)
            cr     = db.cursor()

            for binlogevent in stream:

                if binlogevent.event_type in (2,):
                    event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                    #print('event1=>',event)
                    if 'create' in event['query'] or 'drop' in event['query'] \
                            or 'alter' in event['query'] or 'truncate' in event['query']:

                        if event['schema'] == schema:
                            #print('event2=>',get_event_name(binlogevent.event_type), event)
                            print(binlogevent.query.lower())
                            cr.execute(binlogevent.query.lower())


                if binlogevent.event_type in (30, 31, 32):

                    for row in binlogevent.rows:

                        event = {"schema": binlogevent.schema, "table": binlogevent.table}

                        if event['schema'] == schema:

                            if isinstance(binlogevent, DeleteRowsEvent):
                                event["action"] = "delete"
                                event["data"] = row["values"]
                                print('delete=', gen_sql(event))
                                cr.execute(gen_sql(event))
                            elif isinstance(binlogevent, UpdateRowsEvent):
                                event["action"] = "update"
                                event["after_values"] = row["after_values"]
                                event["before_values"] = row["before_values"]
                                print('update=', gen_sql(event))
                                cr.execute(gen_sql(event))
                            elif isinstance(binlogevent, WriteRowsEvent):
                                event["action"] = "insert"
                                event["data"] = row["values"]
                                print('insert=', gen_sql(event))
                                cr.execute(gen_sql(event))
                            #print(get_event_name(binlogevent.event_type),json.dumps(event, cls=DateEncoder))
                            #print(json.dumps(event))

        except Exception as e:
            print(str(e))
        finally:
            cr.close()
            stream.close()


if __name__ == "__main__":
    main()