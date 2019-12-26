#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/12/14 11:02
# @Author : 马飞
# @File : sync_mongo_oplog.py
# @Software: PyCharm

import pymongo
import time

def get_ds_mongo_auth_replset(mongodb_str,replset_name):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    #user          = mongodb_str.split(':')[3]
    #password      = mongodb_str.split(':')[4]
    replstr       = ''
    for v in range(len(ip.split(','))):
        replstr=replstr+'{0}:{1},'.format(ip.split(',')[v],port.split(',')[v])
        print('replstr=',replstr)
    conn          = pymongo.MongoClient('mongodb://{0},replicaSet={1}'.format(replstr[0:-1],replset_name))
    db            = conn[service]
    #db.authenticate(user, password)
    return db


def main():
   #同步库和表
   sync_db  = 'test'
   sync_tab = 'xs'

   #连接复本集，获取日志
   db = get_ds_mongo_auth_replset('10.2.39.171,10.2.39.170,10.2.39.169:27016,27016,27016:local:root:root','hopsondemo')
   cr = db['oplog.rs']

   #初始获取表的所有日志
   print('db=',db)
   rs=cr.find({"ns":"test.xs"})
   for i in rs:
       print('full=',i)
       ts=i['ts']


   #获取增量日志
   print('ts=',ts)
   while True:
       rs = cr.find({"$and":[{"ts": {"$gt": ts}},{"ns":"test.xs"}]})
       for i in rs:
           print('incr=',i)
           ts = i['ts']
       time.sleep(1)
       print('sleep 3s')

if __name__ == "__main__":
     main()

