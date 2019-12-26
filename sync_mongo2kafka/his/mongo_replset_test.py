#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/10/23 21:21
# @Author : 马飞
# @File : mongo_replset_test.py.py
# @Software: PyCharm

import pymongo

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

def main():
    mongo_url='39.106.184.57,39.96.30.168,39.96.14.108:27016,27017,27018:posB:root:JULc9GnEuNHYUTBG'
    replset_name='posb'
    db_mongo = get_ds_mongo_auth_replset(mongo_url, replset_name)
    print(db_mongo)

if __name__ == "__main__":
     main()


