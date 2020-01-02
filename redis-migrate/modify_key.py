#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/5/6 11:52
# @Author : 马飞
# @File : modify_key.py
# @Software: PyCharm

import redis

settings={
     "db_sour":{
                "192.168.1.1",
                "port":6379,
                "db":0,
                "password":'SAbcd123@^'
     },
     "db_dest":{
                "host":"192.168.1.2",
                "port":6379,
                "db":6,
                "password":'Ty5@58Wk#'
     },
     "key_match":"abc:*,xyz:*"
}

def get_config():
    cfg={}
    cfg['db_sour']   = redis.Redis(host=settings['db_sour']['host'],
                                   port=settings['db_sour']['port'],
                                   password=settings['db_sour']['password'],
                                   db=settings['db_sour']['db'])

    cfg['db_dest']   = redis.Redis(host=settings['db_dest']['host'],
                                   port=settings['db_dest']['port'],
                                   password=settings['db_dest']['password'],
                                   db=settings['db_dest']['db'])
    cfg['key_match'] = settings['key_match']
    return cfg

def get_keys(cfg):
    db=cfg['db_sour']
    key=[]
    for i in cfg['key_match'].split(','):
        tmp = db.keys(i)
        for j in tmp:
            key.append(j)
    return key

def main():

    cfg   = get_config()
    keys  = get_keys(cfg)
    print('cfg=',cfg)
    print('keys=',keys)

    keys.sort()
    for key in keys:
        d=cfg['db_sour'].dump(key)
        print(key,d)
        cfg['db_dest'].delete(key)
        cfg['db_dest'].restore(key,0,d)

if __name__ == "__main__":
     main()
