#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/5/6 11:52
# @Author : 马飞
# @File : modify_key.py
# @Software: PyCharm
import redis
from redis import StrictRedis
import json
import re

def print_dict(r,config):
    print('-'.ljust(150,'-'))
    print(' '.ljust(3,' ')+"name".ljust(60,' ')+"type".ljust(10,' ')+'value'.ljust(80,' '))
    print('-'.ljust(150,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(60,' ')+r.type(key).decode('UTF-8').ljust(10,' '),config[key].ljust(80,' '))
    print('-'.ljust(150,'-'))
    print('合计:{0}'.format(str(len(config))))

def main2():
    i_counter       = 0
    redis_db_sour   = redis.Redis(host='192.168.1.101', port=6379,db=0)
    redis_db_dest   = redis.Redis(host='192.168.1.102', port=6379, db=0)
    keys            = redis_db_sour.keys()
    keys.sort()
    for key in keys:
        v_key = key.decode('UTF-8')
        v_typ = redis_db_sour.type(v_key).decode('UTF-8')
        if v_typ=="string":
           v_val = redis_db_sour.get(v_key).decode('UTF-8')
           i_counter=i_counter+1
           redis_db_dest.set(v_key,v_val)
           print('Type={0},{1}={2}'.format(v_typ,v_key,v_val))
    print('migrate {0} string keys complete'.format(str(i_counter)))


def main():
    i_counter       = 0
    redis_db_sour   = StrictRedis(host='192.168.1.101', port=6379,db=0,encoding='utf8', decode_responses=True)
    redis_db_dest   = StrictRedis(host='192.168.1.102', port=6379, db=0,encoding='utf8', decode_responses=True)
    keys            = redis_db_sour.keys()
    keys.sort()
    for key in keys:
        v_key = key
        v_typ = redis_db_sour.type(v_key)
        if v_typ=="string":
           v_val = redis_db_sour.get(v_key)
           i_counter=i_counter+1
           redis_db_dest.set(v_key,v_val.decode('latin1'))
           print('Type={0},{1}={2}'.format(v_typ,v_key,v_val))

    print('migrate {0} string keys complete'.format(str(i_counter)))


if __name__ == "__main__":
     main()
