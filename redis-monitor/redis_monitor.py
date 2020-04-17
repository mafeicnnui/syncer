#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/6/4 9:20
# @Author : 马飞
# @File : redis_monitor.py
# @Software: PyCharm

import redis
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

def main():
    #r   = redis.Redis(host='10.2.39.70', port=6379,db=1)
    r   = redis.StrictRedis(host='r-2ze9f53dad8419b4.redis.rds.aliyuncs.com',port=6379,password='WXwk2018',db=1)
    v   = r.slowlog_get()
    for i in v:
        print(i)

if __name__ == "__main__":
     main()



