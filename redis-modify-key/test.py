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

def main():
    i_counter = 0
    r = redis.Redis(host='r-2ze9f53dad8419b4.redis.rds.aliyuncs.com', port=6379, db=1)
    print(r)



if __name__ == "__main__":
     main()
