#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/11/30 11:36
# @Author : 马飞
# @File : generate_rpt.py.py
# @Software: PyCharm

import phoenixdb
import phoenixdb.cursor
import json

def get_db(cfg):
    config = {}
    db_phoenix                         = cfg['phoenix_db']
    db_phoenix_ip                      = db_phoenix.split(':')[0]
    db_phoenix_port                    = db_phoenix.split(':')[1]
    config['db_phoenix_ip']            = db_phoenix_ip
    config['db_phoenix_port']          = db_phoenix_port
    config['db_phoenix']               = get_ds_phoenix(db_phoenix_ip, db_phoenix_port)
    return config['db_phoenix']

def get_ds_phoenix(ip,port):
    url = 'http://{0}:{1}/'.format(ip,port)
    conn = phoenixdb.connect(url, autocommit=True)
    return conn

def loadJson():
    f = open("config.json", encoding='utf-8')
    setting = json.load(f)
    return setting

if __name__ == "__main__":
    cfg = loadJson()
    db  = get_db(cfg)
    cr  = db.cursor()
    sid = '100'

    #执行SQL
    for i in cfg['data']:
        try:
          print('execute:',i['sql'].replace(':sid', '100'))
          cr.execute(i['sql'].replace(':sid','100'))
        except Exception as e :
          print(str(e))

    #输出报表结果
    print('Report result:')
    print('------------------------------------------------')
    cr.execute('select * from "t_bb01_tmp" where "sid"=100 order by "id"')
    rs=cr.fetchall()
    for i in rs:
        print(i)