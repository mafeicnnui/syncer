#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/5/6 11:52
# @Author : 马飞
# @File : modify_key.py
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

def print_dict_hz_name(r,config):
    print('redis 变更key名称统计:')
    print('-'.ljust(80,'-'))
    print(' '.ljust(3,' ')+"name".ljust(30,' ')+'value'.ljust(20,' '))
    print('-'.ljust(80,'-'))
    i_counter = 0
    for key in config:
      i_counter = i_counter + config[key]
      print(' '.ljust(3,' ')+key.ljust(60,' '),str(config[key]).ljust(20,' '))
    print('-'.ljust(80,'-'))
    print('合计:{0}'.format(str(i_counter)))

def print_dict_hz_type(r,config):
    print('redis 变更key类型统计:')
    print('-'.ljust(80,'-'))
    print(' '.ljust(3,' ')+"name".ljust(30,' ')+'value'.ljust(20,' '))
    print('-'.ljust(80,'-'))
    i_counter = 0
    for key in config:
      i_counter = i_counter + config[key]
      print(' '.ljust(3,' ')+key.ljust(60,' ')+str(config[key]).ljust(20,' '))
    print('-'.ljust(80,'-'))
    print('合计:{0}'.format(str(i_counter)))


def modify_key_name(r,p_key,rkeys,rkeys_hj):
    #####################################################################
    #   规则 1：卡券上架状态                                              #
    #   旧key:  coupons:onlineStatus:{couponsId}                        #
    #   新key:  coupons:status:{couponsId}                              #
    #####################################################################
    v_key=p_key.decode('UTF-8')

    if len(re.findall(r'^coupons:onlineStatus:',v_key,re.M))>0:
       v_new_key=v_key.replace('coupons:onlineStatus:','coupons:status:')
       r.rename(v_key,v_new_key)
       rkeys[v_new_key]=r.get(v_new_key).decode('UTF-8')
       rkeys_hj['卡券上架状态(string)']=rkeys_hj['卡券上架状态(string)']+1

    #####################################################################
    #   规则 2：单人领取次数                                              #
    #   旧key:  coupons:single:member:get:count:{couponsId}:{mid}       #
    #   新key:  coupons:member:{mid}:{couponsId}:getCount:single        #
    #####################################################################
    if len(re.findall(r'^coupons:single:member:get:count:', v_key, re.M)) > 0:
       couponsId = v_key.split(':')[5]
       mid       = v_key.split(':')[6]
       v_new_key = 'coupons:member:{0}:{1}:getCount:single'.format(mid,couponsId)
       r.rename(v_key, v_new_key)
       rkeys[v_new_key] = r.get(v_new_key).decode('UTF-8')
       rkeys_hj['单人领取次数(string)'] = rkeys_hj['单人领取次数(string)'] + 1

    #####################################################################
    #   规则 3：单人单日领取次数                                           #
    #   旧key:  coupons:day:member:get:count:{couponsId}:{mid}          #
    #   新key:  coupons:member:{mid}:{couponsId}:getCount:day           #
    #####################################################################
    if len(re.findall(r'^coupons:day:member:get:count:', v_key, re.M)) > 0:
       couponsId = v_key.split(':')[5]
       mid       = v_key.split(':')[6]
       v_new_key = 'coupons:member:{0}:{1}:getCount:day'.format(mid,couponsId)
       r.rename(v_key, v_new_key)
       rkeys[v_new_key] = r.get(v_new_key).decode('UTF-8')
       rkeys_hj['单人单日领取次数(string)'] = rkeys_hj['单人单日领取次数(string)'] + 1

    #####################################################################
    #   规则 4：单人单月领取次数                                           #
    #   旧key:  coupons:month:member:get:count:{couponsId}:{mid}        #
    #   新key:  coupons:member:{mid}:{couponsId}:getCount:month         #
    #####################################################################
    if len(re.findall(r'^coupons:month:member:get:count:', v_key, re.M)) > 0:
       couponsId = v_key.split(':')[5]
       mid       = v_key.split(':')[6]
       v_new_key = 'coupons:member:{0}:{1}:getCount:month'.format(mid,couponsId)
       r.rename(v_key, v_new_key)
       rkeys[v_new_key] = r.get(v_new_key).decode('UTF-8')
       rkeys_hj['单人单月领取次数(string)'] = rkeys_hj['单人单月领取次数(string)'] + 1

    #####################################################################
    #   规则 5：卡券剩余量(卡券中心)                                       #
    #   旧key:  coupons:center:number:remaining:%s                      #
    #   新key:  coupons:remainingQuantity:sencen:center:{couponsId}     #
    #####################################################################
    if len(re.findall(r'^coupons:center:number:remaining:', v_key, re.M)) > 0:
       couponsId = v_key.split(':')[4]
       v_new_key = 'coupons:remainingQuantity:sencen:center:{0}'.format(couponsId)
       r.rename(v_key, v_new_key)
       rkeys[v_new_key] = r.get(v_new_key).decode('UTF-8')
       rkeys_hj['卡券剩余量(卡券中心)(string)'] = rkeys_hj['卡券剩余量(卡券中心)(string)'] + 1

    #####################################################################
    #   规则 6：卡券核销所属商家（list)                                           #
    #   旧key:  coupons:verification:business:{couponsId}               #
    #   新key:  coupons:use:business:{couponsId}                        #
    #####################################################################
    if len(re.findall(r'^coupons:verification:business:', v_key, re.M)) > 0:
       couponsId = v_key.split(':')[3]
       v_new_key = 'coupons:use:business:{0}'.format(couponsId)
       n_len     = r.llen(v_key)
       v_val     = [x.decode('UTF-8') for x in r.lrange(v_key, 0, n_len)]
       r.rename(v_key, v_new_key)
       rkeys[v_new_key] = str(v_val)
       rkeys_hj['卡券核销所属商家（list)'] = rkeys_hj['卡券核销所属商家（list)'] + 1

    #####################################################################
    #   规则 7：单人单日核销张数(string)                                   #
    #   旧key:  coupons:day:member:use:count:{couponsId}:{mid}          #
    #   新key:  coupons:member:{mid}:{couponsId}:useCount:day           #
    #####################################################################
    if len(re.findall(r'^coupons:day:member:use:count:', v_key, re.M)) > 0:
       couponsId = v_key.split(':')[5]
       mid       = v_key.split(':')[6]
       v_new_key = 'coupons:member:{0}:{1}:useCount:day'.format(mid,couponsId)
       r.rename(v_key, v_new_key)
       rkeys[v_new_key] =  r.get(v_new_key).decode('UTF-8')
       rkeys_hj['单人单日核销张数(string)'] = rkeys_hj['单人单日核销张数(string)'] + 1

    ############################################################################
    #   规则 8：卡券剩余量(任务投放)(string)                                      #
    #   旧key:  coupons:task:relations:remainingNumber:{taskId}:{couponsId}    #
    #   新key:  coupons:remainingQuantity:sencen:task:{taskId}:{couponsId}     #
    ############################################################################
    if len(re.findall(r'^coupons:task:relations:remainingNumber:', v_key, re.M)) > 0:
       taskId    = v_key.split(':')[4]
       couponsId = v_key.split(':')[5]
       v_new_key = 'coupons:remainingQuantity:sencen:task:{0}:{1}'.format(taskId,couponsId)
       r.rename(v_key, v_new_key)
       rkeys[v_new_key] =  r.get(v_new_key).decode('UTF-8')
       rkeys_hj['卡券剩余量(任务投放)(string)'] = rkeys_hj['卡券剩余量(任务投放)(string)'] + 1

    ############################################################################
    #   规则 9：任务发放给用的次数(string)                                        #
    #   旧key:  coupons:get:num:{mid}:{taskId}                                 #
    #   新key:  coupons:task:member:{mid}:{taskId}:getCount                    #
    ############################################################################
    if len(re.findall(r'^coupons:get:num:', v_key, re.M)) > 0:
       mid       = v_key.split(':')[3]
       taskId    = v_key.split(':')[4]
       v_new_key = 'coupons:task:member:{0}:{1}:getCount'.format(mid,taskId)
       r.rename(v_key, v_new_key)
       rkeys[v_new_key] =  r.get(v_new_key).decode('UTF-8')
       rkeys_hj['任务发放给用的次数(string)'] = rkeys_hj['任务发放给用的次数(string)'] + 1

    ############################################################################
    #   规则 10：任务发放次数锁(string)                                           #
    #   旧key:  coupons:send:num:lock:{taskId}                                  #
    #   新key:  coupons:task:send:lock:member:{taskId}:getCount           #
    #############################################################################
    if len(re.findall(r'^coupons:send:num:lock:', v_key, re.M)) > 0:
       taskId    = v_key.split(':')[4]
       v_new_key = 'coupons:task:member:{0}:getCount'.format(taskId)
       r.rename(v_key, v_new_key)
       rkeys[v_new_key] =  r.get(v_new_key).decode('UTF-8')
       rkeys_hj['任务发放次数锁(string)'] = rkeys_hj['任务发放次数锁(string)'] + 1


def modify_key_type(r,p_key,rkeys,rkeys_hj):
   #####################################################################
   #   规则 1：卡券领取限制                                              #
   #   旧key:  coupons:get:rule:{couponsId} ,旧类型:string              #
   #   新key:  coupons:get:rule:{couponsId} ,新类型:hash                #
   #####################################################################
   v_key=p_key.decode('UTF-8')
   if len(re.findall(r'^coupons:get:rule:', v_key, re.M)) > 0 and r.type(v_key).decode('UTF-8')=='string':
       v_val = r.get(v_key).decode('UTF-8')
       d_val = json.loads(v_val)
       r.delete(v_key)
       for key in d_val:
           r.hset(name=v_key, key=key, value=d_val[key])
       rkeys[v_key]=str(d_val)
       rkeys_hj['卡券领取限制(hash)']=rkeys_hj['卡券领取限制(hash)']+1

   #####################################################################
   #   规则 2：任务发放限制                                              #
   #   旧key:  coupons:task:issueRule:{couponsId} ,旧类型:string        #
   #   新key:  coupons:task:issueRule:{couponsId} ,新类型:hash          #
   #####################################################################
   v_key=p_key.decode('UTF-8')
   if len(re.findall(r'^coupons:task:issueRule:', v_key, re.M)) > 0 and r.type(v_key).decode('UTF-8')=='string':
       v_val = r.get(v_key).decode('UTF-8')
       d_val = json.loads(v_val)
       r.delete(v_key)
       for key in d_val:
           r.hset(name=v_key, key=key, value=d_val[key])
       rkeys[v_key]=str(d_val)
       rkeys_hj['任务发放限制(hash)']=rkeys_hj['任务发放限制(hash)']+1

def init_dict(d_name,d_type):
    d_name['卡券上架状态(string)'] = 0
    d_name['单人领取次数(string)'] = 0
    d_name['单人单日领取次数(string)'] = 0
    d_name['单人单月领取次数(string)'] = 0
    d_name['卡券剩余量(卡券中心)(string)'] = 0
    d_name['卡券核销所属商家（list)'] = 0
    d_name['单人单日核销张数(string)'] = 0
    d_name['卡券剩余量(任务投放)(string)'] = 0
    d_name['任务发放给用的次数(string)'] = 0
    d_name['任务发放次数锁(string)'] = 0
    d_type['卡券领取限制(hash)'] = 0
    d_type['任务发放限制(hash)'] = 0
    return d_name,d_type

def main():
    r        = redis.Redis(host='10.2.39.70', port=6379,db=1)
    keys     = r.keys()
    rkeys    = {}
    d_name   = {}
    d_type   = {}
    init_dict(d_name,d_type)
    keys.sort()
    for key in keys:
        modify_key_name(r,key,rkeys,d_name)
        modify_key_type(r,key, rkeys,d_type)

    if len(rkeys)>0:
       print_dict(r,rkeys)

    if len(d_name)>0:
       print_dict_hz_name(r,d_name)

    if len(d_type)>0:
       print_dict_hz_type(r,d_type)


if __name__ == "__main__":
     main()
