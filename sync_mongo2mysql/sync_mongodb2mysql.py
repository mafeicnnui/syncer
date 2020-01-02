#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/30 9:31
# @Author  : 马飞
# @File    : sync_mysql2mongo.py
# @Software: PyCharm
# @Func    : optimizer create table

import sys,time
import traceback
import configparser
import warnings
import pymongo
from bson.objectid import ObjectId
import pymysql
import json
import datetime
import smtplib
from email.mime.text import MIMEText
from dateutil import parser

def send_mail465(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def send_mail(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port),user=user, passwd=password, db=service,charset='utf8',
                           cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_ds_mongo(ip,port,service):
    conn = pymongo.MongoClient(host=ip, port=int(port))
    return conn[service]

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    #get mail parameter
    config['send_user']                = cfg.get("sync", "send_mail_user")
    config['send_pass']                = cfg.get("sync", "send_mail_pass")
    config['acpt_user']                = cfg.get("sync", "acpt_mail_user")
    config['mail_title']               = cfg.get("sync", "mail_title")
    config['sync_table']               = cfg.get("sync", "sync_table")
    config['batch_size']               = cfg.get("sync", "batch_size")
    config['gather_rows']              = cfg.get("sync", "gather_rows")

    #get mysql parameter
    db_mysql                           = cfg.get("sync", "db_mysql")
    db_mysql_ip                        = db_mysql.split(':')[0]
    db_mysql_port                      = db_mysql.split(':')[1]
    db_mysql_service                   = db_mysql.split(':')[2]
    db_mysql_user                      = db_mysql.split(':')[3]
    db_mysql_pass                      = db_mysql.split(':')[4]
    config['db_mysql_ip']              = db_mysql_ip
    config['db_mysql_port']            = db_mysql_port
    config['db_mysql_service']         = db_mysql_service
    config['db_mysql_user']            = db_mysql_user
    config['db_mysql_pass']            = db_mysql_pass
    config['db_mysql_string']          = db_mysql_ip + ':' + db_mysql_port + '/' + db_mysql_service
    config['db_mysql']                 = get_ds_mysql(db_mysql_ip, db_mysql_port, db_mysql_service, db_mysql_user, db_mysql_pass)
    #get mongodb parameter
    db_mongo                           = cfg.get("sync", "db_mongo")
    db_mongo_ip                        = db_mongo.split(':')[0]
    db_mongo_port                      = db_mongo.split(':')[1]
    db_mongo_service                   = db_mongo.split(':')[2]
    config['db_mongo_ip']              = db_mongo_ip
    config['db_mongo_port']            = db_mongo_port
    config['db_mongo_service']         = db_mongo_service
    config['db_mongo']                 = get_ds_mongo(db_mongo_ip, db_mongo_port, db_mongo_service)
    return config

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def format_table(sync_tab):
    v_line     = ''
    v_lines    = ''
    i_counter  = 0
    v_space    = ' '.ljust(25,' ')
    for tab in  sync_tab.split(","):
        v_line    = v_line+tab+','
        i_counter = i_counter+1
        if i_counter%5==0:
           if i_counter==5:
              v_lines = v_lines + v_line[0:-1] + '\n'
              v_line  = ''
           else:
              v_lines = v_lines +v_space+ v_line[0:-1] + '\n'
              v_line  = ''
    v_lines = v_lines + v_space+v_line[0:-1]
    return v_lines

def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        if key=='sync_table':
           print(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=', format_table(config[key]))
        else:
           print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def init(config,debug):
    config = get_config(config)
    #print dict
    if debug:
       print_dict(config)
    return config

def is_valid_date(strdate):
    try:
        if ":" in strdate and len(strdate)==26 and time.strptime(strdate[0:19], "%Y-%m-%d %H:%M:%S"):
           return True

        if time.strptime(strdate, "%Y-%m-%d %H:%M:%S"):
           return True

        if time.strptime(strdate, "%Y-%m-%d"):
           return True

        return False
    except:
        return False

def get_col_len(db,tab,col,gather_rows):
    c1      = db[tab]
    results = c1.find({col:{"$exists":"true"}},{col:1}).limit(int(gather_rows))
    n_len   = 0
    for rec in results:
        #print(rec,rec[col])
        if len(str(rec[col]))>n_len:
           n_len=len(str(rec[col]))*2
    return n_len

def get_col_type(db,tab,col,gather_rows):
    c1           = db[tab]
    results      = c1.find({col:{"$exists":"true"}},{col:1}).limit(int(gather_rows))
    i_int_counter= 0
    i_str_counter= 0
    i_rq_counter = 0

    for rec in results:
        #print(col,str(rec[col]),str(rec[col]).isdigit(),str(rec[col]).count(','))
        if str(rec[col]).isdigit() and str(rec[col]).count(',')==0 :
            i_int_counter=i_int_counter+1
        elif is_valid_date(str(rec[col])):
            i_rq_counter=i_rq_counter+1
        else:
            i_str_counter=i_str_counter+1

    if i_str_counter>0:
       if get_col_len(db,tab,col,gather_rows)<4000:
          return 'varchar'
       elif get_col_len(db, tab, col,gather_rows)< 8000:
          return 'text'
       else:
          return 'longtext'

    if i_int_counter>0 and i_str_counter==0 and i_rq_counter==0:
       return 'bigint'

    if i_rq_counter>0 and i_int_counter==0 and i_str_counter==0:
       return 'datetime'

def is_number(str):
  try:
    # 因为使用float有一个例外是'NaN'
    if str=='NaN':
      return False
    float(str)
    return True
  except ValueError:
    return False

######################################################################
#
#  1.遍历MongoDB表所有记录
#  2.遍历每一条记录所有key,获取每个key的长度，及类型统计
#  3.逐条遍历以后每一条数据，动态修改key的长度，及类型统计
#
######################################################################
def get_tab_ddl2(db,tab, where,gather_rows,debug):
    desc       = set()
    stats      = {}
    c1         = db[tab]
    start_time = datetime.datetime.now()
    r_counter  = 0
    results    = c1.find(where).limit(gather_rows)
    n_totals   = gather_rows     #results.count()
    print('begin calc column...')
    for rec in results:
        r_counter = r_counter + 1
        for key in rec:
            #add key to set
            desc.add(key)
            #calc column type stats
            v_counter = 0
            v_max_len = 0
            i_counter = 0
            f_counter = 0
            f_prec    = 4
            d_counter = 0

            if str(rec[key]).isdigit() and str(rec[key]).count(',') == 0:
                i_counter = 1
                f_prec    = 0
                v_max_len = 0
            elif is_number(str(rec[key])) and str(rec[key]).count(',') == 0 and str(rec[key]).count('_') == 0:
                f_counter = 1
                f_prec    = 4
                v_max_len = 40

            elif is_valid_date(str(rec[key])):
                d_counter = 1
                f_prec    = 0
                v_max_len = 0
            else:
                v_counter = 1
                f_prec    = 0
                v_max_len = len(str(rec[key]))

            if key in stats:
               stats[key]={
                           'v_counter':stats[key]['v_counter']+v_counter,
                           'v_max_len':v_max_len if v_max_len>stats[key]['v_max_len'] else stats[key]['v_max_len'],
                           'd_counter':stats[key]['d_counter']+d_counter,
                           'i_counter':stats[key]['i_counter']+i_counter,
                           'f_counter':stats[key]['f_counter']+f_counter,
                           'f_prec'   :f_prec
                          }
            else:
               stats[key]={
                           'v_counter':v_counter,
                           'd_counter':d_counter,
                           'v_max_len':v_max_len,
                           'i_counter':i_counter,
                           'f_counter':f_counter,
                           'f_prec'   : f_prec
                           }
            if  r_counter % 1000 ==0 :
                print('\rComputing table:{0} column type and length,process:{1}/{2} ,Complete:{3}%,elapse:{4}s'.
                         format(tab,n_totals,r_counter,
                                round(r_counter / n_totals * 100, 2),
                                str(get_seconds(start_time))),end='')

    print('\rComputing table:{0} column type and length,process:{1}/{2} ,Complete:{3}%,elapse:{4}s'.
          format(tab, n_totals, r_counter,
                 round(r_counter / n_totals * 100, 2),
                 str(get_seconds(start_time))), end='')
    print('')
    print('end calc column,elapse time:{0}'.format(str(get_seconds(start_time))))
    #output dict stats
    if debug:
       print_dict(stats)
       print(desc)


    d_desc={}
    for key in stats:
        if stats[key]['v_counter']>0:
            if stats[key]['v_max_len']< 4000:
               d_desc[key]={'type':'varchar','length':stats[key]['v_max_len'],'scale':stats[key]['f_prec']}
            elif stats[key]['v_max_len']< 8000:
               d_desc[key] = {'type': 'text', 'length': 0,'scale':stats[key]['f_prec]']}
            else:
               d_desc[key] = {'type': 'longtext','length': 0,'scale':stats[key]['f_prec]']}
        elif stats[key]['i_counter']>0 and stats[key]['f_counter']==0 and stats[key]['v_counter']==0 and stats[key]['d_counter']==0:
             d_desc[key] = {'type': 'bigint', 'length': 0,'scale':stats[key]['f_prec']}
        elif stats[key]['f_counter']>0 and stats[key]['i_counter']==0 and stats[key]['v_counter']==0 and stats[key]['d_counter']==0:
             d_desc[key] = {'type': 'decimal', 'length': stats[key]['v_max_len'],'scale':stats[key]['f_prec']}
        elif stats[key]['d_counter'] > 0 and stats[key]['i_counter'] == 0 and stats[key]['v_counter'] == 0 and stats[key]['f_counter'] == 0:
             d_desc[key] = {'type': 'datetime', 'length': 0, 'scale': stats[key]['f_prec']}
        else:
             d_desc[key] = {'type': 'varchar', 'length': stats[key]['v_max_len'], 'scale': stats[key]['f_prec']}
    if debug:
       print('print dict d_desc...')
       print_dict(d_desc)

    v_pre = ' '.ljust(5, ' ')
    v_ddl = 'create table {0} (\n'.format(tab)
    for key in d_desc:
        if d_desc[key]['type'] == 'varchar':
            if d_desc[key]['length']==0:
               v_ddl = v_ddl + v_pre + key.ljust(40, ' ') + '{0}({1}),\n'.format(d_desc[key]['type'],100)
            else:
               v_ddl = v_ddl + v_pre + key.ljust(40, ' ') + '{0}({1}),\n'.format(d_desc[key]['type'],
                                                                                 d_desc[key]['length']*3)
        elif d_desc[key]['type'] =='decimal':
            v_ddl = v_ddl + v_pre + key.ljust(40, ' ') + '{0}({1},{2}),\n'.format(d_desc[key]['type'],
                                                                                  d_desc[key]['length'],
                                                                                  d_desc[key]['scale'])
        else:
            v_ddl = v_ddl + v_pre + key.ljust(40, ' ') + d_desc[key]['type'] + ',\n'
    v_ddl = v_ddl[0:-2] + '\n' + ')'
    return v_ddl


def get_tab_ddl(db,tab,where,gather_rows,debug):
    desc  = set()
    cols  = ['_id']
    lens  = {}
    types = {}
    c1    = db[tab]
    for rec in c1.find(where):
       for key in rec:
           desc.add(key)

    desc = list(desc)
    desc.remove('_id')
    cols.extend(desc)
    print('Get table {0} column length,please wait!'.format(tab))
    for col in cols:
        lens[col] = get_col_len(db, tab, col,gather_rows)
        if debug:
           print('Table {0} column {1} length is {2}'.format(tab,col,str(lens[col])))

    print('Get table {0} column type,please wait!'.format(tab))
    for col in cols:
        types[col] = get_col_type(db, tab, col,gather_rows)
        if debug:
           print('Table {0} column {1} length is {2}'.format(tab, col, types[col]))

    v_pre = ' '.ljust(5, ' ')
    v_ddl = 'create table {0} (\n'.format(tab)
    for key in cols:
        if types[key]=='varchar':
           v_ddl = v_ddl + v_pre + key.ljust(40, ' ') + '{0}({1}),\n'.format(types[key],str(lens[key]))
        else:
           v_ddl = v_ddl + v_pre + key.ljust(40, ' ') + types[key] + ',\n'
    v_ddl = v_ddl[0:-2] + '\n' + ')'
    return v_ddl

def check_mysql_tab_exists(db,tab):
   cr=db.cursor()
   sql="""select count(0) from information_schema.tables
            where table_schema=database() and table_name='{0}'""".format(tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs['count(0)']

def get_mongodb_table_total_rows(db,tab):
    cr = db.cursor()
    v_sql="select count(0) from {0}".format(tab)
    cr.execute(v_sql)
    rs=cr.fetchone()
    cr.close()
    return rs['count(0)']

def get_table_total_rows(db,tab):
    cr = db.cursor()
    v_sql="select count(0) from {0}".format(tab)
    cr.execute(v_sql)
    rs=cr.fetchone()
    cr.close()
    return rs['count(0)']

def get_ins_header(db,tab,val):
    c1 = db[tab]
    results = c1.find({"_id":val})
    dic = results[0]
    #del dic['_id']
    v_ddl='insert into {0} ('.format(tab)
    for key in dic:
        v_ddl = v_ddl + key+','
    v_ddl=v_ddl[0:-1]+')'
    return v_ddl

def get_ins_values(dic):
    v_tmp=''
    for key in dic:
        v_tmp=v_tmp+"'"+format_sql(str(dic[key]))+"',"
    return v_tmp[0:-1]

def get_mongo_where(db,p_start,p_end):
    cur_mysql  = db.cursor()
    cur_mysql.execute('select _id from t_pk where id between {0} and {1}'.format(p_start,p_end))
    v_ids    = []
    rs_mysql = cur_mysql.fetchall()
    for dic in list(rs_mysql):
        v_ids.append(ObjectId(dic['_id']))
    return v_ids

def get_mongo_incr_where_old(tab):
    v_day = tab.split(':')[2]
    #print('v_day=',v_day,type(v_day))
    if v_day =='':
       return {}
    n_day = int(tab.split(':')[2])
    v_col = tab.split(':')[1]
    v_rq  = (datetime.datetime.now() + datetime.timedelta(days=-n_day)).strftime('%Y-%m-%d %H:%M:%S')
    v_json={"{0}".format(v_col):{"$gt":"{0}".format(v_rq)}}
    return v_json

def get_mongo_incr_where(tab):
    v_day = tab.split(':')[2]
    if v_day =='':
       return {}
    n_day = int(tab.split(':')[2])
    v_col=tab.split(':')[1]
    v_rq = (datetime.datetime.now() + datetime.timedelta(days=-n_day)).strftime('%Y-%m-%dT%H:%M:%S.00Z')
    v_rq = parser.parse(v_rq)
    v_json = {v_col: {"$gt": v_rq}}
    return v_json

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def init_tmp_tab(config):
    db_mysql  = config['db_mysql']
    cur_mysql = db_mysql.cursor()
    if check_mysql_tab_exists(db_mysql, 't_pk')==0:
        cur_mysql.execute('''CREATE TABLE t_pk (
                              _id varchar(200) DEFAULT NULL,
                              id bigint(20) NOT NULL AUTO_INCREMENT,
                              PRIMARY KEY (id))''')
        print('MySQL:{0},t_pk temp table created!'.format(config['db_mysql_string']))
    cur_mysql.close()

def main():
    #init variable
    config = ""
    debug = False
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    #get config
    config          = init(config,debug)
    config['debug'] = debug
    #process
    db_mongodb      = config['db_mongo']
    db_mysql        = config['db_mysql']
    cur_mysql       = db_mysql.cursor()

    #init t_pk
    init_tmp_tab(config)

    for tabs in config['sync_table'].split(","):
        tab       = tabs.split(':')[0]
        cur_mongo = db_mongodb[tab]
        v_where   = get_mongo_incr_where(tabs)
        results   = cur_mongo.find(v_where)
        n_totals  = results.count()
        n_batch   = int(config['batch_size'])
        n_gather_rows   = int(config['gather_rows'])
        if n_totals == 0:
           print("Table:{0} 0 rows,skip sync!".format(tab))
           continue

        # create tables
        if check_mysql_tab_exists(db_mysql,tab)==0:
            v_ddl =get_tab_ddl2(db_mongodb,tab,v_where,n_gather_rows,config['debug'])
            if config['debug']:
               print(v_ddl)
            #sys.exit(0)
            #v_ddl = get_tab_ddl(db_mongodb,tab,v_where,n_gather_rows,config['debug'])
            cur_mysql.execute(v_ddl)
            print('MySQL:{} table created!'.format(tab))
        else:
            print('MySQL:{} table already exists!'.format(tab))

        #write t_pk
        v_ins      = ''
        v_header   ='insert into t_pk(_id) values '
        i_counter  = 0
        start_time = datetime.datetime.now()

        #init t_pk
        cur_mysql.execute('truncate table t_pk')
        #insert t_pk
        for dic in results:
            v_ins =v_ins+ "('{0}'),".format(dic['_id'])
            i_counter=i_counter+1
            if i_counter % n_batch ==0:
               v_sql=v_header+v_ins[0:-1]
               cur_mysql.execute(v_sql)
               db_mysql.commit()
               v_ins=''
               print('\rMySQL:t_pk insert {0}/{1} ,Complete:{2}%,elapse:{3}s'.
                     format(n_totals,i_counter,round(i_counter / n_totals * 100, 2),str(get_seconds(start_time))),end='')
        #process last batch
        if v_ins!='':
            v_sql = v_header + v_ins[0:-1]
            cur_mysql.execute(v_sql)
            db_mysql.commit()
            print('\rMySQL:t_pk insert {0}/{1} ,Complete:{2}%,elapse:{3}s'.
                  format(n_totals, i_counter, round(i_counter / n_totals * 100, 2),str(get_seconds(start_time))), end='')
        print('')
        print('MySQL:t_pk insert {0} rows ok!'.format(str(n_totals)))

        #insert data
        start_time    = datetime.datetime.now()
        cur_mongo     = db_mongodb[tab]
        n_batch_size  = int(config['batch_size'])
        n_total_rows  = get_table_total_rows(db_mysql,'t_pk')
        n_start       = 1
        n_end         = n_batch
        i_counter     = 0
        while n_start<= n_total_rows:
            v_sql     = ''
            v_where   = get_mongo_where(db_mysql, n_start, n_end)
            results   = cur_mongo.find({"_id": {'$in':v_where}})
            for dic in results:
                v_header = get_ins_header(db_mongodb, tab,dic['_id'])
                v_sql=v_header+' values '+'('+get_ins_values(dic)+')'
                try:
                   cur_mysql.execute(v_sql)
                except:
                   print(traceback.format_exc())
                   print(v_sql)
                   sys.exit(0)
            db_mysql.commit()
            i_counter=i_counter+results.count()
            n_start  = n_start+n_batch_size
            n_end    = n_end+n_batch_size
            print("\rTable:{0},Total :{1},Process :{2},Complete:{3}%,elapse:{4}s"
                  .format(tab, n_total_rows, i_counter,
                          round(i_counter / n_total_rows * 100, 2),
                          str(get_seconds(start_time))), end='')
        print('')

if __name__ == "__main__":
     main()
