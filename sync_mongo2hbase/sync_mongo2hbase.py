# -*- coding: utf-8 -*-
# @Time    : 2019/1/30 9:31
# @Author  : ma.fei
# @File    : sync_mysql2mongo.py
# @Software: PyCharm
# @Func    : optimizer bulk insert

import sys,time
import configparser
import warnings
import pymongo
from   bson.objectid import ObjectId
import happybase
import datetime
from dateutil import parser

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_ds_mongo_auth(mongodb_str):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    user          = mongodb_str.split(':')[3]
    password      = mongodb_str.split(':')[4]
    conn          = pymongo.MongoClient('mongodb://{0}:{1}/'.format(ip,int(port)))
    db            = conn[service]
    if user != '' and password != '':
        db.authenticate(user, password)
    return db

def get_ds_mongo_str(mongodb_str):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    return 'MongoDB:{0}:{1}/{2}'.format(ip,port,service)

def get_ds_hbase(ip,port):
    conn = happybase.Connection(host=ip, port=int(port), timeout=3600000, autoconnect=True, table_prefix=None,
                                table_prefix_separator=b'_', compat='0.98', transport='buffered',protocol='binary')
    conn.open()
    return conn

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    #get mail parameter
    config['sync_table']               = cfg.get("sync", "sync_table")
    config['batch_size']               = cfg.get("sync", "batch_size")
    config['batch_size_incr']          = cfg.get("sync", "batch_size_incr")
    config['gather_rows']              = cfg.get("sync", "gather_rows")
    config['sync_time_type']           = cfg.get("sync", "sync_time_type")
    config['full_sync_gaps']           = cfg.get("sync", "full_sync_gaps")

    #get mongodb parameter
    db_mongo                           = cfg.get("sync", "db_mongo")
    db_mongo_ip                        = db_mongo.split(':')[0]
    db_mongo_port                      = db_mongo.split(':')[1]
    db_mongo_service                   = db_mongo.split(':')[2]
    db_mongo_user                      = db_mongo.split(':')[3]
    db_mongo_password                  = db_mongo.split(':')[4]
    config['db_mongo_ip']              = db_mongo_ip
    config['db_mongo_port']            = db_mongo_port
    config['db_mongo_service']         = db_mongo_service
    config['db_mongo_user']            = db_mongo_user
    config['db_mongo_password']        = db_mongo_password
    config['db_mongo']                 = get_ds_mongo_auth(cfg.get("sync", "db_mongo"))
    config['db_mongo_str']             = get_ds_mongo_str(cfg.get("sync", "db_mongo"))
    #get hbase parameter
    db_hbase                           = cfg.get("sync", "db_hbase")
    db_hbase_ip                        = db_hbase.split(':')[0]
    db_hbase_port                      = db_hbase.split(':')[1]
    config['db_hbase_ip']              = db_hbase_ip
    config['db_hbase_port']            = db_hbase_port
    config['db_hbase']                 = get_ds_hbase(db_hbase_ip, db_hbase_port)
    return config

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

def init(config,debug):
    config = get_config(config)
    if debug:
       print_dict(config)
    return config

def is_valid_datetime(strdate):
    if isinstance(strdate,datetime.datetime):
        return True
    else:
        return False

def get_table_total_rows(db,tab):
    cr = db.cursor()
    v_sql="select count(0) from {0}".format(tab)
    cr.execute(v_sql)
    rs=cr.fetchone()
    cr.close()
    return rs['count(0)']

def get_mongo_where(p_ids):
    try:
        v_ids = []
        for dic in p_ids:
            v_ids.append(ObjectId(dic))
        return v_ids
    except:
        return p_ids

def get_mongo_incr_where(config,tab):
    v_day = tab.split(':')[2]
    if v_day =='':
       return {}

    n_day = int(tab.split(':')[2])
    v_col=tab.split(':')[1]

    if config['sync_time_type']=='day':
       v_rq = (datetime.datetime.now() + datetime.timedelta(days=-n_day)).strftime('%Y-%m-%dT%H:%M:%S.00Z')
    elif config['sync_time_type']=='hour':
       v_rq = (datetime.datetime.now() + datetime.timedelta(hours=-n_day)).strftime('%Y-%m-%dT%H:%M:%S.00Z')
    elif config['sync_time_type']=='min':
       v_rq = (datetime.datetime.now() + datetime.timedelta(minutes=-n_day)).strftime('%Y-%m-%dT%H:%M:%S.00Z')
    else:
       v_rq =''

    if v_rq =='':
       return {}
    v_rq = parser.parse(v_rq)
    v_json = {v_col: {"$gte": v_rq}}
    #print('v_json:',v_json)
    return v_json

def get_mongo_full_where(config,tab,n_days):
    v_col = tab.split(':')[1]
    d_min_rq   = get_mongo_tab_min_rq(config, tab)
    d_begin_rq = d_min_rq+datetime.timedelta(days=n_days)
    d_end_rq   = d_min_rq+datetime.timedelta(days=n_days+int(config['full_sync_gaps']))
    v_begin_rq = datetime.datetime.strftime(d_begin_rq, '%Y-%m-%d %H:%M:%S')
    v_end_rq   = datetime.datetime.strftime(d_end_rq, '%Y-%m-%d %H:%M:%S')
    d_begin_rq = parser.parse(v_begin_rq)
    d_end_rq   = parser.parse(v_end_rq)

    v_json = {
              "$and":[
                  {v_col:{"$gte" : d_begin_rq,"$lt" : d_end_rq}}
                 ]
              }
    return v_json,d_begin_rq,d_end_rq

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def check_mysql_tab_exists(db,tab):
   cr=db.cursor()
   sql="""select count(0) from information_schema.tables
            where table_schema=database() and table_name='{0}'""".format(tab )
   cr.execute(sql)
   rs=cr.fetchone()
   return rs['count(0)']

def get_hbase_tab_exists_rows(config,tab):
    db_hbase = config['db_hbase']
    v_tname = 'hopsonone:source_mallcoo_' + tab
    table = db_hbase.table(v_tname)
    i_counter =0
    for key, data in table.scan():
        i_counter=i_counter+1
        if i_counter>=1:
           break
    return i_counter

def full_sync(config):
    db_mongodb  = config['db_mongo']
    db_mongodb2 = config['db_mongo']
    config_init = {}
    n_days      = 0
    d_now       = datetime.datetime.now()
    start_time  = datetime.datetime.now()
    #sync data
    for tabs in config['sync_table'].split(","):
      tab              = tabs.split(':')[0]
      col              = tabs.split(':')[1]
      n_all_rows       = get_mongo_tab_rows(config,tabs)
      config_init[tab] = False
      n_total_rows     = 0

      if exist_hbase_table(config,tab) and get_hbase_tab_exists_rows(config,tab)==0:
         if col!='':
             while True:
                 config_init[tab] = True
                 cur_mongo        = db_mongodb[tab]
                 cur_mongo2       = db_mongodb2[tab]
                 v_where,d_begin_rq,d_end_rq = get_mongo_full_where(config,tabs,n_days)
                 v_begin_rq       = datetime.datetime.strftime(d_begin_rq, '%Y-%m-%d %H:%M:%S')
                 v_end_rq         = datetime.datetime.strftime(d_end_rq, '%Y-%m-%d %H:%M:%S')
                 results          = cur_mongo.find(v_where, {'_id': 1}, no_cursor_timeout=True)
                 n_batch_size     = int(config['batch_size'])
                 n_rows           = results.count()
                 i_counter        = 0
                 v_ids            = []
                 n_total_rows     = n_total_rows+n_rows
                 n_rows_loop      = 0

                 if d_end_rq>d_now:
                    break

                 for rec in results:
                     i_counter = i_counter + 1
                     v_ids.append(rec['_id'])
                     v_results = []
                     if i_counter % n_batch_size == 0:
                         v_mongo_where   = get_mongo_where(v_ids)
                         v_mongo_results = cur_mongo2.find({"_id": {'$in': v_mongo_where}})
                         n_rows_loop     = n_rows_loop+ v_mongo_results.count()

                         for r in v_mongo_results:
                             v_results.append(r)

                         for x in v_results:
                             x['_id']=str(x['_id'])

                         write_hbase_rows_batch(config,tab,preprocessor(v_results))
                         v_ids = []

                     if  i_counter%10000==0:
                         print("\rTable:{0},Total :{1},Processed :{2},Complete:{3}%,elapse:{4}s,Range[{5}~{6}],rows:{7}[{8}%]"
                               .format(tab,
                                       n_all_rows,
                                       n_total_rows,
                                       round(n_total_rows / n_all_rows * 100, 2),
                                       str(get_seconds(start_time)),
                                       v_begin_rq,
                                       v_end_rq,
                                       n_rows,
                                       round(n_rows_loop / n_rows * 100, 2)
                                       ), end='')

                 #last batch
                 if len(v_ids)>0:
                    v_results       = []
                    v_mongo_where   = get_mongo_where(v_ids)
                    v_mongo_results = cur_mongo2.find({"_id": {'$in': v_mongo_where}})
                    n_rows_loop     = n_rows_loop + v_mongo_results.count()

                    for r in v_mongo_results:
                        v_results.append(r)

                    for x in v_results:
                        x['_id'] = str(x['_id'])

                    write_hbase_rows_batch(config, tab, preprocessor(v_results))
                    v_ids = []

                    print("\rTable:{0},Total :{1},Processed :{2},Complete:{3}%,elapse:{4}s,Range[{5}~{6}],rows:{7}[{8}%]"
                          .format(tab,
                                  n_all_rows,
                                  n_total_rows,
                                  round(n_total_rows / n_all_rows * 100, 2),
                                  str(get_seconds(start_time)),
                                  v_begin_rq,
                                  v_end_rq,
                                  n_rows,
                                  round(n_rows_loop / n_rows * 100, 2)
                                  ), end='')
                    print('')

                 n_days = n_days + int(config['full_sync_gaps'])
         else:
             config_init[tab] = True
             cur_mongo = db_mongodb[tab]
             cur_mongo2 = db_mongodb2[tab]
             results = cur_mongo.find({'_id': {"$exists": "true"}}, {'_id': 1}, no_cursor_timeout=True)
             start_time = datetime.datetime.now()
             n_batch_size = int(config['batch_size'])
             n_total_rows = results.count()
             i_counter = 0
             v_ids = []

             print('Table:{0} total {1} rows!'.format(tab, str(n_total_rows)))
             for rec in results:
                 i_counter = i_counter + 1
                 v_ids.append(rec['_id'])
                 v_results = []
                 if i_counter % n_batch_size == 0:
                     v_mongo_where = get_mongo_where(v_ids)
                     v_mongo_results = cur_mongo2.find({"_id": {'$in': v_mongo_where}})
                     for r in v_mongo_results:
                         v_results.append(r)

                     for x in v_results:
                         x['_id'] = str(x['_id'])

                     write_hbase_rows_batch(config, tab, v_results)
                     v_ids = []

                 if i_counter % 10000 == 0:
                     print("\rTable:{0},Total :{1},Process :{2},Complete:{3}%,elapse:{4}s"
                           .format(tab, n_total_rows, i_counter,
                                   round(i_counter / n_total_rows * 100, 2),
                                   str(get_seconds(start_time))), end='')

             # last batch
             if len(v_ids) > 0:
                 v_results = []
                 v_mongo_where = get_mongo_where(v_ids)
                 v_mongo_results = cur_mongo2.find({"_id": {'$in': v_mongo_where}})

                 for r in v_mongo_results:
                     v_results.append(r)

                 for x in v_results:
                     x['_id'] = str(x['_id'])

                 write_hbase_rows_batch(config, tab, v_results)
                 v_ids = []

                 print("\rTable:{0},Total :{1},Process :{2},Complete:{3}%,elapse:{4}s"
                       .format(tab, n_total_rows, i_counter,
                               round(i_counter / n_total_rows * 100, 2),
                               str(get_seconds(start_time))), end='')
                 print('')

      else:
         print('{0} Table: {1} already exists data,skip full sync!'.format(get_time(),tab))

    return config_init

def incr_sync(config,config_init):
    db_mongodb  = config['db_mongo']
    db_mongodb2 = config['db_mongo']
    #sync data
    for tabs in config['sync_table'].split(","):
        tab = tabs.split(':')[0]
        day = tabs.split(':')[2]
        if exist_hbase_table(config,tab):
            cur_mongo    = db_mongodb[tab]
            cur_mongo2   = db_mongodb2[tab]
            start_time   = datetime.datetime.now()
            n_batch_size = int(config['batch_size_incr'])
            i_counter    = 0
            v_ids        = []
            if day=='':
               print('{0} Table: {1} no config sync column,starting full sync!'.format(get_time(),tab))

            v_incr       = get_mongo_incr_where(config, tabs)
            results      = cur_mongo.find(v_incr, {'_id': 1}, no_cursor_timeout=True)
            n_total_rows = results.count()
            if n_total_rows>0:
                for rec in results:
                    i_counter = i_counter + 1
                    v_ids.append(rec['_id'])
                    v_results = []
                    if i_counter % n_batch_size == 0:
                        v_mongo_where   = get_mongo_where(v_ids)
                        v_mongo_results = cur_mongo2.find({"_id": {'$in': v_mongo_where}},no_cursor_timeout = True)

                        for r in v_mongo_results:
                            v_results.append(r)

                        for x in v_results:
                            x['_id'] = str(x['_id'])

                        write_hbase_rows_batch(config, tab, preprocessor(v_results))
                        v_ids = []


                    if i_counter % int(config['batch_size_incr']) == 0:
                        print("\r{0} Table:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                              .format(get_time(),tab, n_total_rows, i_counter,
                                      round(i_counter / n_total_rows * 100, 2),
                                      str(get_seconds(start_time))), end='')

                #last batch
                if len(v_ids) > 0:
                    v_results =[]
                    v_mongo_where = get_mongo_where(v_ids)
                    v_mongo_results = cur_mongo2.find({"_id": {'$in': v_mongo_where}})

                    for r in v_mongo_results:
                        v_results.append(r)

                    for x in v_results:
                        x['_id'] = str(x['_id'])

                    write_hbase_rows_batch(config, tab, preprocessor(v_results))

                    print("\r{0} Table:{1},Total :{2},Process :{3},Complete:{4}%,elapse:{5}s"
                          .format(get_time(),
                                  tab, n_total_rows, i_counter,
                                  round(i_counter / n_total_rows * 100, 2),
                                  str(get_seconds(start_time))), end='')
                    print('')

            else:
               print('{0} Table:{1} recent {2} {3} no found data,skip increment sync!'.
                     format(get_time(), tab, day,config['sync_time_type']))
        else:
           if day=='':
              print('{0} Table:{1} no exists! skip sync!'.format(get_time(),tab))

    return config_init

def exist_hbase_table(config,tname):
    db_hbase=config['db_hbase']
    v_tname='hopsonone:source_mallcoo_'+tname
    tab_list = db_hbase.tables()
    for t in tab_list:
        tab=bytes.decode(t)
        if tab==v_tname:
           return True
    return False

def write_hbase_write(config,tname):
    db_hbase=config['db_hbase']
    v_tname = 'hopsonone:source_mallcoo_' + tname
    families = {
       'info':dict(max_versions=3)
    }
    db_hbase.create_table(v_tname, families)
    print('hbase create table {0}'.format(v_tname))

def write_hbase_rows(config,tab,row_data):
    v_rkey   = ''
    d_cols   = {}
    db_hbase = config['db_hbase']
    pk_name  = '_id'
    table    = db_hbase.table(tab)
    for r in row_data:
        print('write_hbase_rows=',r)
        for key in r:
            if key==pk_name:
               v_rkey=r[key]
            d_cols['info:'+key]=str(r[key])
        table.put(v_rkey,d_cols)

def preprocessor(result):
    for res in result:
        for key in res:
            if is_valid_datetime(res[key]):
               res[key] = res[key] + datetime.timedelta(hours=8)
    return result

def write_hbase_rows_batch(config,tab,row_data):
    v_rkey   = ''
    d_cols   = {}
    db_hbase = config['db_hbase']
    pk_name  = '_id'
    v_tname  = 'hopsonone:source_mallcoo_' + tab
    table    = db_hbase.table(v_tname)
    bat      = table.batch()
    for r in row_data:
        for key in r:
            if key==pk_name:
               v_rkey=r[key]
            d_cols['info:'+key]=str(r[key])
        bat.put(v_rkey,d_cols)
    bat.send()

def cre_tab(config):
    db_mongo = config['db_mongo']
    for tabs in config['sync_table'].split(","):
        tab           = tabs.split(':')[0]
        cur_mongo     = db_mongo[tab]
        n_totals      = cur_mongo.count()

        if n_totals == 0:
            print("{0} Table:{1} 0 rows,skip sync!".format(get_time(),tab))
            continue

        if not exist_hbase_table(config, tab):
           write_hbase_write(config, tab)

def get_mongo_tab_min_rq(config,tab):
    db_mongo = config['db_mongo']
    tname    = tab.split(':')[0]
    cname    = tab.split(':')[1]
    c_mongo  = db_mongo[tname]
    results  = c_mongo.find({},{"updateTime":1,"_id":0}).sort([(cname,1)]).limit(1)
    for i in results:
        return i['updateTime']

def get_mongo_tab_rows(config,tab):
    db_mongo = config['db_mongo']
    tname    = tab.split(':')[0]
    c_mongo  = db_mongo[tname]
    results  = c_mongo.count()
    return results

def main():
    #init
    config = ""
    debug = False
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    #get config
    config = init(config,debug)
    config['debug'] = debug

    #hbase create table
    print("Creating table,please wait!".format(get_time()))
    cre_tab(config)

    #full sync
    print("Full Sync data,please wait!".format(get_time()))
    config_init=full_sync(config)

    #incr_sync
    print("Increment Sync data,please wait!".format(get_time()))
    incr_sync(config,config_init)


if __name__ == "__main__":
     main()
