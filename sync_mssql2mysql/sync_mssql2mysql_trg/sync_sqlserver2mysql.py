#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Software: PyCharm
import sys,time
import traceback
import configparser
import warnings
import pymssql
import pymysql
import datetime

def exception_info():
    e_str=traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

def get_ds_sqlserver(ip,port,service,user,password):
    conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service,charset='utf8')
    return conn

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    sync_server_sour                  = cfg.get("sync","sync_db_server")
    sync_server_dest                  = cfg.get("sync","sync_db_mysql")
    sync_dir                          = cfg.get("sync","sync_dir")
    sync_table                        = cfg.get("sync", "sync_table").lower()
    sync_col_name                     = cfg.get("sync", "sync_col_name").lower()
    sync_col_type                     = cfg.get("sync", "sync_col_type").lower()
    sync_col_val                      = cfg.get("sync", "sync_col_val").lower()
    batch_size                        = cfg.get("sync", "batch_size")
    sync_gap                          = cfg.get("sync", "sync_gap")
    full_sync_gap                     = cfg.get("sync", "full_sync_gap")
    ddl_supported                     = cfg.get("sync", "ddl_supported")
    db_sour_ip                        = sync_server_sour.split(':')[0]
    db_sour_port                      = sync_server_sour.split(':')[1]
    db_sour_service                   = sync_server_sour.split(':')[2]
    db_sour_user                      = sync_server_sour.split(':')[3]
    db_sour_pass                      = sync_server_sour.split(':')[4]
    db_dest_ip                        = sync_server_dest.split(':')[0]
    db_dest_port                      = sync_server_dest.split(':')[1]
    db_dest_service                   = sync_server_dest.split(':')[2]
    db_dest_user                      = sync_server_dest.split(':')[3]
    db_dest_pass                      = sync_server_dest.split(':')[4]
    config['db_sqlserver_ip']         = db_sour_ip
    config['db_sqlserver_port']       = db_sour_port
    config['db_sqlserver_service']    = db_sour_service
    config['db_sqlserver_user']       = db_sour_user
    config['db_sqlserver_pass']       = db_sour_pass
    config['db_mysql_ip']             = db_dest_ip
    config['db_mysql_port']           = db_dest_port
    config['db_mysql_service']        = db_dest_service
    config['db_mysql_user']           = db_dest_user
    config['db_mysql_pass']           = db_dest_pass
    config['db_sqlserver_string']     = "SQLServer:"+db_sour_ip+':'+db_sour_port+'/'+db_sour_service
    config['db_mysql_string']         = "MySQL:"+db_dest_ip+':'+db_dest_port+'/'+db_dest_service
    config['db_sqlserver']            = get_ds_sqlserver(db_sour_ip,db_sour_port ,db_sour_service,db_sour_user,db_sour_pass)
    config['db_sqlserver2']           = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
    config['db_mysql']                = get_ds_mysql(db_dest_ip,db_dest_port ,db_dest_service,db_dest_user,db_dest_pass)
    config['sync_table']              = sync_table
    config['sync_col_name']           = sync_col_name
    config['sync_col_type']           = sync_col_type
    config['sync_col_val']            = sync_col_val
    config['batch_size']              = batch_size
    config['sync_gap']                = sync_gap
    config['sync_dir']                = sync_dir
    config['full_sync_gap']           = full_sync_gap
    config['ddl_supported']           = ddl_supported
    return config

def check_mysql_tab_exists(config,tab):
   db=config['db_mysql']
   cr=db.cursor()
   sql="select count(0) from information_schema.tables where table_schema='{0}' and table_name='{1}'".format(config['db_mysql_service'],tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_sqlserver_tab_exists_data(config,tname):
   db=config['db_sqlserver']
   cr=db.cursor()
   sql="select count(0) from {0}".format(tname)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_sqlserver_tab_exists(config,tab):
   db=config['db_sqlserver']
   cr=db.cursor()
   sql="select count(0) from sysobjects where id=object_id(N'{0}')".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_sqlserver_tab_sync(config,tab):
   db=config['db_sqlserver']
   cr=db.cursor()
   sql="select count(0) from t_sync_log_init where tname='{0}'".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_sqlserver_tab_exists_pk(config,tab):
   db=config['db_sqlserver']
   cr=db.cursor()
   sql = """select count(0)
              from syscolumns col, sysobjects obj
            where col.id=obj.id and obj.id=object_id(N'{0}')
            and  (select  1
                  from  dbo.sysindexes si
                      inner join dbo.sysindexkeys sik on si.id = sik.id and si.indid = sik.indid
                      inner join dbo.syscolumns sc on sc.id = sik.id    and sc.colid = sik.colid
                      inner join dbo.sysobjects so on so.name = si.name and so.xtype = 'pk'
                  where  sc.id = col.id  and sc.colid = col.colid)=1
         """.format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def convert(v_sql):
    return v_sql.lower().replace("nvarchar","varchar").\
                  replace("datetime(23)","datetime").\
                  replace("numeric","decimal").\
                  replace("nvarchar","varchar").\
                  replace("money","DECIMAL").\
                  replace("IDENTITY(1,1)","").\
                  replace("smalldatetime(16)","datetime").\
                  replace("float","decimal").\
                  replace("bit","varchar").\
                  replace("timestamp(8)","varchar(50)")

def log(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_sql(filename):
    file_handle = open(filename, 'r')
    line = file_handle.readline()
    lines = ''
    while line:
        lines = lines + line
        line = file_handle.readline()
    lines = lines + line
    return lines

def check_dml_trigger(config,tab):
    cr = config['db_sqlserver'].cursor()
    sql = "SELECT count(0) FROM sysobjects WHERE name='trig_{0}_dml_sync' AND type='TR'".format(tab.replace('.','_'))
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]

def check_ddl_trigger(config):
    cr = config['db_sqlserver'].cursor()
    sql = "select count(0) from sys.triggers  where name='trig_db_ddl_sync'"
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]

def cre_ddl_trgger(config,debug):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    cr_trg_sql= get_sql('./templete/sync.ddl.templete')
    dp_trg_sql='drop trigger trig_db_ddl_sync on database'
    if check_ddl_trigger(config)>0:
       cr_source.execute(dp_trg_sql)
       if debug:
           print('Trigger:trig_db_ddl_sync drop complete!')
    #print(cr_trg_sql)
    cr_source.execute(cr_trg_sql)
    db_source.commit()
    if debug:
         print('Trigger:trig_db_ddl_sync create complete!')


def check_db_initialize(config):
    cr = config['db_sqlserver'].cursor()
    sql = "select count(0) from sysobjects  where xtype='U'  and id=object_id(N't_sync_log')"
    cr.execute(sql)
    rs=cr.fetchone()
    return rs[0]

def db_initialize(config):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    if check_db_initialize(config)>0:
       print('SQLServer already initialize,skipping...')
    else:
       print("SQLServer initialize sync metadata table...")
       cr_tab_sql = get_sql('./initialize/cre_tab.txt')
       cr_source.execute(cr_tab_sql)
       print("SQLServer initialize sync function f_get_table_ddl...")
       cr_fun_sql = get_sql('./initialize/f_get_table_ddl.txt')
       cr_source.execute(cr_fun_sql)
       print("SQLServer initialize sync function f_get_pk_vals...")
       cr_fun_sql = get_sql('./initialize/f_get_pk_vals.txt')
       cr_source.execute(cr_fun_sql)
       print("SQLServer initialize sync function f_get_pk_names...")
       cr_fun_sql = get_sql('./initialize/f_get_pk_names.txt')
       cr_source.execute(cr_fun_sql)
       print("SQLServer initialize sync procedure proc_init_tab...")
       cr_fun_sql = get_sql('./initialize/proc_init_tab.txt')
       cr_source.execute(cr_fun_sql)
       print("SQLServer initialize sync procedure proc_init_tab_nopk...")
       cr_fun_sql = get_sql('./initialize/proc_init_tab_nopk.txt')
       cr_source.execute(cr_fun_sql)
       db_source.commit()
       print('SQLServer Initizlize ok!')

def write_sqlserver_dml_trigger(config):
    cr = config['db_sqlserver'].cursor()
    file_handle = open('./cancelsync/drop_dml_trg.txt', 'w')
    sql = """select 'disable trigger '+lower(OBJECT_SCHEMA_NAME(object_id)+'.'+name)+' on ' 
                      +lower(OBJECT_SCHEMA_NAME(parent_id))+'.'+lower(OBJECT_NAME(parent_id))
                      +' drop trigger '+lower(OBJECT_SCHEMA_NAME(object_id)+'.'+name)
              from sys.triggers 
             where name like 'trig_%dml_sync' order by 1"""
    cr.execute(sql)
    rs=cr.fetchall()
    for i in list(rs):
       file_handle.write(i[0]+';'+'\n')
    file_handle.close()

def db_cancelsync(config):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    print("SQLServer cancel sync ddl trigger...")
    cr_tab_sql = get_sql('./cancelsync/drop_ddl_trg.txt')
    cr_source.execute(cr_tab_sql)
    print("SQLServer cancel sync dml trigger...")
    write_sqlserver_dml_trigger(config)
    cr_tab_sql = get_sql('./cancelsync/drop_dml_trg.txt')
    cr_source.execute(cr_tab_sql)
    print("SQLServer cancel sync table...")
    cr_tab_sql = get_sql('./cancelsync/drop_tab.txt')
    cr_source.execute(cr_tab_sql)
    print("SQLServer cancel sync function...")
    cr_tab_sql = get_sql('./cancelsync/drop_fun.txt')
    cr_source.execute(cr_tab_sql)
    print("SQLServer cancel sync procedure...")
    cr_tab_sql = get_sql('./cancelsync/drop_proc.txt')
    cr_source.execute(cr_tab_sql)
    db_source.commit()
    print('SQLServer cancel sync ok...')

def db_reinitialize(config):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    print("SQLServer reinitizlize sync ddl trigger...")
    cr_tab_sql = get_sql('./cancelsync/drop_ddl_trg.txt')
    cr_source.execute(cr_tab_sql)
    print("SQLServer reinitizlize sync dml trigger...")
    write_sqlserver_dml_trigger(config)
    cr_tab_sql = get_sql('./cancelsync/drop_dml_trg.txt')
    cr_source.execute(cr_tab_sql)
    print("SQLServer reinitizlize sync metadata table...")
    cr_tab_sql = get_sql('./reinitialize/cre_tab.txt')
    cr_source.execute(cr_tab_sql)
    print("SQLServer reinitizlize sync function f_get_table_ddl...")
    cr_fun_sql = get_sql('./reinitialize/f_get_table_ddl.txt')
    cr_source.execute(cr_fun_sql)
    print("SQLServer reinitizlize sync function f_get_pk_vals...")
    cr_fun_sql = get_sql('./reinitialize/f_get_pk_vals.txt')
    cr_source.execute(cr_fun_sql)
    print("SQLServer reinitizlize sync function f_get_pk_names...")
    cr_fun_sql = get_sql('./reinitialize/f_get_pk_names.txt')
    cr_source.execute(cr_fun_sql)
    print("SQLServer reinitizlize sync procedure proc_init_tab...")
    cr_fun_sql = get_sql('./reinitialize/proc_init_tab.txt')
    cr_source.execute(cr_fun_sql)
    print("SQLServer reinitizlize sync procedure proc_init_tab_nopk...")
    cr_fun_sql = get_sql('./reinitialize/proc_init_tab_nopk.txt')
    cr_source.execute(cr_fun_sql)
    db_source.commit()
    print('SQLServer reinitizlize ok!')

def get_sqlserver_schema_name(config,tname):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    cr_source.execute("select OBJECT_SCHEMA_NAME(object_id('{0}'))".format(tname))
    rs_source=cr_source.fetchone()
    return rs_source[0].lower()

def cre_dml_trgger(config,tab,debug):
    pk_vals   = get_sync_table_pk_vals(config,tab)
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    cr_trg_sql= get_sql('./templete/sync.dml.templete').format(tab.replace('.','_'),tab,tab,pk_vals,tab,pk_vals,tab,pk_vals,tab)
    dp_trg_sql= 'drop trigger {0}.trig_{1}_dml_sync'.format(get_sqlserver_schema_name(config,tab),tab.replace('.','_'))
    if check_dml_trigger(config,tab)>0:
       cr_source.execute(dp_trg_sql)
       if debug:
           print('Trigger:trig_{0}_dml_sync drop complete!'.format(get_sqlserver_schema_name(config,tab),tab.replace('.','_')))
    #print(cr_trg_sql)
    cr_source.execute(cr_trg_sql)
    db_source.commit()
    if debug:
         print('Trigger:trig_{0}_dml_sync create complete!'.format(tab.replace('.','_')))

def init_table(config,tab,debug):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    config['sync_tab']=tab+'_sync'
    #检测同步表是否存在，如果存在将其删除
    if check_sqlserver_tab_exists(config,config['sync_tab'])>0:
        print("\nTable:{0} already exists,dropped!".format(config['sync_tab']))
        cr_source.execute("drop table {0}".format(config['sync_tab']))
    #对同步表进行加共享读锁，并拷贝数据
    print("lock table {0} copy data into temp table {1},please wait...".format(tab, config['sync_tab']))
    cr_source.execute("begin transaction")
    cr_source.execute("select * into {0} from {1}  with(tablock, holdlock)".format(config['sync_tab'],tab))
    print("copy data into temp table {0} complete!".format( config['sync_tab']))
    #如果表无主键，则不建立DML触发器
    if check_sqlserver_tab_exists_pk(config, tab.replace('_pk','')) > 0:
       cre_dml_trgger(config, tab, debug)
    cr_source.execute("commit transaction")
    db_source.commit()
    print("release table {0} lock ok!".format(tab))
    sql="exec proc_init_tab '{0}'".format(tab)
    print('Initializing Table:{0},please wait...'.format(tab))
    cr_source.execute(sql)
    db_source.commit()
    if debug:
         print('Table:{0} init complete!'.format(tab))

def init_table_nopk(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    sync_sqlserver_ddl_nopk(config,tab)
    sql="exec proc_init_tab_nopk '{0}'".format(tab)
    print('Initializing nopk Table:{0},please wait...'.format(tab))
    cr_source.execute(sql)
    db_source.commit()
    print('Table:{0} init complete!'.format(tab))
    cr_source.execute("delete from t_sync_log where tname='{0}' and opt='DDL'".format(tab))
    print("Table {0} nopk ddl log clear complete!".format(tab))

def get_tab_header(config,tab):
    cr=config['db_sqlserver'].cursor()
    sql="select top 1 * from {0}".format(tab)
    cr.execute(sql)
    desc=cr.description
    s1=""
    if tab.find('.')>0:
       s1="insert into "+get_mapping_tname(config,tab).lower()+"("
    else:
       s1="insert into "+get_mapping_tname(config,tab).lower()+"("
    s2=" values "
    for i in range(len(desc)):
      s1=s1+desc[i][0].lower()+','
    s1=s1+config['sync_col_name']+")"
    cr.close()
    return s1+s2

def check_sync_sqlserver_col_comments(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_comment = """SELECT  count(0)
                    FROM sys.tables A
                    INNER JOIN syscolumns B ON B.id = A.object_id
                    left join  systypes t   on b.xtype=t.xusertype
                    LEFT JOIN sys.extended_properties C ON C.major_id = B.id AND C.minor_id = B.colid
                    WHERE A.object_id = object_id('{0}')  and c.value is not null        
                   """.format(tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    return rs_source[0]

def sync_sqlserver_col_comments(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc   = config['db_mysql']
    cr_desc   = db_desc.cursor()
    v_tname   = get_mapping_tname(config, tab).lower()
    v_comment ="""SELECT                                
                        case when t.name ='numeric' then
                          'alter table {0} modify column '+lower(B.name)+' '+t.name+'('+
                            cast(COLUMNPROPERTY(b.id,b.name,'PRECISION') AS varchar)+','+
                            CAST(isnull(COLUMNPROPERTY(b.id,b.name,'Scale'),0) as varchar)	   
                           +') comment '''+CAST(c.value as varchar)+''''
                        when t.name in('nvarchar','varchar','int') then
                          'alter table {1} modify column '+lower(B.name)+' '+t.name+'('+
                            cast(COLUMNPROPERTY(b.id,b.name,'PRECISION') AS varchar)+') comment '''+CAST(c.value as varchar)+''''
                        else
                          'alter table {2} modify column '+lower(B.name)+' '+t.name+' comment '''+CAST(c.value as varchar)+''''
                        end
                FROM sys.tables A
                INNER JOIN syscolumns B ON B.id = A.object_id
                left join  systypes t   on b.xtype=t.xusertype
                LEFT JOIN sys.extended_properties C ON C.major_id = B.id AND C.minor_id = B.colid
                WHERE A.object_id = object_id('{3}') and c.value is not null        
               """.format(v_tname,v_tname,v_tname,tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchall()
    for j in range(len(rs_source)):
        v_ddl_sql = rs_source[j][0]
        print(v_ddl_sql)
        cr_desc.execute(convert(v_ddl_sql))
    db_desc.commit()
    cr_desc.close()

def check_sync_sqlserver_tab_comments(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_comment ="""select count(0)  from sys.extended_properties A  
                  where A.major_id=object_id('{0}')  and a.name='MS_Description'""".format(tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    return rs_source[0]

def sync_sqlserver_tab_comments_his(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc   = config['db_mysql']
    cr_desc   = db_desc.cursor()
    v_comment ="""select 
                   'alter table '+lower(object_name(object_id('{0}')))+' comment '''+cast(a.value as varchar)+''''
                  from sys.extended_properties A  
                  where A.major_id=object_id('{1}')
                    and a.name='MS_Description'""".format(tab,tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    if len(rs_source)>0:
        v_ddl_sql = rs_source[0]
        cr_desc.execute(v_ddl_sql)
        cr_desc.close()

def sync_sqlserver_tab_comments(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc   = config['db_mysql']
    cr_desc   = db_desc.cursor()
    v_tname   = get_mapping_tname(config,tab).lower()
    v_comment ="""select 
                   'alter table {0} comment '''+cast(a.value as varchar)+''''
                  from sys.extended_properties A  
                  where A.major_id=object_id('{1}')
                    and a.name='MS_Description'""".format(v_tname,tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    if len(rs_source)>0:
        v_ddl_sql = rs_source[0]
        cr_desc.execute(v_ddl_sql)
        cr_desc.close()

def alter_mapping_column_name(config,tab):
    db_source  = config['db_sqlserver']
    cr_source  = db_source.cursor()
    cr_source2 = db_source.cursor()
    mapping_sql="""select tname,col_name,mapping_name from t_sync_col_mapping where tname='{0}' order by id""".format(tab)
    cr_source.execute(mapping_sql)
    rs=cr_source.fetchall()
    for i in list(rs):
      rename_sql="""exec sp_rename '{0}.[{1}]','{2}','COLUMN'""".format(i[0],i[1],i[2])
      cr_source2.execute(rename_sql)
      print("Table: {0}'[{1}] column mapping is:{2}...ok".format(i[0],i[1],i[2]))

def get_mapping_tname(config,tab):
    db_source  = config['db_sqlserver']
    cr_source  = db_source.cursor()
    cr_source2 = db_source.cursor()
    mapping_sql="""select top 1 mapping_name from t_sync_tab_mapping where tname='{0}'""".format(tab)
    cr_source.execute(mapping_sql)
    rs=cr_source.fetchone()
    if rs is None:
        if tab.find('.')==0:
           return tab
        else:
           return tab.replace('.','_')
    else:
        return rs[0]

def sync_sqlserver_ddl_nopk(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc   = config['db_mysql']
    cr_desc   = db_desc.cursor()
    cr_source.execute("select name from sysobjects where xtype='U' and name='{0}' order by name".format(tab))
    rs_source = cr_source.fetchall()
    for j in range(len(rs_source)):
        #处理无主键的表,检查原始表
        if check_sqlserver_tab_exists_pk(config,rs_source[j][0].replace('_pk',''))==0:
           print("{0},Table:{1} not exist primary!".format(config['db_sqlserver_string'],rs_source[j][0]))
           #如果新表存在将其删除
           if check_sqlserver_tab_exists(config, rs_source[j][0]) > 0:
               print("\n{0},Table:{1} already exists,dropped!".format(config['db_sqlserver_string'],rs_source[j][0]))
               cr_source.execute("drop table {0}".format(rs_source[j][0]))
           print("{0},Copy data to {1} table from {2}...".format(config['db_sqlserver_string'],rs_source[j][0] ,rs_source[j][0] ))
           cr_source.execute("select * into {0} from {1}  with(nolock)".format(rs_source[j][0],rs_source[j][0].replace('_pk','')))
           print("{0},Copy data to {1} table from {2}...ok!".format(config['db_sqlserver_string'],rs_source[j][0] ,rs_source[j][0] ))
           cr_source.execute("alter table {0} add id int identity(1,1) not null primary key".format(rs_source[j][0]))
           print("Table {0} add primary key id success!".format(rs_source[j][0]))
           print("modify table:{0} column mapping...".format(rs_source[j][0]))
           alter_mapping_column_name(config,rs_source[j][0])
           print("modify table:{0} column mapping...ok".format(rs_source[j][0]))
           #删除以上DDL日志,避免以上操作DDL再次执行报错
           cr_source.execute("delete from t_sync_log where tname='{0}' and opt='DDL'".format(rs_source[j][0]))
           print("Table {0} ddl log clear complete!".format(rs_source[j][0] ))
           v_ddl_sql ="select dbo.f_get_table_ddl('{0}')".format(rs_source[j][0])
           cr_source.execute(v_ddl_sql)
           rs_source_cre_tab = cr_source.fetchall()
           v_cre_sql = rs_source_cre_tab[0][0]
           if check_mysql_tab_exists(config, rs_source[j][0]) > 0:
               print("{0},Table :{1} already exists!".format(config['db_mysql_string'], rs_source[j][0]))
           else:
               cr_desc.execute(convert(v_cre_sql))
               print("Table:{0} creating success!".format(rs_source[j][0]))
               cr_desc.execute('alter table {0} add {1} {2}'.format(rs_source[j][0], config['sync_col_name'],config['sync_col_type']))
               print("Table:{0} add column {1} success!".format(rs_source[j][0], config['sync_col_name']))
               db_desc.commit()
               #create mysql table comments
               if check_sync_sqlserver_tab_comments(config, rs_source[j][0]) > 0:
                  sync_sqlserver_tab_comments(config, rs_source[j][0])
                  print("Table:{0} comments create complete!".format(rs_source[j][0]))
               #create mysql table column comments
               if check_sync_sqlserver_col_comments(config, rs_source[j][0]) > 0:
                  sync_sqlserver_col_comments(config, rs_source[j][0])
                  print("Table:{0} columns comments create complete!".format(rs_source[j][0]))

    cr_source.close()
    cr_desc.close()

def sync_sqlserver_ddl(config,debug):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc   = config['db_mysql']
    cr_desc   = db_desc.cursor()
    for tab in config['sync_table'].split(","):
        cr_source.execute("""select id, 
                                    OBJECT_SCHEMA_NAME(id) as schema_name, 
                                    OBJECT_NAME(id) as table_name,
                                    DB_NAME() as db_name,
                                    OBJECT_SCHEMA_NAME(id)+'.'+OBJECT_NAME(id) as full_table_name
                             from sysobjects  
                             where xtype='U'  and id=object_id(N'{0}') order by name""".format(tab))
        rs_source = cr_source.fetchall()
        for j in range(len(rs_source)):
            #声明变量
            tab_name=rs_source[j][2].lower()
            tab_prefix = (str(rs_source[j][1])+'.').lower()
            full_tab_name=rs_source[j][4].lower()
            #处理无主键的表
            if check_sqlserver_tab_exists_pk(config,full_tab_name)==0:
               print("{0},Table:{1} not exist primary!".format(config['db_sqlserver_string'],full_tab_name))
               #如果新表存在将其删除
               if check_sqlserver_tab_exists(config, full_tab_name+"_pk") > 0:
                   print("{0},Table:{1} already exists,dropped!".format(config['db_sqlserver_string'],full_tab_name + "_pk"))
                   cr_source.execute("drop table {0}".format(rs_source[j][4] + "_pk"))
               print("{0},Copy data to {1} table from {2}...".format(config['db_sqlserver_string'],full_tab_name + "_pk",full_tab_name ))
               cr_source.execute("select * into {0} from {1}  with(nolock)".format(full_tab_name+"_pk",full_tab_name))
               print("{0},Copy data to {1} from {2}...ok!".format(config['db_sqlserver_string'],full_tab_name + "_pk",full_tab_name))
               config['sync_table'] = config['sync_table'].replace(full_tab_name, full_tab_name + "_pk")
               print("Update config['sync_table']={0}...ok".format(config['sync_table']))
               cr_source.execute("alter table {0} add pkid int identity(1,1) not null primary key".format(full_tab_name+"_pk"))
               print("Table {0} add primary key pkid success!".format(full_tab_name+"_pk"))
               print("modify table:{0} column mapping...".format(full_tab_name+"_pk"))
               alter_mapping_column_name(config,full_tab_name+"_pk")
               print("modify table:{0} column mapping...ok".format(full_tab_name + "_pk"))
               #删除以上DDL日志,避免以上操作DDL再次执行报错
               cr_source.execute("delete from t_sync_log where tname='{0}' and opt='DDL'".format(full_tab_name+"_pk"))
               print("Table {0} ddl log clear complete!".format(full_tab_name + "_pk"))
               v_ddl_sql ="select dbo.f_get_table_ddl('{0}')".format(full_tab_name+"_pk")
               cr_source.execute(v_ddl_sql)
               rs_source_cre_tab = cr_source.fetchall()
               v_cre_sql = rs_source_cre_tab[0][0].replace(full_tab_name, get_mapping_tname(config, full_tab_name))
               if check_mysql_tab_exists(config, get_mapping_tname(config,full_tab_name+"_pk")) > 0:
                   print("{0},Table :{1} already exists!".format(config['db_mysql_string'], get_mapping_tname(config,full_tab_name+"_pk")))
               else:
                   cr_desc.execute(convert(v_cre_sql))
                   print("Table:{0} creating success!".format(get_mapping_tname(config,full_tab_name+"_pk")))
                   print('alter table {0} add primary key ({1})'.format(get_mapping_tname(config,full_tab_name)+"_pk", get_sync_table_pk_cols(config, rs_source[j][4]+"_pk")))
                   cr_desc.execute('alter table {0} add primary key ({1})'.format(get_mapping_tname(config,full_tab_name)+"_pk", get_sync_table_pk_cols(config, rs_source[j][4]+"_pk")))
                   print("Table:{0} add primary key {1} success!".format(get_mapping_tname(config,full_tab_name)+"_pk", get_sync_table_pk_cols(config, rs_source[j][4]+"_pk")))
                   cr_desc.execute('alter table {0} add {1} {2}'.format(get_mapping_tname(config,full_tab_name)+"_pk", config['sync_col_name'],config['sync_col_type']))
                   print("Table:{0} add column {1} success!".format(get_mapping_tname(config,full_tab_name)+"_pk", config['sync_col_name']))
                   db_desc.commit()
                   #create mysql table comments
                   if check_sync_sqlserver_tab_comments(config, full_tab_name+"_pk") > 0:
                      sync_sqlserver_tab_comments(config, full_tab_name+"_pk")
                      print("Table:{0}  comments create complete!".format(full_tab_name+"_pk"))
                   #create mysql table column comments
                   if check_sync_sqlserver_col_comments(config, full_tab_name+"_pk") > 0:
                      sync_sqlserver_col_comments(config, full_tab_name+"_pk")
                      print("Table:{0} columns comments create complete!".format(full_tab_name+"_pk"))
            #处理有主键的表
            else:
               v_ddl_sql="select dbo.f_get_table_ddl('{0}')".format(full_tab_name)
               cr_source.execute(v_ddl_sql)
               rs_source_cre_tab = cr_source.fetchall()
               v_cre_sql =rs_source_cre_tab[0][0].replace(full_tab_name,get_mapping_tname(config,full_tab_name))
               #print(v_cre_sql)
               if check_mysql_tab_exists(config,get_mapping_tname(config,full_tab_name))>0:
                  print("{0},Table :{1} already exists!".format(config['db_mysql_string'],get_mapping_tname(config,full_tab_name)))
               else:
                  cr_desc.execute(convert(v_cre_sql))
                  print("Table:{0} creating success!".format(get_mapping_tname(config,full_tab_name)))
                  cr_desc.execute('alter table {0} add primary key ({1})'.format(get_mapping_tname(config,full_tab_name),get_sync_table_pk_cols(config,rs_source[j][4])))
                  print("Table:{0} add primary key {1} success!".format(get_mapping_tname(config,full_tab_name), config['sync_col_name']))
                  cr_desc.execute('alter table {0} add {1} {2}'.format(get_mapping_tname(config,full_tab_name),config['sync_col_name'],config['sync_col_type']))
                  print("Table:{0} add column {1} success!".format(get_mapping_tname(config,full_tab_name),config['sync_col_name']))
                  db_desc.commit()
                  #create mysql table comments
                  if check_sync_sqlserver_tab_comments(config,full_tab_name)>0:
                     sync_sqlserver_tab_comments(config, full_tab_name)
                     print("Table:{0} comments add complete!".format(full_tab_name))
                  #create mysql table column comments
                  if check_sync_sqlserver_col_comments(config,full_tab_name)>0:
                     sync_sqlserver_col_comments(config, full_tab_name)
                     print("Table:{0} columns comments add complete!".format(full_tab_name))
    cr_source.close()
    cr_desc.close()

def get_sync_init_table_total_rows(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    cr_source.execute("select count(0) from t_sync_log_init where flag='N' and tname='{0}'".format(tab))
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]

def get_sync_table_total_rows(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    cr_source.execute("select count(0) from t_sync_log where flag='N' and tname='{0}'".format(tab))
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]

def get_sqlserver_table_rows(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    cr_source.execute("select count(0) from {0}".format(tab))
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]

def get_sync_table_pk(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_sql="""select col.name
              from syscolumns col, sysobjects obj
             where col.id=obj.id and obj.name='{0}'
               and (SELECT  1
                FROM  dbo.sysindexes si
                    INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                    INNER JOIN dbo.syscolumns sc ON sc.id = sik.id    AND sc.colid = sik.colid
                    INNER JOIN dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                WHERE  sc.id = col.id  AND sc.colid = col.colid)=1;
            """.format(tab)
    cr_source.execute(v_sql)
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]

def get_sync_table_pk_vals(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_sql="""select dbo.f_get_pk_vals('{0}')""".format(tab)
    cr_source.execute(v_sql)
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]

def get_sync_table_pk_cols(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_sql="""select dbo.f_get_pk_names('{0}')""".format(tab)
    cr_source.execute(v_sql)
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]

def get_sync_where(pk_cols,pk_vals):
    v_where=''
    for i in range(len(pk_cols.split(','))):
        v_where=v_where+pk_cols.split(',')[i]+"='"+pk_vals.split('^^^')[i]+"' and "
    return v_where[0:-4]

def sync_sqlserver_no_pk_init(config):
    for tab in config['sync_table'].split(","):
        if check_sqlserver_tab_exists_pk(config, tab.replace('_pk', '')) == 0:
            print("\nFull sync no primary table {0},plear wait...".format(tab))
            init_table_nopk(config,tab)

def sync_sqlserver_init(config,debug):
    for tab in config['sync_table'].split(","):
        #file_handle = open(config['sync_dir']+tab+'.initialize', 'a')
        if check_sqlserver_tab_sync(config,tab)==0 and get_sqlserver_table_rows(config,tab)>0:
           init_table(config,tab,debug)
        #start first sync data
        i_counter        = 0
        n_tab_total_rows = get_sync_init_table_total_rows(config,tab)
        ins_sql_header   = get_tab_header(config,tab)
        n_batch_size     = int(config['batch_size'])
        db_source        = config['db_sqlserver']
        db_source2       = config['db_sqlserver2']
        cr_source        = db_source.cursor()
        cr_source_row    = db_source2.cursor()
        db_desc          = config['db_mysql']
        cr_desc          = db_desc.cursor()
        cr_source.execute("select tname,pk_name,pk_val,opt,id from t_sync_log_init where flag='N' and tname='{0}' and tname not like 'sync_%' order by id".format(tab))
        rs_source = cr_source.fetchmany(n_batch_size)
        while rs_source:
            n_sync_id =''
            v_sql = ''
            for i in range(len(rs_source)):
                n_sync_id=n_sync_id+str(rs_source[i][4])+","
                if rs_source[i][3] == 'I':
                    sql_source_row="select * from {0} where {1} order by {2}".format(rs_source[i][0], get_sync_where(rs_source[i][1],rs_source[i][2]), rs_source[i][1])
                    #print(sql_source_row)
                    cr_source_row.execute(sql_source_row)
                    rs_source_row  = cr_source_row.fetchall()
                    rs_source_desc = cr_source_row.description
                    #print(rs_source_desc)
                    for r in list(rs_source_row):
                        ins_val = ""
                        for j in range(len(r)):
                            col_type = str(rs_source_desc[j][1])
                            if  r[j] is None:
                                ins_val = ins_val + "null,"
                            elif col_type == "1":  #varchar,date
                                ins_val = ins_val + "'"+format_sql(str(r[j])) + "',"
                            elif col_type == "5":  #decimal
                                ins_val = ins_val + "'" + str(r[j])+ "',"
                            elif col_type == "4":  #datetime
                                ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                            elif col_type == "3":
                                if str(r[j]) == "True":    #bit
                                    ins_val = ins_val + "'" + "1" + "',"
                                elif str(r[j]) == "False": #bit
                                    ins_val = ins_val + "'" + "0" + "',"
                                else: #bigint ,int
                                    ins_val = ins_val + "'" +str(r[j])+ "',"
                            elif col_type == "2":  #timestamp
                                ins_val = ins_val + "null,"
                            else:
                                ins_val = ins_val + "'" + str(r[j]) + "',"
                    v_sql = v_sql +'('+ins_val+config['sync_col_val']+'),'
            batch_sql = ins_sql_header + v_sql[0:-1]
            #file_handle.write(batch_sql+';'+'\n')
            #noinspection PyBroadException
            try:
              cr_desc.execute(batch_sql)
              cr_source_row.execute("update t_sync_log_init set flag='Y' where id in ({0})".format(n_sync_id[0:-1]))
              i_counter = i_counter +len(rs_source)
            except:
              print(traceback.format_exc())
              #print(batch_sql)
              sys.exit(0)
            db_desc.commit()
            db_source2.commit()
            print("\rTotal rec:{0},Process rec:{1},Complete:{2}%".format(n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100,2)), end='')
            rs_source = cr_source.fetchmany(n_batch_size)
        #file_handle.close()
        if check_sqlserver_tab_exists(config,'t_sync_'+tab) > 0:
            cr_source.execute("drop table {0}".format('t_sync_'+tab))
            print("\nsync temp table {0} drop complete!\n".format('t_sync_'+tab))

def sync_sqlserver_data(config,debug):
    # start sync dml data
    for tab in config['sync_table'].split(","):
        i_counter        = 0
        i_counter_ins    = 0
        i_counter_del    = 0
        i_counter_upd    = 0
        i_counter_ddl    = 0
        n_tab_total_rows = get_sync_table_total_rows(config,tab)
        ins_sql_header   = get_tab_header(config,tab)
        n_batch_size     = int(config['batch_size'])
        db_source        = config['db_sqlserver']
        db_source2       = config['db_sqlserver2']
        cr_source        = db_source.cursor()
        cr_source_row    = db_source2.cursor()
        db_desc          = config['db_mysql']
        cr_desc          = db_desc.cursor()
        if n_tab_total_rows > 0:
           cr_source.execute("select tname,pk_name,pk_val,opt,id,tsql from t_sync_log where flag='N' and tname='{0}' and tname not like 'sync_%' order by id".format(tab))
           rs_source = cr_source.fetchmany(n_batch_size)
           while rs_source:
                n_sync_id =""
                for i in range(len(rs_source)):
                    n_sync_id=n_sync_id+str(rs_source[i][4])+","
                    v_sql_i=''
                    v_sql_d=''
                    if rs_source[i][3] in('I','U') :
                        sql_source_tab="select * from {0} where {1} order by {2}".format(rs_source[i][0].lower(), get_sync_where(rs_source[i][1],rs_source[i][2]), rs_source[i][1])
                        cr_source_row.execute(sql_source_tab)
                        rs_source_tab = cr_source_row.fetchall()
                        rs_source_desc= cr_source_row.description
                        for r in list(rs_source_tab):
                            ins_val = ""
                            for j in range(len(r)):
                                col_type = str(rs_source_desc[j][1])
                                if  r[j] is None:
                                    ins_val = ins_val + "null,"
                                elif col_type == "1":    #varchar,date
                                    ins_val = ins_val + "'"+format_sql(str(r[j])) + "',"
                                elif col_type == "5":  #int,decimal
                                    ins_val = ins_val + "'" + str(r[j])+ "',"
                                elif col_type == "4":  #datetime
                                    ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                                elif col_type == "3":  #bit
                                    if str(r[j]) == "True":    #bit
                                        ins_val = ins_val + "'" + "1" + "',"
                                    elif str(r[j]) == "False": #bit
                                        ins_val = ins_val + "'" + "0" + "',"
                                    else: #bigint ,int
                                        ins_val = ins_val + "'" +str(r[j])+ "',"
                                elif col_type == "2":  # timestamp
                                    ins_val = ins_val + "null,"
                                else:
                                    ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                            ins_val = ins_val + config['sync_col_val']
                        v_sql_i = ins_sql_header +'('+ins_val+')'
                        v_sql_d = "delete from {0} where {1}".format(get_mapping_tname(config,rs_source[i][0].lower()), get_sync_where(rs_source[i][1].lower(),str(rs_source[i][2])))
                    elif rs_source[i][3] == 'D':
                        v_sql_d = "delete from {0} where {1}".format(get_mapping_tname(config,rs_source[i][0].lower()),get_sync_where(rs_source[i][1].lower(),str(rs_source[i][2])))
                    elif rs_source[i][3] == 'DDL':
                        v_sql_ddl=rs_source[i][5].lower()
                    else:
                        pass

                    #noinspection PyBroadException
                    try:
                        if rs_source[i][3] == 'I':
                            cr_desc.execute(v_sql_i)
                            i_counter_ins=i_counter_ins+1
                        elif rs_source[i][3] == 'D':
                            cr_desc.execute(v_sql_d)
                            i_counter_del=i_counter_del+1
                        elif rs_source[i][3] == 'U':
                            cr_desc.execute(v_sql_d)
                            cr_desc.execute(v_sql_i)
                            i_counter_upd=i_counter_upd+1
                        elif rs_source[i][3] == 'DDL':
                            cr_desc.execute(v_sql_ddl)
                            i_counter_ddl=i_counter_ddl+1
                        else:
                            pass
                        cr_source_row.execute("update t_sync_log set flag='Y' where id in ({0})".format(n_sync_id[0:-1]))
                        i_counter = i_counter +1
                    except:
                        print(traceback.format_exc())
                        #print(v_sql_d)
                        #print(v_sql_i)
                        sys.exit(0)
                    db_desc.commit()
                    db_source2.commit()
                print("\nTime:{0},Total rec:{1},Process rec:{2}(tab:{3},insert:{4},update:{5},delete:{6},ddl:{7}),Complete:{8}%".\
                      format(get_time(),n_tab_total_rows, i_counter,tab,i_counter_ins,i_counter_upd,i_counter_del,i_counter_ddl,round(i_counter / n_tab_total_rows * 100,2)), end='')
                rs_source = cr_source.fetchmany(n_batch_size)

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def sync(config,debug,reinitialize):
    #init timer
    start_time = datetime.datetime.now()
    config=get_config(config)
    #print parameter
    if debug:
       log(config)
    #reinitialize db
    if reinitialize:
       db_reinitialize(config)
    #initialize db
    db_initialize(config)
    #cre ddl trigger
    if config['ddl_supported'].lower()=='y':
       cre_ddl_trgger(config,debug)
    #sync table ddl
    sync_sqlserver_ddl(config, debug)
    #sync data
    while True:
      #init sync table
      sync_sqlserver_init(config, debug)
      #no_pk table full sync
      if get_seconds(start_time)>=int(config['full_sync_gap']):
         sync_sqlserver_no_pk_init(config)
         start_time = datetime.datetime.now()
      # sync data
      sync_sqlserver_data(config, debug)
      time.sleep(int(config['sync_gap']))

def main():
    #init variable
    config = ""
    debug = False
    reinitialize=False
    cancelsync=False
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True
        elif sys.argv[p] == "-reinitialize":
            reinitialize = True
        elif sys.argv[p] == "-cancelsync":
            cancelsync=True
        else:
            pass
    #process
    if cancelsync:
       db_cancelsync(get_config(config))
    else:
       sync(config, debug,reinitialize)

if __name__ == "__main__":
     main()
