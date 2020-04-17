#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Software: PyCharm
# 功能：MSSQL->MSSQL数据同步工具
import sys,time
import traceback
import configparser
import warnings
import pymssql
import pymysql
import datetime
import hashlib
import smtplib
from email.mime.text import MIMEText

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

def send_mail25(p_from_user,p_from_pass,p_to_user,p_title,p_content):
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

def exception_info():
    e_str=traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]

def get_now():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

def get_ds_sqlserver(ip,port,service,user,password):
    conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service,charset='utf8')
    return conn

def get_sync_time_type_name(sync_time_type):
    if sync_time_type=="day":
       return '天'
    elif sync_time_type=="hour":
       return '小时'
    elif sync_time_type=="min":
       return '分'
    else:
       return ''

def get_init_tables(config):
    db  = config['db_sqlserver_sour']
    cr  = db.cursor()
    sql = '''Select OBJECT_SCHEMA_NAME(id), Name
              FROM SysObjects 
              Where XType='U' 
                and CHARINDEX(upper(OBJECT_SCHEMA_NAME(id)+'.'+name),upper('{0}')) >0 
              Order BY Name
          '''.format(config['sync_table'])
    cr.execute(sql)
    rs = cr.fetchall()
    v=''
    for i in rs:
      v=v+'{0}.{1}::,'.format(i[0].lower(),i[1].lower())
    cr.close()
    db.commit()
    print('v=',v)
    return v


def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    sync_server_sour                  = cfg.get("sync","sync_db_server_sour")
    sync_server_dest                  = cfg.get("sync","sync_db_server_dest")
    config['sync_table']              = cfg.get("sync", "sync_table").lower()
    config['batch_size']              = cfg.get("sync", "batch_size")
    config['batch_size_incr']         = cfg.get("sync", "batch_size_incr")
    config['sync_gap']                = cfg.get("sync", "sync_gap")
    config['send_user']               = cfg.get("sync", "send_mail_user")
    config['send_pass']               = cfg.get("sync", "send_mail_pass")
    config['acpt_user']               = cfg.get("sync", "acpt_mail_user")
    config['mail_gap']                = cfg.get("sync", "send_mail_gap")
    config['mail_title']              = cfg.get("sync", "mail_title")
    config['sync_type']               = cfg.get("sync", "sync_type")
    config['sync_time_type']          = cfg.get("sync", "sync_time_type")
    config['sync_time_type_name']     = get_sync_time_type_name(config['sync_time_type'])
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
    config['db_sqlserver_sour_ip']         = db_sour_ip
    config['db_sqlserver_sour_port']       = db_sour_port
    config['db_sqlserver_sour_service']    = db_sour_service
    config['db_sqlserver_sour_user']       = db_sour_user
    config['db_sqlserver_sour_pass']       = db_sour_pass
    config['db_sqlserver_dest_ip']         = db_dest_ip
    config['db_sqlserver_dest_port']       = db_dest_port
    config['db_sqlserver_dest_service']    = db_dest_service
    config['db_sqlserver_dest_user']       = db_dest_user
    config['db_sqlserver_dest_pass']       = db_dest_pass
    config['db_sqlserver_sour_string']     = db_sour_ip+':'+db_sour_port+'/'+db_sour_service
    config['db_sqlserver_dest_string']     = db_dest_ip+':'+db_dest_port+'/'+db_dest_service
    config['db_sqlserver_sour']            = get_ds_sqlserver(db_sour_ip,db_sour_port ,db_sour_service,db_sour_user,db_sour_pass)
    config['db_sqlserver_sour2']           = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user,db_sour_pass)
    config['db_sqlserver_dest']            = get_ds_sqlserver(db_dest_ip,db_dest_port ,db_dest_service,db_dest_user,db_dest_pass)
    return config

def check_sqlserver_tab_sync(db,tname):
   cr=db.cursor()
   sql="select count(0) from {0}".format(tname)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_sqlserver_tab_exists(db,tname):
   cr=db.cursor()
   sql="select count(0) from sysobjects where id=object_id('{0}')".format(tname)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_sqlserver_schema_exists(db,schema):
   cr=db.cursor()
   sql="select count(0) from sys.schemas where name='{0}'".format(schema)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_sqlserver_tab_exists_pk(config,tab):
   table_schema=tab.split('.')[0]
   table_name  =tab.split('.')[1]
   db=config['db_sqlserver_sour']
   cr=db.cursor()
   '''
   sql = """select
             count(0)
            from syscolumns col, sysobjects obj
            where col.id=obj.id and obj.id=object_id('{0}')
            and  (select count(0)
                  from  dbo.sysindexes si
                      inner join dbo.sysindexkeys sik on si.id = sik.id and si.indid = sik.indid
                      inner join dbo.syscolumns sc on sc.id = sik.id    and sc.colid = sik.colid
                      inner join dbo.sysobjects so on so.name = si.name and so.xtype = 'pk'
                  where  sc.id = col.id  and sc.colid = col.colid)=1
         """.format(table_schema,table_name)
   '''

   sql = """SELECT count(0)
             FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA='{0}' and TABLE_NAME='{1}' and upper(constraint_name) like 'PK%'
         """.format(table_schema,table_name)
   print('check_sqlserver_tab_exists_pk=',sql)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())


def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(40,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(40,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def format_sql(v_sql):
    return v_sql.replace("'","''")

def get_tab_columns(config,tab):
    table_schema = tab.split('.')[0]
    table_name   = tab.split('.')[1]
    cr=config['db_sqlserver_sour'].cursor()
    '''
    sql="""select col.name
           from syscolumns col, sysobjects obj
           where col.id=obj.id 
             and obj.id=object_id('{0}')
           order by isnull((SELECT  'Y'
                            FROM  dbo.sysindexes si
                            INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                            inner join dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                            where sik.id=obj.id and sik.colid=col.colid),'N') desc,col.colid    
        """.format(tab)
    '''
    sql = """SELECT column_name
                 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA='{0}' and TABLE_NAME='{1}'
             """.format(table_schema, table_name)
    cr.execute(sql)
    rs=cr.fetchall()
    s1=""
    for i in range(len(rs)):
      s1=s1+rs[i][0].lower()+','
    cr.close()
    return s1[0:-1]

def get_tab_header(config,tab):
    cr=config['db_sqlserver_sour'].cursor()
    sql="select top 1 * from {0}".format(tab)
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+tab.lower()+"("
    s2=" values "
    '''for i in range(len(desc)):
      s1=s1+desc[i][0].lower()+','
    '''
    s1=s1+get_sync_table_cols(config,tab)+") "
    #s1=s1+config['sync_col_name']+")"
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
                    WHERE A.name = '{0}'  and c.value is not null        
                   """.format(tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    return rs_source[0]

def sync_sqlserver_col_comments(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc   = config['db_mysql']
    cr_desc   = db_desc.cursor()
    v_comment ="""SELECT                                
                        case when t.name ='numeric' then
                          'alter table '+lower(A.name)+' modify column '+lower(B.name)+' '+t.name+'('+
                            cast(COLUMNPROPERTY(b.id,b.name,'PRECISION') AS varchar)+','+
                            CAST(isnull(COLUMNPROPERTY(b.id,b.name,'Scale'),0) as varchar)	   
                           +') comment '''+CAST(c.value as varchar)+''''
                        when t.name in('nvarchar','varchar','int') then
                          'alter table '+lower(A.name)+' modify column '+lower(B.name)+' '+t.name+'('+
                            cast(COLUMNPROPERTY(b.id,b.name,'PRECISION') AS varchar)+') comment '''+CAST(c.value as varchar)+''''
                        else
                          'alter table '+lower(A.name)+' modify column '+lower(B.name)+' '+t.name+' comment '''+CAST(c.value as varchar)+''''
                        end
                FROM sys.tables A
                INNER JOIN syscolumns B ON B.id = A.object_id
                left join  systypes t   on b.xtype=t.xusertype
                LEFT JOIN sys.extended_properties C ON C.major_id = B.id AND C.minor_id = B.colid
                WHERE A.name = '{0}'  and c.value is not null        
               """.format(tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchall()
    for j in range(len(rs_source)):
        v_ddl_sql = rs_source[j][0]
        cr_desc.execute(v_ddl_sql)
    db_desc.commit()
    cr_desc.close()

def check_sync_sqlserver_tab_comments(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_comment ="""select count(0)  from sys.extended_properties A  
                  where A.major_id=object_id('{0}')  and a.name='{0}'""".format(tab,tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    return rs_source[0]

def sync_sqlserver_tab_comments(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc   = config['db_mysql']
    cr_desc   = db_desc.cursor()
    v_comment ="""select 
                   'alter table '+lower(a.name)+' comment '''+cast(a.value as varchar)+''''
                  from sys.extended_properties A  
                  where A.major_id=object_id('{0}')
                    and a.name='{0}'""".format(tab,tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    if len(rs_source)>0:
        v_ddl_sql = rs_source[0]
        cr_desc.execute(v_ddl_sql)
        cr_desc.close()

def f_get_table_ddl(config,tab):
    db_source = config['db_sqlserver_sour']
    cr_source = db_source.cursor()
    v_sql     ="""SELECT       									
                    a.colorder 字段序号,
                    a.name 字段名,
                    b.name 类型,
                    COLUMNPROPERTY(a.id,a.name,'PRECISION') as 长度,
                    isnull(COLUMNPROPERTY(a.id,a.name,'Scale'),0) as 小数位,
                    (case when (SELECT count(*)
                        FROM sysobjects
                        WHERE (name in
                                (SELECT name
                                FROM sysindexes
                                WHERE (id = a.id) AND (indid in
                                         (SELECT indid
                                        FROM sysindexkeys
                                        WHERE (id = a.id) AND (colid in
                                                  (SELECT colid
                                                   FROM syscolumns
                                                  WHERE (id = a.id) AND (name = a.name))))))) AND
                            (xtype = 'PK'))>0 then '√' else '' end) 主键,
                            a.isnullable AS 是否允许非空,
                            (select name from sys.identity_columns x where x.object_id=d.id and x.column_id=a.colid) as 是否自增
                    FROM  syscolumns  a 
                    left join systypes b    on a.xtype=b.xusertype
                    inner join sysobjects d on a.id=d.id  and  d.xtype='U' and  d.name<>'dtproperties'
                    left join syscomments e on a.cdefault=e.id
                    left join sys.extended_properties g on a.id=g.major_id AND a.colid = g.major_id
                    where d.id=object_id('{0}') 
                    order by a.id,a.colorder""".format(tab)
    cr_source.execute(v_sql)
    rs=cr_source.fetchall()
    v_cre_tab= 'create table '+tab+'(';
    for i in range(len(rs)):
        v_name     = rs[i][1]
        v_type     = rs[i][2]
        v_len      = str(rs[i][3])
        v_scale    = str(rs[i][4])
        v_null     = str(rs[i][6])
        v_identify = str(rs[i][7])

        if v_null == '0':
           v_null =' not null'
        else:
           v_null =''

        if v_identify !='None':
           v_identify = ' IDENTITY(1,1)'
        else:
           v_identify = ''

        if v_type in('int','date','time','bigint','datetime','bit','tinyint','smallint','money','image','timestamp','datetime2','text','xml','smalldatetime'):
           v_cre_tab=v_cre_tab+'   ['+v_name+']    '+v_type+v_identify+v_null+','
        elif v_type in('numeric','decimal'):
           v_cre_tab = v_cre_tab + '   [' + v_name + ']    ' + v_type +'('+ v_len+','+ v_scale+') '+v_identify+v_null+' ,'
        else:
           if v_len=='-1':
              v_cre_tab = v_cre_tab + '   [' + v_name + ']    ' + v_type + '(max)  '+v_identify+v_null+' ,'
           else:
              v_cre_tab = v_cre_tab + '   [' + v_name + ']    ' + v_type + '(' + v_len + ')  ' + v_identify + v_null + ' ,'

    return v_cre_tab[0:-1]+')'

def sync_sqlserver_ddl(config,debug):
    db_source = config['db_sqlserver_sour']
    cr_source = db_source.cursor()
    db_desc   = config['db_sqlserver_dest']
    cr_desc   = db_desc.cursor()
    #for i in config['sync_table'].split(","):
    for i in get_init_tables(config).split(","):
        tab=i.split(':')[0]
        cr_source.execute("""select id,
                                    OBJECT_SCHEMA_NAME(id) as schema_name, 
                                    OBJECT_NAME(id) as table_name,
                                    DB_NAME() as db_name,
                                    OBJECT_SCHEMA_NAME(id)+'.'+OBJECT_NAME(id) as full_table_name
                             from sysobjects 
                             where xtype='U' and id=object_id('{0}') order by name""".format(tab))
        rs_source = cr_source.fetchall()
        for j in range(len(rs_source)):
            tab_name      = rs_source[j][2].lower()
            schema_name   = rs_source[j][1].lower()
            tab_prefix    = (str(rs_source[j][1]) + '.').lower()
            full_tab_name = rs_source[j][4].lower()
            if check_sqlserver_tab_exists_pk(config,tab)==0:
                print("DB:{0},Table:{1} not exist primary!".format(config['db_sqlserver_sour_string'],full_tab_name))

                if check_sqlserver_schema_exists(config['db_sqlserver_dest'], schema_name) == 0:
                    v_cre_sql ='create schema {0}'.format(schema_name)
                    cr_desc.execute(v_cre_sql)
                    db_desc.commit()
                    print("Schema:{0} creating success!".format(schema_name))

                if check_sqlserver_tab_exists(config['db_sqlserver_dest'], tab) == 0:
                    v_cre_sql = f_get_table_ddl(config, full_tab_name)
                    print(v_cre_sql)
                    cr_desc.execute(v_cre_sql)
                    db_desc.commit()
                    print("Table:{0} creating success!".format(full_tab_name))
                    db_desc.commit()
                else:
                    print('Table:{0} already exists,skip sync!'.format(tab))
            else:

                if check_sqlserver_schema_exists(config['db_sqlserver_dest'], schema_name) == 0:
                    v_cre_sql ='create schema {0}'.format(schema_name)
                    cr_desc.execute(v_cre_sql)
                    db_desc.commit()
                    print("Schema:{0} creating success!".format(schema_name))

                if check_sqlserver_tab_exists(config['db_sqlserver_dest'], tab) == 0:
                   v_cre_sql = f_get_table_ddl(config,full_tab_name)
                   print(v_cre_sql)
                   cr_desc.execute(v_cre_sql)
                   db_desc.commit()
                   print("Table:{0} creating success!".format(full_tab_name))
                   v_con_sql='alter table {0} add constraint pk_{1} primary key ({2})'.\
                              format(full_tab_name,full_tab_name.replace('.','_'),get_sync_table_pk_names(config, full_tab_name))
                   print('pk=', v_con_sql)
                   cr_desc.execute(v_con_sql)
                   print("Table:{0} add primary key {1} success!".format(full_tab_name,get_sync_table_pk_names(config, full_tab_name)))
                   db_desc.commit()
                else:
                   print('Table:{0} already exists,skip sync!'.format(tab))

    cr_source.close()
    cr_desc.close()

def get_sync_table_total_rows(db,tab,v_where):
    cr_source = db.cursor()
    v_sql="select count(0) from {0} with(nolock) {1}".format(tab,v_where)
    #print('get_sync_table_total_rows=',v_sql)
    cr_source.execute(v_sql)
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]


def get_sync_table_pk_names(config,tab):
    table_schema = tab.split('.')[0]
    table_name   = tab.split('.')[1]
    cr_source    = config['db_sqlserver_sour'].cursor()
    v_col = ''
    v_sql = """
              SELECT col.name
                 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE con ,syscolumns col
                WHERE con.TABLE_SCHEMA='{0}' and con.TABLE_NAME='{1}' and upper(con.CONSTRAINT_NAME)  like 'PK%'
                  and col.id=object_id('{2}')
                  and col.name=con.COLUMN_NAME   order by col.colid
            """.format(table_schema, table_name,tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col=v_col+'['+i[0]+'],'
    cr_source.close()
    return v_col[0:-1]

def get_sync_table_cols(config,tab):
    table_schema = tab.split('.')[0]
    table_name   = tab.split('.')[1]
    db_source = config['db_sqlserver_dest']
    cr_source = db_source.cursor()
    v_col=''
    v_sql="""select col.name
              from syscolumns col, sysobjects obj
              where col.id=obj.id and  obj.id=object_id('{0}')
               order by col.colid
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col=v_col+'['+i[0]+'],'
    cr_source.close()
    return v_col[0:-1]

def get_sync_table_pk_vals(config,tab):
    db_source = config['db_sqlserver_sour']
    cr_source = db_source.cursor()
    v_col=''
    v_sql="""select col.name
              from syscolumns col, sysobjects obj
              where col.id=obj.id and  obj.id=object_id('{0}')
               and (SELECT  1
                    FROM dbo.sysindexes si
                        INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                        INNER JOIN dbo.syscolumns sc ON sc.id = sik.id    AND sc.colid = sik.colid
                        INNER JOIN dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                    WHERE  sc.id = col.id  AND sc.colid = col.colid)=1 order by col.colid
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + "CONVERT(varchar(100)," + i[0] + ", 20)+" + "\'^^^\'" + "+"
    cr_source.close()
    return v_col[0:-7]

def get_sync_where(pk_cols,pk_vals):
    v_where=''
    for i in range(len(pk_cols.split(','))):
        v_where=v_where+pk_cols.split(',')[i]+"='"+pk_vals.split('^^^')[i]+"' and "
    return v_where[0:-4]

def get_sync_where_incr(tab,config):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = 'where {0} >=DATEADD(DAY,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'hour':
        v = 'where {0} >=DATEADD(HOUR,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'min':
        v = 'where {0} >=DATEADD(MINUTE,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    else:
        v = ''
    if tab.split(':')[1]=='':
       return ''
    else:
       return v

def get_sync_where_incr_rq(tab,config,currq):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = "where {0} >=DATEADD(DAY,-{1},'{2}')".format(v_rq_col, v_expire_time,currq)
    elif config['sync_time_type'] == 'hour':
        v = "where {0} >=DATEADD(HOUR,-{1},'{2}')".format(v_rq_col, v_expire_time,currq)
    elif config['sync_time_type'] == 'min':
        v = "where {0} >=DATEADD(MINUTE,-{1},'{2}')".format(v_rq_col, v_expire_time,currq)
    else:
        v = ''
    if tab.split(':')[1]=='':
       return ''
    else:
       return v

def check_tab_identify(tab,config):
    db_source = config['db_sqlserver_sour2']
    cr_source = db_source.cursor()
    cr_source.execute("select count(0) from sys.identity_columns x where x.object_id = object_id('{0}')".format(tab))
    rs_source=cr_source.fetchone()
    cr_source.close()
    return rs_source[0]

def sync_sqlserver_init(config,debug):
    config_init = {}
    #for i in config['init_tables'].split(","):
    for i in get_init_tables(config).split(","):
        tab=i.split(':')[0]
        config_init[tab] = False
        if check_sqlserver_tab_exists(config['db_sqlserver_dest'],tab)>0 \
               and check_sqlserver_tab_sync(config['db_sqlserver_dest'],tab)==0:
                    #and check_sqlserver_tab_exists_pk(config,tab)>0:
            print('Initiate Table {0}...'.format(tab))
            #write init dict
            config_init[tab] = True
            #start first sync data
            i_counter        = 0
            start_time       = datetime.datetime.now()
            n_tab_total_rows = get_sync_table_total_rows(config['db_sqlserver_sour'],tab,'')
            ins_sql_header   = get_tab_header(config,tab)
            v_tab_cols       = get_tab_columns(config,tab)
            v_pk_name        = get_sync_table_pk_names(config,tab)
            n_batch_size     = int(config['batch_size'])
            db_source        = config['db_sqlserver_sour']
            cr_source        = db_source.cursor()
            db_desc          = config['db_sqlserver_dest']
            cr_desc          = db_desc.cursor()
            v_sql            = "select * from {0} with(nolock)".format(tab)
            cr_source.execute(v_sql)
            rs_source = cr_source.fetchmany(n_batch_size)
            while rs_source:
                v_batch_sql= ''
                v_sql    = ''
                if check_tab_identify(tab,config)>0:
                   v_batch_sql  = 'begin\nset identity_insert {0} ON\n'.format(tab)
                else:
                   v_batch_sql = 'begin\n'.format(tab)

                for i in range(len(rs_source)):
                    rs_source_desc = cr_source.description
                    ins_val = ''
                    for j in range(len(rs_source[i])):
                        col_type = str(rs_source_desc[j][1])
                        if  rs_source[i][j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type == "1":  #varchar,date
                            ins_val = ins_val + "'"+format_sql(str(rs_source[i][j])) + "',"
                        elif col_type == "5":  #int,decimal
                            ins_val = ins_val + "'" + str(rs_source[i][j])+ "',"
                        elif col_type == "4":  #datetime
                            ins_val = ins_val + "'" + str(rs_source[i][j]).split('.')[0] + "',"
                        elif col_type == "3":  # bit
                            if str(rs_source[i][j]) == "True":  # bit
                                ins_val = ins_val + "'" + "1" + "',"
                            elif str(rs_source[i][j]) == "False":  # bit
                                ins_val = ins_val + "'" + "0" + "',"
                            else:  # bigint ,int
                                ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                        elif col_type == "2":  # timestamp
                            ins_val = ins_val + "null,"
                        else:
                            ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"

                    v_sql = ins_sql_header +'('+ins_val[0:-1]+')'
                    v_batch_sql = v_batch_sql+v_sql+'\n'

                if check_tab_identify(tab,config)>0:
                   v_batch_sql  = v_batch_sql + 'set identity_insert {0} OFF\nend;'.format(tab)
                else:
                   v_batch_sql  = v_batch_sql + '\nend;'

                #noinspection PyBroadException
                try:
                  cr_desc.execute(v_batch_sql)
                  i_counter = i_counter +len(rs_source)
                  rs_source = cr_source.fetchmany(n_batch_size)
                except:
                  print(traceback.format_exc())
                  print(v_batch_sql)
                  sys.exit(0)

                print("\rTime:{0},Table:{1},Total rec:{2},Process rec:{3},Complete:{4}%,elapsed time:{5}s"
                      .format(get_time(),tab,n_tab_total_rows, i_counter,
                              round(i_counter / n_tab_total_rows * 100,2),str(get_seconds(start_time))), end='')
            if n_tab_total_rows == 0:
               print("Table:{0},Total rec:0,skip init sync!".format(tab))
            db_desc.commit()
            print('')
    return config_init

def get_pk_vals_sqlserver(config,ftab):
    db_source  = config['db_sqlserver']
    cr_source  = db_source.cursor()
    tab        = ftab.split(':')[0]
    v_pk_cols  = get_sync_table_pk_vals(config, tab)
    v_sql      = "select {0} from {1} with(nolock) {2}".format(v_pk_cols, tab,get_sync_where_incr(ftab))
    cr_source.execute(v_sql)
    rs_source  = cr_source.fetchall()
    l_pk_vals  =[]
    for i in list(rs_source):
        l_pk_vals.append(i[0])
    cr_source.close()
    return l_pk_vals


def check_sqlserver_exists_pk(db,ftab,v_where):
    cr       = db.cursor()
    v_tab    = ftab.split(':')[0]
    v_sql    = 'select count(0) from {0} where {1}'.format(v_tab,v_where)
    #print('check_mysql_exists_pk=',v_sql)
    cr.execute(v_sql)
    rs=cr.fetchone()
    #print('rs=',rs)
    if rs[0]>0 :
       return True
    else:
       return False

def sync_sqlserver_data_pk(config,ftab,config_init):
    #start sync dml data
    tab = ftab.split(':')[0]
    if  check_sqlserver_tab_exists_pk(config, tab) > 0:
        i_counter        = 0
        v_where          = get_sync_where_incr(ftab,config)
        n_tab_total_rows = get_sync_table_total_rows(config['db_sqlserver_sour'],tab,v_where)
        ins_sql_header   = get_tab_header(config,tab)
        v_pk_names       = get_sync_table_pk_names(config, tab)
        v_pk_cols        = get_sync_table_pk_vals(config, tab)
        n_batch_size     = int(config['batch_size_incr'])
        db_source        = config['db_sqlserver_sour']
        cr_source        = db_source.cursor()
        db_desc          = config['db_sqlserver_dest']
        cr_desc          = db_desc.cursor()
        v_sql            = """select {0} as 'pk',{1} from {2} with(nolock) {3}
                           """.format(v_pk_cols,get_sync_table_cols(config,tab), tab,v_where)
        n_rows           = 0
        cr_source.execute(v_sql)
        rs_source        = cr_source.fetchmany(n_batch_size)
        start_time       = datetime.datetime.now()

        if ftab.split(':')[1]=='':
            print("Sync Table increment :{0} ...".format(ftab.split(':')[0]))
        else:
            print("Sync Table increment :{0} for In recent {1} {2}...".
                  format(ftab.split(':')[0], ftab.split(':')[2],config['sync_time_type']))

        while rs_source:
            v_sql_tmp = ''
            v_sql_ins = ''
            v_sql_ins_batch =''
            v_sql_del_batch =''

            if check_tab_identify(tab, config) > 0:
                v_sql_ins_batch = 'begin\nset identity_insert {0} ON\n'.format(tab)
                v_sql_del_batch = 'begin\nset identity_insert {0} ON\n'.format(tab)
            else:
                v_sql_ins_batch = 'begin\n'
                v_sql_del_batch = 'begin\n'

            n_rows=n_rows+len(rs_source)
            #print("\r{0},Scanning table:{1},{2}/{3} rows,elapsed time:{4}s...".
            #      format(get_time(),tab,str(n_rows),str(n_tab_total_rows),str(get_seconds(start_time))),end='')
            rs_source_desc = cr_source.description
            if len(rs_source) > 0:
                for r in list(rs_source):
                    v_sql_tmp = ''
                    for j in range(1, len(r)):
                        col_type = str(rs_source_desc[j][1])
                        if r[j] is None:
                            v_sql_tmp = v_sql_tmp + "null,"
                        elif col_type == "1":  # varchar,date
                            v_sql_tmp = v_sql_tmp + "'" + format_sql(str(r[j])) + "',"
                        elif col_type == "5":  # int,decimal
                            v_sql_tmp = v_sql_tmp + "'" + str(r[j]) + "',"
                        elif col_type == "4":  # datetime
                            v_sql_tmp = v_sql_tmp + "'" + str(r[j]).split('.')[0] + "',"
                        elif col_type == "3":  # bit
                            if str(r[j]) == "True":  # bit
                                v_sql_tmp = v_sql_tmp + "'" + "1" + "',"
                            elif str(r[j]) == "False":  # bit
                                v_sql_tmp = v_sql_tmp + "'" + "0" + "',"
                            else:  # bigint ,int
                                v_sql_tmp = v_sql_tmp + "'" + str(r[j]) + "',"
                        elif col_type == "2":  # timestamp
                            v_sql_tmp = v_sql_tmp + "null,"
                        else:
                            v_sql_tmp = v_sql_tmp + "'" + format_sql(str(r[j])) + "',"

                    v_where   = get_sync_where(v_pk_names, r[0])
                    v_sql_ins = ins_sql_header  + '(' + v_sql_tmp[0:-1]+ ')'
                    v_sql_del = "delete from {0} where {1}".format(tab,v_where)
                    v_sql_ins_batch = v_sql_ins_batch + v_sql_ins+'\n'
                    v_sql_del_batch = v_sql_del_batch + v_sql_del + '\n'

            if check_tab_identify(tab, config) > 0:
               v_sql_ins_batch = v_sql_ins_batch + 'set identity_insert {0} OFF\nend;'.format(tab)
               v_sql_del_batch = v_sql_del_batch + 'set identity_insert {0} OFF\nend;'.format(tab)
            else:
               v_sql_ins_batch = v_sql_ins_batch+'\nend;'
               v_sql_del_batch = v_sql_del_batch+'\nend;'

            try:
               cr_desc.execute(v_sql_del_batch)
               cr_desc.execute(v_sql_ins_batch)
               i_counter = i_counter + len(rs_source)
               rs_source = cr_source.fetchmany(n_batch_size)
            except:
               print(traceback.format_exc())
               print('v_sql_del_batch=', v_sql_del_batch)
               print('v_sql_ins_batch=', v_sql_ins_batch)
               sys.exit(0)

            print("\rTable:{0},Total :{1},Process ins:{2},Complete:{3}%,elapsed time:{4}s"
                  .format(tab,
                          n_tab_total_rows,
                          i_counter,
                          round((i_counter) / n_tab_total_rows * 100, 2),
                          str(get_seconds(start_time))))

        if n_tab_total_rows == 0:
            print("Table:{0},Total :0,skip increment sync!".format(tab))
        else:
            print("\tTable:{0},Total :{1},Process ins:{2},Complete:{3}%,elapsed time:{4}s"
                  .format(tab,
                          n_tab_total_rows,
                          i_counter,
                          round((i_counter) / n_tab_total_rows * 100, 2),
                          str(get_seconds(start_time))))
        db_desc.commit()

def sync_sqlserver_data(config,config_init):
    for v in config['sync_table'].split(","):
        sync_sqlserver_data_pk(config, v,config_init)

def sync(config,debug):
    #init dict
    config=get_config(config)
    start_mail_time = datetime.datetime.now()

    #print dict
    if debug:
       print_dict(config)

    print('sync table definition...')
    sync_sqlserver_ddl(config, debug)

    print('Init Sync...')
    config_init =sync_sqlserver_init(config, debug)
    sys.exit(0)
    #sync data
    while True:
      print('increment Sync...')
      sync_sqlserver_data(config,config_init)

      #sleeping
      print('Sleep {0}s...'.format(config['sync_gap']))
      time.sleep(int(config['sync_gap']))

      #exit program
      sys.exit(0)

def main():
    #init variable
    config = ""
    debug = False
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True
    #process
    sync(config, debug)

if __name__ == "__main__":
     main()
