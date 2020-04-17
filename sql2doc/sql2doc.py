#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys
import traceback
import os
import configparser
import time
import datetime
import pymssql
import xlwt
import warnings

def get_ds_sqlserver(ip,port,service,user,password):
    conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service,charset='utf8')
    return conn

def get_tab_rows(config,tab):
    cr = config['db'].cursor()
    cr = config['db'].cursor()
    sql= "select count(0) from [{0}].[dbo].[{1}]".format(config['db_service'],tab)
    #print("get_tab_rows=",initialize)
    cr.execute(sql)
    rs=cr.fetchone()
    cr.close()
    return rs[0]

def set_styles(fontsize):
    cell_borders   = xlwt.Borders()
    header_borders = xlwt.Borders()
    header_styles  = xlwt.XFStyle()
    cell_styles    = xlwt.XFStyle()
    # add table header style
    header_borders.left   = xlwt.Borders.THIN
    header_borders.right  = xlwt.Borders.THIN
    header_borders.top    = xlwt.Borders.THIN
    header_borders.bottom = xlwt.Borders.THIN
    header_styles.borders = header_borders
    header_pattern = xlwt.Pattern()
    header_pattern.pattern = xlwt.Pattern.SOLID_PATTERN
    header_pattern.pattern_fore_colour = 22
    # add font
    font = xlwt.Font()
    font.name = 'Times New Roman'
    font.bold = True
    font.size = fontsize
    header_styles.font = font
    #add alignment
    header_alignment = xlwt.Alignment()
    header_alignment.horz = xlwt.Alignment.HORZ_CENTER
    header_alignment.vert = xlwt.Alignment.VERT_CENTER
    header_styles.alignment = header_alignment
    header_styles.borders = header_borders
    header_styles.pattern = header_pattern
    #add col style
    cell_borders.left     = xlwt.Borders.THIN
    cell_borders.right    = xlwt.Borders.THIN
    cell_borders.top      = xlwt.Borders.THIN
    cell_borders.bottom   = xlwt.Borders.THIN
    # add alignment
    cell_alignment        = xlwt.Alignment()
    cell_alignment.horz   = xlwt.Alignment.HORZ_LEFT
    cell_alignment.vert   = xlwt.Alignment.VERT_CENTER
    cell_styles.alignment = cell_alignment
    cell_styles.borders   = cell_borders
    font2 = xlwt.Font()
    font2.name = 'Times New Roman'
    font2.size = fontsize
    cell_styles.font = font2
    return header_styles,cell_styles

def exp_xls(config):
    workbook        = xlwt.Workbook(encoding='utf8')
    worksheet       = workbook.add_sheet('非空表')
    worksheet2      = workbook.add_sheet('空表')
    header_styles, cell_styles=set_styles(15)
    cr = config['db'].cursor()
    cr_tab = config['db'].cursor()
    file_name = "客流数据数据字典_Summary_Traffic.xls"
    tab_sql= """select name from sysobjects 
                 where xtype='U'                   
                     and (name like 'Summary%' or name like 'Traffic%') order by name
             """
    exp_sql ="""SELECT       									
                a.colorder 序号,
                a.name 字段名,
                b.name 类型,
                COLUMNPROPERTY(a.id,a.name,'PRECISION') as 长度,
                isnull(COLUMNPROPERTY(a.id,a.name,'Scale'),0) as 小数位,
               (case when (SELECT count(0)
	             FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
	               WHERE TABLE_NAME=d.name and COLUMN_NAME=a.name and constraint_name like 'PK%'
				)>0 then '√' else '' end) 主键,
			   (case when (select COUNT(0)
				           from sys.foreign_key_columns
			   	            where parent_object_id = d.id  and parent_column_id=a.colid )>0 then '√' else '' end) as 外键,			   	   
			   (case when (select COUNT(0)
				           from sys.foreign_key_columns
				            where parent_object_id = d.id  and parent_column_id=a.colid )>0 then 
				   (select  object_name(referenced_object_id)+'('+col_name(referenced_object_id,referenced_column_id)+')'
				   from sys.foreign_key_columns
				   where parent_object_id = d.id  and parent_column_id=a.colid) else '' end) as  参考,
                case when isnullable=0 then  '' else '√' end as '是否为空',
                cast(g.value as varchar) as 描述
            FROM  syscolumns  a 
            left join systypes b    on a.xtype=b.xusertype
            inner join sysobjects d on a.id=d.id  and  d.xtype='U' and  d.name<>'dtproperties'
            left join syscomments e on a.cdefault=e.id
            left join sys.extended_properties g on a.id=g.major_id AND a.colid = g.minor_id
            where d.name='{0}'
            order by a.id,a.colorder
    """
    file_handle = open(file_name, 'w')
    cr_tab.execute(tab_sql)
    rs_tab=cr_tab.fetchall()
    row_data = 0
    row_no_data=0
    for i in list(rs_tab):
        n_rows=get_tab_rows(config,i[0])
        #print("i[0]=", i[0],"n_rows=",n_rows )
        if n_rows>0:
                print("Writing table {0} {1} rows into  data sheet...".format(i[0],str(n_rows)))
                #worksheet.write_merge(row, 0, row, 7, 'First Merge')
                worksheet.write(row_data, 0, "表名：{0} 行数:{1}"
                                .format(i[0].ljust(40,' '),str(n_rows).ljust(20,' ')), cell_styles)
                row_data = row_data + 1
                cr.execute(exp_sql.format(i[0]))
                rs   = cr.fetchall()
                desc = cr.description
                #output header
                for k in range(len(desc)):
                    worksheet.write(row_data, k, desc[k][0],header_styles)
                    if k==len(desc)-1:
                       worksheet.col(k).width = 8000
                    else:
                       worksheet.col(k).width = 4000
                #output cell contents
                row_data = row_data+1
                for i in rs:
                    for j in range(len(i)):
                        if i[j] is None:
                            worksheet.write(row_data, j,'',cell_styles)
                        else:
                            worksheet.write(row_data, j, str(i[j]),cell_styles)
                    row_data = row_data + 1
                #write 2 null rows
                worksheet.write(row_data, 0, '')
                row_data = row_data + 1
                worksheet.write(row_data, 0, '')
                row_data = row_data + 1
        else:
            print("Writing table {0} {1} rows into  no_data sheet...".format(i[0], str(n_rows)))
            # worksheet.write_merge(row, 0, row, 7, 'First Merge')
            worksheet2.write(row_no_data, 0, "表名：{0} 行数:{1}"
                            .format(i[0].ljust(40, ' '), str(n_rows).ljust(20, ' ')), cell_styles)
            row_no_data = row_no_data + 1
            cr.execute(exp_sql.format(i[0]))
            rs = cr.fetchall()
            desc = cr.description
            # output header
            for k in range(len(desc)):
                worksheet2.write(row_no_data, k, desc[k][0], header_styles)
                if k == len(desc) - 1:
                    worksheet2.col(k).width = 8000
                else:
                    worksheet2.col(k).width = 4000
            # output cell contents
            row_no_data = row_no_data + 1
            for i in rs:
                for j in range(len(i)):
                    if i[j] is None:
                        worksheet2.write(row_no_data, j, '', cell_styles)
                    else:
                        worksheet2.write(row_no_data, j, str(i[j]), cell_styles)
                row_no_data = row_no_data + 1
            # write 2 null rows
            worksheet2.write(row_no_data, 0, '')
            row_no_data = row_no_data + 1
            worksheet2.write(row_no_data, 0, '')
            row_no_data = row_no_data + 1
    workbook.save(file_name)
    config['db'].commit()
    cr.close()
    cr_tab.close()
    print("{0} export complete!".format(file_name))

def dict(fname):
    config = get_config(fname)
    print('exp SQLServer dict...')
    log(config)
    exp_xls(config)

def log(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def get_config(fname):
    t_cfg = {}
    config=configparser.ConfigParser()
    config.read(fname,encoding="utf-8-sig")
    server               = config.get("DICT","server")
    t_cfg['db_ip']       = server.split(':')[0]
    t_cfg['db_port']     = server.split(':')[1]
    t_cfg['db_service']  = server.split(':')[2]
    t_cfg['db_user']     = server.split(':')[3]
    t_cfg['db_pass']     = server.split(':')[4]
    t_cfg['db_string']   = t_cfg['db_ip']+':'+t_cfg['db_port']+'/'+t_cfg['db_service']
    t_cfg['db']          = get_ds_sqlserver(t_cfg['db_ip'], t_cfg['db_port'], t_cfg['db_service'], t_cfg['db_user'], t_cfg['db_pass'])
    return t_cfg

def main():
    #init variable 
    mode=""
    config=""
    debug=False
    warnings.filterwarnings("ignore")
    #get parameter from console    
    for p in range(len(sys.argv)): 
       if sys.argv[p] == "-conf":        
          config=sys.argv[p+1]
    #process
    dict(config)

if __name__ == "__main__":       
   main()

