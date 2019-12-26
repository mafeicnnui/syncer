

**一、概述**

------------


   功能：Mysql向Mysql数据同步工具，可实现全量、增量同步。基于PYTHON3语言开发，支持MongoDB表全量、增量方式同步至MySQL数据库中，基于时间戳实现。

   1.1 安装依赖

   pip install pymysql

   1.3 启动同步

   python3 sync_mysql2mysql.py -conf sync_mysql2mysql.ini -debug

**二、配置文件**

------------
[sync]
sync_db_mysql_sour=10.28.8.233:3306:hopsonone_flow_cd_real_time:sync:sync

sync_db_mysql_desc=10.28.8.234:3306:hopsonone_flow_cd_real_time:sync:sync

sync_table=dbo_summary_thirty:modifytime:1

batch_size=2000

batch_size_incr=200  

sync_gap=30  

mail_title=成都实时客流同步情况[PROD/BI]=>[MySQL->MySQL]  

send_mail_user=190343@lifeat.cn  

send_mail_pass=Hhc5HBtAuYTPGHQ8  

acpt_mail_user=190343@lifeat.cn  

send_mail_gap=1800  

sync_type=flow_real_cd  

sync_col_val=108  

sync_time_type=hour  



 2.2 参数说明：

------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| sync_db_mysql_sour |  配置 MySQL源数据库连接，格式：IP:PORT:DB:USER:PASS |
| sync_db_mysql_desc  | 配置 MySQL目标数据库连接，格式：IP:PORT:DB:USER:PASS  |
| sync_table  | 配置同步表，格式:TNAME:COL:TIME,COL:指的是时间列，TIME：指同步最近多长时间的数据，值依赖参数sync_time_type，TNAME为必填项，其它两项为空表示全量，有值表示增量。  |
| sync_time_type  |配置同步时间类型，值为：day,hour,min   |
| batch_size  |全量同步批大小，必须为正数，全量同步时多少数据打包在一起发送  |
| batch_size_incr  | 增量同步批大小，必须为正数  |
| sync_gap |  配置每次同步后休眠的秒数 |
| mail_title  | 邮件标题  |
| send_mail_user  |发件人用户 |
| send_mail_pass  |发件人密码  |
| acpt_mail_user  |收件人用户 |
| send_mail_gap  | 发送邮件间隔，单位：秒 |
| sync_time_type  | 配置同步时间类型，值为：day,hour,min  |

