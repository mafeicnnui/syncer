

**一、概述**

------------


   功能：MySQL向Hbase数据同步工具，可实现全量、增量同步。基于PYTHON3语言开发，支持MySQL表全量、增量方式同步至Hbase数据库中，基于时间戳实现，Hbase数据库需要打开thrift服务。

   1.1 安装依赖

   pip install pymysql

   pip install happybase

   1.2 Hbase开启thrift服务

   hbase thrift start-port:9090

   1.3 启动同步

   python3 sync_mysql2hbase.py -conf sync_mysql2hbase.ini -debug

**二、配置文件**

------------
[sync]
db_mysql=10.2.39.40:3306:sync:sync:sync

db_hbase=10.2.39.167:9090:hopsonone_cms

sync_table=t_sync_log::

sync_time_type=day

batch_size=200


 2.2 参数说明：

------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| db_mysql |  配置MySQL数据库连接，格式：IP:PORT:DB:USER:PASS |
| db_hbase  | 配置Hbase数据库thrift服务连接方式,格式:IP:PORT  |
| sync_table  | 配置同步表，格式:TNAME:COL:TIME,COL:指的是时间列，TIME：指同步最近多长时间的数据，值依赖参数sync_time_type，TNAME为必填项，其它两项为空表示全量，有值表示增量。  |
| sync_time_type  |配置同步时间类型，值为：day,hour,min   |
| batch_size  |全量同步批大小，必须为正数，全量同步时多少数据打包在一起发送hbase   |