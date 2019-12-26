

**一、概述**

------------


   功能：MongoDB向Hbase数据同步工具，可实现全量、增量同步。基于PYTHON3语言开发，支持MongoDB表全量、增量方式同步至Hbase数据库中，基于时间戳实现，Hbase数据库需要打开thrift服务。

   1.1 安装依赖

   pip install pymongo

   pip install happybase

   pip install python-dateutil


   1.2 Hbase开启thrift服务

   hbase thrift start-port:9090

   1.3 启动同步

   python3 sync_mongo2hbase.py -conf sync_mongo2hbase.ini -debug

**二、配置文件**

------------
[sync]
db_mongo=192.168.1.1:27016:test:uesr:password

db_hbase=192.168.1.12:9090

full_sync_gaps=7

sync_table=test1:updateTime:1,test2::

sync_time_type=day

batch_size=2000

batch_size_incr=200


 2.2 参数说明：

------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| db_mongo |  配置 MongoDB数据库连接，格式：IP:PORT:DB:USER:PASS |
| db_hbase  | 配置 Hbase数据库thrift服务连接方式 ，格式:IP:PORT  |
| full_sync_gaps  | 配置全量同步时每次查询最近几天的数据进行同步  |
| sync_table  | 配置同步表，格式:TNAME:COL:TIME,COL:指的是时间列，TIME：指同步最近多长时间的数据，值依赖参数sync_time_type，TNAME为必填项，其它两项为空表示全量，有值表示增量。  |
| sync_time_type  |配置同步时间类型，值为：day,hour,min   |
| batch_size  |全量同步批大小，必须为正数，全量同步时多少数据打包在一起发送hbase   |
| batch_size_incr  | 增量同步批大小，必须为正数  |

