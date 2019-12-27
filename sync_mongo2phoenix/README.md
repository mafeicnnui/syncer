

**一、概述**

------------


   功能：MongoDB向phoenix数据同步工具，可实现全量、增量同步。基于PYTHON3语言开发，于时间戳实现，phoenix需要启动queryserver服务。

   1.1 安装依赖

   pip install pymongo

   pip install phoenixdb

   pip install bson


   1.2 phoenix开启queryserver服务

   cd /usr/local/phoenix-5.0.0/bin
   python queryserver.py


   1.3 启动同步

   python3 sync_mongo2phoenix.py -conf sync_mongo2phoenix.ini -debug

**二、配置文件**

------------
[sync]
db_mongo=192.168.1.1:27016:test:uesr:password

db_phoenix=10.2.39.167:8765:hopsonone

sync_table=saleDetail:updateTime:3

sync_time_type=day

batch_size=2000

batch_size_incr=200

gather_rows=0


 2.2 参数说明：

------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| db_mongo |  配置 MongoDB数据库连接，格式：IP:PORT:DB:USER:PASS |
| db_phoenix  | 配置 Phoenix服务连接方式 ，格式:IP:PORT:DBNAME  |
| sync_table  | 配置同步表，格式:TNAME:COL:TIME,COL:指的是时间列，TIME：指同步最近多长时间的数据，值依赖参数sync_time_type，TNAME为必填项，其它两项为空表示全量，有值表示增量。  |
| sync_time_type  |配置同步时间类型，值为：day,hour,min   |
| batch_size  |全量同步批大小，必须为正数，全量同步时多少数据打包在一起发送hbase   |
| batch_size_incr  | 增量同步批大小，必须为正数  |

