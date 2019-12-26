

**一、概述**

------------


   功能：MongoDB向Kafka数据同步工具。基于PYTHON3.6语言开发，支持MongoDB表实时同步至kafka中，通过解析oplog实现。

   1.1 安装依赖

   pip install pymongo

   pip install kafka-python
   
   1.2 启动kafka服务

   bin/kafka-server-start.sh config/server.properties 

   1.3 启动同步

   python3 sync_mongo2kafka.py

**二、配置文件**

------------
MONGO_SETTINGS = {  

    "host"     : 'dds-2ze8179ceab498d41.mongodb.rds.aliyuncs.com',  
    
    "port"     : '3717',  
    
    "db"       : 'admin',  
    
    "user"     : 'root',  
    
    "passwd"   : 'Mongo-kkm!2019',  
    
    "replset"  : '',  
    
    "db_name"  : 'posB',  
    
    "tab_name" : 'saleDetail'
    
}

KAFKA_SETTINGS = {  

    "host"  : '172.17.194.79',  
    
    "port"  :  9092,  
    
    "topic" : 'sync_mongo2kafka_hopson'  
    
}  


 2.2 MONGO_SETTINGS 参数说明：


------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| host     |  MongoDB 数据库IP,复本集配置IP以逗号分隔 |
| port     | MongoDB 数据库PORT,复本集配置PORT以逗号分隔  |
| db       | MongoDB 认证数据库名  |
| user     |MongoDB 用户名  |
| passwd   |MongoDB 口令   |
| replset  |MongoDB复本集名称，为空时表示连接单实例   |
| db_name  | 定义监控的数据库名称  |
| tab_name | 定义监控的表名称  |

 2.3 KAFKA_SETTINGS 参数说明：


------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| host  | 配置 kafka 数据库IP    |
| port  | 配置 kafka 数据库PORT  |
| topic | 配置 kafka TOPIC名称   |



**三、配置示例**

------------  
单实例：  

MONGO_SETTINGS = {  

    "host"     : 'dds-2ze8179ceab498d41.mongodb.rds.aliyuncs.com',  
    
    "port"     : '3717',  
    
    "db"       : 'admin',  
    
    "user"     : 'root',  
    
    "passwd"   : 'Mongo-kkm!2019',  
    
    "replset"  : '',  
    
    "db_name"  : 'posB',  
    
    "tab_name" : 'saleDetail'
    
}  

复本集连接方式：

MONGO_SETTINGS = {
    "host"     : '172.17.194.79,172.17.129.195,172.17.129.194',  
    
    "port"     : '27016,27017,27018',  
    
    "db"       : 'admin',  
    
    "user"     : 'root',  
    
    "passwd"   : 'YxBfV0Q3ne6z6rny',  
    
    "replset"  : 'posb',  
    
    "db_name"  : 'posB',  
    
    "tab_name" : 'saleDetail',  
    
}
