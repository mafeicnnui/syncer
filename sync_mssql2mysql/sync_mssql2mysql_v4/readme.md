工具介绍：
1.SQLServer向MyQL数据库同步工具，支持DML操作，不支持DDL同步。SQLServer端不进写操作，有查询权限即可
2.首次同步会采用全量方式，以后采用增量方式，通过在Python端比较行串来判断数据是否变动
3.对于无主键表采用定时全量同步方式
4.对于大表也可以配置为采用定时全量方式同步

配置说明：

[sync]
#北京客流数据库配置
#MySQL数据源配置格式：IP:端口:数据库名:用户名:口令
sync_db_server=10.2.39.9:1433:database:user:pass

#SQLServer数据源配置格式：IP:端口:数据库名:用户名:口令
sync_db_mysql=10.2.39.40:3306:database:user:pass

#SQLServer需要同步表列表，以速号分隔，需要加schema_name前缀
sync_table=dbo.Summary_Month,dbo.Summary_Week,dbo.Summary_Day,dbo.Summary_Thirty,dbo.Traffic_Sites,dbo.Settings_Building

#首次初始化同步时会将同步SQL写入此目录下，方便测试
sync_dir=./sql/

#MySQL端同步前会在表中新增加该列
sync_col_name=market_id

#MyQL端同步时对新增加的列赋于默认值
sync_col_val=218

#增量同步时每次读取的批大小
batch_size=1000

#增量同步间隔
sync_gap=30

#全量同步表行数阀值，超过该行会采用全量同步
full_sync_rows=100000

#全量同步方式 ，有两个值：period周期同步，timing：定时同步
full_sync_method=period

#全量同步时定时同步时间
full_sync_time=16:30:00

#全量同步周期同步时间间隔
full_sync_period=300