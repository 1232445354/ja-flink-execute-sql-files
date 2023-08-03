-- ===========================================================
-- Author: yibo@jingan-inc.com
-- Create date: 2023-02-20
-- Description: 靖安科技机器狗数据-宝钢
-- ===========================================================

SET 'parallelism.default' = '1';
SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'pipeline.name' = 'ja-device-dog-track-rt';
SET 'table.exec.state.ttl' = '360000000';
-- SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
SET 'execution.checkpointing.interval' = '60000';
SET 'state.checkpoints.dir' = 's3://flink/ja-device-dog-track-rt-checkpoints' ;

-- 机器狗数据(Source:kafka)
drop table if exists ja_device_index_kafka;
create table ja_device_index_kafka (
    recordId            string    comment '行为id or 事件id',
    type                string    comment 'ELECTRIC 电量 POSITION 位置 AGL_ALTITUDE 高度',
    `value`             string    comment '值',
    locationType        string    comment '位置类型',
    reportDeviceId      string    comment '上报的设备id',
    reportDeviceType    string    comment '上报的设备类型',
    reportTimeStamp     bigint    comment '上报时间戳',
    proctime as PROCTIME()			    -- 维表关联的时间函数
) WITH (
  'connector' = 'kafka',
  'topic' = 'ja_device_index',
  'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
  'properties.group.id' = 'ja-device-index-track-rt',
  -- 'scan.startup.mode' = 'latest-offset',
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp-millis' = '0',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);


-- 建立映射mysql的表（为了查询用户名称）(Source:mysql)
drop table if exists device_mysql;
create table device_mysql (
    id	             int,
    device_id	     varchar(20),
    username	     varchar(32),
    primary key (id) NOT ENFORCED
)with (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
    'username' = 'root',
    'password' = 'jingansi110',
    'table-name' = 'device',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '3600s',
    'lookup.max-retries' = '3'
);


-- 建立映射mysql的表（为了查询组织id）(Source:mysql)
drop table if exists ja_device_index_mysql_user;
create table ja_device_index_mysql_user (
     user_id	int,
     username	varchar(255),
     password	varchar(255),
     name	    varchar(255),
     phone	    varchar(255),
     id_card	varchar(255),
     group_id	varchar(255),
     create_at	timestamp,
     update_at	timestamp,
     remark	    varchar(255),
    primary key (user_id) NOT ENFORCED
)with (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja-4a?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
    'username' = 'root',
    'password' = 'jingansi110',
    'table-name' = 'users',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '3600s',
    'lookup.max-retries' = '3'
);


-- 设备狗数据创建flink映射doris的表-5s取出一个点位(Sink:doris)
drop table if exists ja_device_index_doris;
create table ja_device_index_doris (
  device_id                 string  comment '上报的设备id',
  acquisition_time_format   string  comment '格式化时间',
  acquisition_time          bigint  comment '上报时间戳',
  device_type               string  comment '上报的设备类型',
  lng_02					string	comment '经度—高德坐标系、火星坐标系',
  lat_02					string	comment '纬度—高德坐标系、火星坐标系',
  lng_84					string	comment '经度—84坐标系',
  lat_84					string	comment '纬度—84坐标系',
  record_id               	string  comment '行为id',
  username                	string  comment '设备用户',
  group_id                	string  comment '组织id',
  `type`                	string  comment 'POSITION 位置',
  location_type             string  comment '位置类型',
  start_time                string  comment '开始时间（开窗）',
  end_time                  string  comment '结束时间（开窗）',
  update_time               string  comment '更新插入时间（数据入库时间'
)WITH (
'connector' = 'doris',
'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:30030',
'table.identifier' = 'device.ja_device_track_rt',
'username' = 'admin',
'password' = 'Jingansi@110',
'doris.request.tablet.size'='1',
'doris.request.read.timeout.ms'='30000',
'sink.batch.size'='100000',
'sink.batch.interval'='2s'
);


-- 设备狗数据创建flink映射doris的表-全量数据(Sink:doris)
drop table if exists ja_device_index_all_doris;
create table ja_device_index_all_doris (
  device_id                 string  comment '上报的设备id',
  acquisition_time_format   string  comment '格式化时间',
  acquisition_time          bigint  comment '上报时间戳',
  device_type               string  comment '上报的设备类型',
  `value`                   string  comment '值',
  record_id               	string  comment '行为id',
  username                	string  comment '设备用户',
  group_id                	string  comment '组织id',
  `type`                	string  comment 'POSITION 位置',
  location_type             string  comment '位置类型',
  update_time               string  comment '更新插入时间（数据入库时间'
)WITH (
'connector' = 'doris',
'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:30030',
'table.identifier' = 'device.ja_device_track_all_rt',
'username' = 'admin',
'password' = 'Jingansi@110',
'doris.request.tablet.size'='1',
'doris.request.read.timeout.ms'='30000',
'sink.batch.size'='100000',
'sink.batch.interval'='2s'
);


---------------

-- 数据处理

---------------

-- 对kafka的数据与维表数据进行关联
drop view if exists tmp_ja_device_index_01;
create view tmp_ja_device_index_01 as
select
    t1.recordId          as   record_id,
    t1.type               ,
	`value`,
    t1.locationType      as location_type,
    t1.reportDeviceId    as device_id,
    t1.reportDeviceType  as device_type,
    t1.reportTimeStamp 	 as acquisition_time,
	from_unixtime(t1.reportTimeStamp/1000) as acquisition_time_format,
	t2.username as username,
	t3.group_id as group_id
from ja_device_index_kafka as t1
left join device_mysql
FOR SYSTEM_TIME AS OF t1.proctime as t2
on t1.reportDeviceId = t2.device_id
left join ja_device_index_mysql_user
FOR SYSTEM_TIME AS OF t1.proctime as t3
on t2.username = t3.username;


-- 对kafka来的数据进行筛选（位置类型的数据,时间戳不为空的数据）
-- 对数据的时间戳（单位s）进行对10取余
drop view if exists tmp_ja_device_index_02;
create view tmp_ja_device_index_02 as
select
	record_id,									-- 记录id
    type               ,
  	split_index(`value`,',',0) as lng_02,
	split_index(`value`,',',1) as lat_02,
    location_type,								-- 位置类型
    device_id,									-- 设备id
    device_type,								-- 设备类型
    acquisition_time,							-- 采集时间戳
	username			,
	group_id			,
	acquisition_time_format,
	acquisition_time/1000%10 as acquisition_time_remainder,			-- 余数
	acquisition_time/1000/10*10 as acquisition_time_delete_remainder	-- 去除余数
from tmp_ja_device_index_01
where type = 'POSITION';


 -- 判断余数是1、2、3、4、5还是0、6、7、8、9、10
drop view if exists tmp_ja_device_index_03;
create view tmp_ja_device_index_03 as
 select
    device_id   	    ,
    record_id   	    ,
    `type`              ,
    location_type		,
    device_type			,
	lng_02				,
    lat_02				,
	username			,
	group_id			,
	acquisition_time_format,
    acquisition_time,
	acquisition_time_remainder,			-- 余数
	acquisition_time_delete_remainder,	-- 去除余数
	if(acquisition_time_remainder>=1 and acquisition_time_remainder<=5,acquisition_time_delete_remainder+5,acquisition_time_delete_remainder+10) as final_acquisition_time
from tmp_ja_device_index_02;


 -----------------------

 -- 数据插入

 -----------------------

 begin statement set;

  -- 数据5s取出一条进行入库
 insert into ja_device_index_doris
 select
  device_id,
  from_unixtime(final_acquisition_time) as acquisition_time_format,
  final_acquisition_time as acquisition_time,
  device_type,
  lng_02,
  lat_02,
  cast(null as varchar) as lng_84,
  cast(null as varchar) as lat_84,
  record_id,
  username,
  group_id,
  type,
  location_type,
  from_unixtime(final_acquisition_time - 5) as start_time,
  from_unixtime(acquisition_time/1000) as end_time,
  from_unixtime(unix_timestamp()) as update_time
 from tmp_ja_device_index_03;


  -- 全量数据入库
 insert into ja_device_index_all_doris
 select
  device_id,
  acquisition_time_format,
  acquisition_time,
  device_type,
  `value`,
  record_id,
  username,
  group_id,
  type,
  location_type,
  from_unixtime(unix_timestamp()) as update_time
 from tmp_ja_device_index_01;

end;