-- ===========================================================
-- Author: 丁串串__弈博
-- Create date: 2022-08-01
-- Description: 无人机运行记录指标存储、执法仪轨迹数据回溯、设备最新状态查询、执法仪的实时点位位置
-- ===========================================================

SET 'parallelism.default' = '1';
SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'pipeline.name' = 'ja-device-index-all-rt';
SET 'table.exec.state.ttl' = '360000000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- 需要导入udf函数：geo-udf-1.0-SNAPSHOT-jar-with-dependencies
-- 自定义函数，转换经纬度
create function distance_udf as 'DistanceUdf';
-- 自定义函数模型
create function mapmatchudf as 'mapMatchServer.map_match' language python;


-- 回溯数据kafka的映射的表,无人机飞行记录数据映射kafka的表
drop table if exists ja_device_index_kafka;
create table ja_device_index_kafka (
    recordId            varchar(50)    comment '行为id or 事件id',
    type                varchar(50)    comment 'ELECTRIC 电量 POSITION 位置 AGL_ALTITUDE 高度',
    `value`             varchar(50)    comment '值',
    locationType        varchar(50)    comment '位置类型',
    reportDeviceId      varchar(50)    comment '上报的设备id',
    reportDeviceType    varchar(50)    comment '上报的设备类型',
    reportTimeStamp     bigint         comment '上报时间戳',
    rowtime as to_timestamp_ltz(reportTimeStamp,3),
    watermark for rowtime as rowtime - interval '3' second
) WITH (
  'connector' = 'kafka',
  'topic' = 'ja_device_index',
  'properties.bootstrap.servers' = '172.27.95.212:30091,172.27.95.212:30092,172.27.95.212:30093',
  'properties.group.id' = 'ja-device-index-track-rt',
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp-millis' = '1659369641000',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);


-- 建立映射mysql的表（为了查询用户名称）
drop table if exists device_mysql;
create table device_mysql (
    id	             int,
    device_id	     varchar(20),
    username	     varchar(32),
    primary key (id) NOT ENFORCED
)with (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://172.27.95.212:31306/dushu?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
    'username' = 'root',
    'password' = 'jingansi110',
    'table-name' = 'device',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '3600s',
    'lookup.max-retries' = '3'
);


-- 建立映射mysql的表（为了查询组织id）
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
    'url' = 'jdbc:mysql://172.27.95.212:31306/ja-4a?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
    'username' = 'root',
    'password' = 'jingansi110',
    'table-name' = 'users',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '3600s',
    'lookup.max-retries' = '3'
);


-- 执法仪回溯数据创建flink映射doris的表
drop table if exists ja_device_index_doris;
create table ja_device_index_doris (
  device_id                varchar(50)  comment '上报的设备id',
  acquisition_time         bigint       comment '上报时间戳',
  device_type              varchar(50)  comment '上报的设备类型',
  acquisition_time_format  string      	comment '格式化时间',
  lng_02					varchar(50)	comment '经度—高德坐标系、火星坐标系',
  lat_02					varchar(50)	comment '纬度—高德坐标系、火星坐标系',
  lng_84					varchar(50)	comment '经度—84坐标系',
  lat_84					varchar(50)	comment '纬度—84坐标系',
  rectify_lng_lat			varchar(50)	comment '高德坐标算法纠偏后经纬度',
  record_id               	varchar(50) comment '行为id',
  username                	varchar(50) comment '设备用户',
  group_id                	varchar(255) comment '组织id',
  `type`                	varchar(50) comment 'POSITION 位置',
  location_type            varchar(100) comment '位置类型',
  start_time               string      comment '开始时间（开窗）',
  end_time                 string      comment '结束时间（开窗）',
  update_time              string      comment '更新插入时间（数据入库时间'
)WITH (
'connector' = 'doris',
'fenodes' = '172.27.95.211:30030',
'table.identifier' = 'test.ja_device_track_rt',
'username' = 'admin',
'password' = 'admin',
'doris.request.tablet.size'='1',
'doris.request.read.timeout.ms'='30000',
'sink.batch.size'='100000',
'sink.batch.interval'='2s'
);


-- 执法仪实时点位位置数据创建flink映射doris的表
drop table if exists ja_device_track_position_doris;
create table ja_device_track_position_doris (
	device_id				varchar(50)		comment '上报的设备id',
	acquisition_time		bigint			comment '上报时间戳',
	device_type				varchar(50)		comment '上报的设备类型',
	acquisition_time_format	string			comment '采集时间戳格式化',
	lng_02					varchar(50)		comment '经度—高德坐标系、火星坐标系',
	lat_02					varchar(50)		comment '纬度—高德坐标系、火星坐标系',
	lng_84					varchar(50)		comment '经度—84坐标系',
	lat_84					varchar(50)		comment '纬度—84坐标系',
	rectify_lng_lat			varchar(50)		comment '高德坐标算法纠偏后经纬度',
	record_id				varchar(50)		comment '行为id',
	username				varchar(50)		comment '设备用户',
	group_id				varchar(255)	comment '组织id',
	type					varchar(100)	comment '类型（高度，电量...）',
	location_type			varchar(100)	comment '位置类型（MAP）',
	update_time				string			comment '数据插入时间'
)WITH (
'connector' = 'doris',
'fenodes' = '172.27.95.211:30030',
'table.identifier' = 'test.ja_device_track_position_rt',
'username' = 'admin',
'password' = 'admin',
'doris.request.tablet.size'='1',
'doris.request.read.timeout.ms'='30000',
'sink.batch.size'='100000',
'sink.batch.interval'='1s'
);


-- 无人机飞行记录所有的数据直接入库mysql
drop table if exists dwd_device_index_all_mysql;
create table dwd_device_index_all_mysql (
    record_id           varchar(50)     comment '行为id or 事件id',
    type                varchar(50)     comment 'ELECTRIC 电量 POSITION 位置 AGL_ALTITUDE 高度',
    report_time_stamp   bigint          comment '上报时间戳',
    `value`             varchar(50)     comment '值',
    location_type       varchar(50)     comment '位置类型',
    report_device_id    varchar(50)     comment '上报的设备id',
    report_device_type  varchar(50)     comment '上报的设备类型',
    time_stamp_date     string          comment '格式化时间',
	username            varchar(50)   	comment '设备用户',
	group_id            varchar(250)   	comment '组织id',
    update_date         string          comment '更新插入时间（数据入库时间）',
    primary key (record_id,type,report_time_stamp) NOT ENFORCED
)with (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://172.27.95.212:31306/dw_rt?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
    'username' = 'root',
    'password' = 'jingansi110',
    'table-name' = 'dwd_device_index_all_rt',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '3600s',
    'lookup.max-retries' = '3'
);


-- 无人机飞行记录所有的数据直接入库doris
drop table if exists dwd_device_index_all_doris;
create table dwd_device_index_all_doris (
    record_id           varchar(50)     comment '行为id or 事件id',
    type                varchar(50)     comment 'ELECTRIC 电量 POSITION 位置 AGL_ALTITUDE 高度',
    report_time_stamp   bigint          comment '上报时间戳',
    `value`             varchar(50)     comment '值',
    location_type       varchar(50)     comment '位置类型',
    report_device_id    varchar(50)     comment '上报的设备id',
    report_device_type  varchar(50)     comment '上报的设备类型',
    time_stamp_date     string          comment '格式化时间',
	username            varchar(50)   	comment '设备用户',
	group_id            varchar(250)   	comment '组织id',
    update_date         string          comment '更新插入时间（数据入库时间）'
)WITH (
'connector' = 'doris',
'fenodes' = '172.27.95.211:30030',
'table.identifier' = 'test.dwd_device_index_all_rt',
'username' = 'admin',
'password' = 'admin',
'doris.request.tablet.size'='1',
'doris.request.read.timeout.ms'='30000',
'sink.batch.size'='100000',
'sink.batch.interval'='2s'
);


-- 无人机飞行记录每个任务的最新状态数据入mysql
drop table if exists dws_device_aircraft_status_mysql;
create table dws_device_aircraft_status_mysql (
    report_device_id    varchar(50)     comment '上报的设备id',
    type                varchar(50)     comment 'ELECTRIC 电量 POSITION 位置 AGL_ALTITUDE 高度',
    `value`             varchar(50)     comment '值',
    record_id           varchar(50)     comment '行为id or 事件id',
    location_type       varchar(50)     comment '位置类型',
    report_device_type  varchar(50)     comment '上报的设备类型',
    report_time_stamp   bigint          comment '上报时间戳',
    time_stamp_date     string          comment '格式化时间',
	username            varchar(50)   	comment '设备用户',
	group_id            varchar(250)   	comment '组织id',
    update_date         string          comment '更新插入时间（数据入库时间）',
    primary key (report_device_id,type) NOT ENFORCED
)with (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://172.27.95.212:31306/dw_rt?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
    'username' = 'root',
    'password' = 'jingansi110',
    'table-name' = 'dws_device_aircraft_status_rt',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '3600s',
    'lookup.max-retries' = '3'
);


-- 无人机飞行记录每个任务的最新状态数据入doris
drop table if exists dws_device_aircraft_status_doris;
create table dws_device_aircraft_status_doris (
    report_device_id    varchar(50)     comment '上报的设备id',
    type                varchar(50)     comment 'ELECTRIC 电量 POSITION 位置 AGL_ALTITUDE 高度',
    `value`             varchar(50)     comment '值',
    record_id           varchar(50)     comment '行为id or 事件id',
    location_type       varchar(50)     comment '位置类型',
    report_device_type  varchar(50)     comment '上报的设备类型',
    report_time_stamp   bigint          comment '上报时间戳',
    time_stamp_date     string          comment '格式化时间',
	username            varchar(50)   	comment '设备用户',
	group_id            varchar(250)   	comment '组织id',
    update_date         string          comment '更新插入时间（数据入库时间）'
)WITH (
'connector' = 'doris',
'fenodes' = '172.27.95.211:30030',
'table.identifier' = 'test.dws_device_aircraft_status_rt',
'username' = 'admin',
'password' = 'admin',
'doris.request.tablet.size'='1',
'doris.request.read.timeout.ms'='30000',
'sink.batch.size'='100000',
'sink.batch.interval'='2s'
);


-- 无人机飞行记录每个任务距离计算入mysql
drop table if exists ads_device_aircraft_distance_info_mysql;
create table ads_device_aircraft_distance_info_mysql (
    record_id   varchar(50)     comment '行为id or 事件id',
    device_id   varchar(50)     comment '设备id',
    start_time  string          comment '开始时间',
    end_time    string          comment '结束时间',
    distance    double          comment '总长度 默认0',
	username    varchar(50)   	comment '设备用户',
	group_id    varchar(250)   	comment '组织id',
    update_date string          comment '更新插入时间（数据入库时间）',
    primary key (record_id) NOT ENFORCED
)with (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://172.27.95.212:31306/dw_rt?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
    'username' = 'root',
    'password' = 'jingansi110',
    'table-name' = 'ads_device_aircraft_distance_info_rt',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '3600s',
    'lookup.max-retries' = '3'
);


-- 无人机飞行记录每个任务距离计算入doris
drop table if exists ads_device_aircraft_distance_info_doris;
create table ads_device_aircraft_distance_info_doris (
    record_id   varchar(50)     comment '行为id or 事件id',
    device_id   varchar(50)     comment '设备id',
    start_time  string          comment '开始时间',
    end_time    string          comment '结束时间',
    distance    double          comment '总长度 默认0',
	username    varchar(50)   	comment '设备用户',
	group_id    varchar(250)   	comment '组织id',
    update_date string          comment '更新插入时间（数据入库时间）'
)WITH (
'connector' = 'doris',
'fenodes' = '172.27.95.211:30030',
'table.identifier' = 'test.ads_device_aircraft_distance_info_rt',
'username' = 'admin',
'password' = 'admin',
'doris.request.tablet.size'='1',
'doris.request.read.timeout.ms'='30000',
'sink.batch.size'='100000',
'sink.batch.interval'='2s'
);


-- 对数据进行处理
-- 对kafka的数据进行加一个关联维表的时间函数
drop view if exists tmp_ja_device_index_01;
create view tmp_ja_device_index_01 as
select
	*,
	PROCTIME() as proctime			-- 维表关联的时间函数
from ja_device_index_kafka;


-- 对kafka的数据与维表数据进行关联
drop view if exists tmp_ja_device_index_02;
create view tmp_ja_device_index_02 as
select
    t1.recordId           ,
    t1.type               ,
    t1.`value`            ,
    t1.locationType       ,
    t1.reportDeviceId     ,
    t1.reportDeviceType   ,
    t1.reportTimeStamp 		,
	t1.rowtime				,
	from_unixtime(t1.reportTimeStamp/1000) as acquisition_time_format,
	t2.username as username,
	t3.group_id as group_id
from tmp_ja_device_index_01 as t1
left join device_mysql
FOR SYSTEM_TIME AS OF t1.proctime as t2
on t1.reportDeviceId = t2.device_id
left join ja_device_index_mysql_user
FOR SYSTEM_TIME AS OF t1.proctime as t3
on t2.username = t3.username;


-- 对kafka来的数据进行筛选（位置类型的数据,时间戳不为空的数据）
-- 对数据的时间戳（单位s）进行对10取余
drop view if exists tmp_ja_device_index_03;
create view tmp_ja_device_index_03 as
select
	recordId         as record_id,									-- 记录id
    type               ,
    `value`            ,
    locationType     as location_type,								-- 位置类型
    reportDeviceId   as device_id,									-- 设备id
    reportDeviceType as device_type,								-- 设备类型
    reportTimeStamp  as acquisition_time,							-- 采集时间戳
	split_index(`value`,',',0) as lng_02				,
	split_index(`value`,',',1) as lat_02				,
	username			,
	group_id			,
	acquisition_time_format,
	reportTimeStamp/1000%10 as acquisition_time_remainder,			-- 余数
	reportTimeStamp/1000/10*10 as acquisition_time_delete_remainder	-- 去除余数
from tmp_ja_device_index_02
where type = 'POSITION';



 -- 判断余数是1、2、3、4、5还是0、6、7、8、9、10
 -- 对数据进行轨迹纠偏
drop view if exists tmp_ja_device_index_04;
create view tmp_ja_device_index_04 as
 select
    device_id   	,
    record_id   	,
    `type`           ,
    `value`          ,
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
	mapmatchudf(record_id,cast(lng_02 as double),cast(lat_02 as double),acquisition_time_format) as rectify_lng_lat,
	if(acquisition_time_remainder>=1 and acquisition_time_remainder<=5,acquisition_time_delete_remainder+5,acquisition_time_delete_remainder+10) as final_acquisition_time
from tmp_ja_device_index_03;


-- 对无人机数据进行处理
-- 开窗取上一条数据经纬度
drop view if exists tmp_ja_device_index_05;
create view tmp_ja_device_index_05 as
select
    recordId as record_id,					-- 记录id
    reportDeviceId as device_id,			-- 设备id
    reportTimeStamp as report_time_stamp,	-- 采集时间戳
    `value`,								-- 值
	split_index(`value`,',',0) as lng_02				,
	split_index(`value`,',',1) as lat_02				,
	username,
	group_id,
	acquisition_time_format,
    lag(reportTimeStamp) over(partition by recordId order by rowtime) as pre_time,	-- 上一个时间数据的时间戳
    lag(`value`) over(partition by recordId order by rowtime) as pre_value			-- 上一个时间数据的值
from tmp_ja_device_index_02
where type = 'POSITION';


-- 经纬度切分转成double类型
drop view if exists tmp_ja_device_index_06;
create view tmp_ja_device_index_06 as
select
    record_id,
    device_id,
    report_time_stamp,		-- 采集时间戳
	pre_value,				-- 上一个时间数据的值
    `value`,
	username,
	group_id,
    cast(split_index(pre_value,',',0) as double) as pre_lng,	-- 上一条数据经度
    cast(split_index(pre_value,',',1) as double) as pre_lat,	-- 上一条数据维度
    cast(lng_02 as double) as lng_02,			-- 这一条数据的经度
    cast(lat_02 as double) as lat_02			-- 这一条数据的维度
from tmp_ja_device_index_05;


-- 计算距离,两个经纬度之间的距离
drop view if exists tmp_ja_device_index_07;
create view tmp_ja_device_index_07 as
select
    record_id,				-- 记录id
    device_id,				-- 设备id
    report_time_stamp,		-- 采集时间戳
	username,
	group_id,
    if(pre_value = `value`,0,distance_udf(lat_02,lng_02,pre_lat,pre_lng)) as distance		-- 计算距离
from tmp_ja_device_index_06;


-- 分组对距离进行求总和
drop view if exists tmp_ja_device_index_08;
create view tmp_ja_device_index_08 as
select
    record_id,						-- 记录id
    max(device_id) as device_id,	-- 设备id
    sum(distance)  as distance,		-- 距离
	max(username) as username,
	max(group_id) as group_id
from tmp_ja_device_index_07
group by record_id;


-- 对kafka全量数据分组（为了取出本次飞行的开始时间和最大结束时间）
drop view if exists tmp_ja_device_index_09;
create view tmp_ja_device_index_09 as
select
    recordId as record_id,
    max(reportDeviceId) as device_id,
    from_unixtime((min(reportTimeStamp))/1000)    as start_time,
    from_unixtime((max(reportTimeStamp))/1000)    as end_time
from tmp_ja_device_index_02
group by recordId;


-- 2个表进行关联
drop view if exists tmp_ja_device_index_10;
create view tmp_ja_device_index_10 as
select
    t1.record_id,
    t1.device_id,
    t1.start_time,
    t1.end_time,
	t2.username,
	t2.group_id,
    coalesce(t2.distance*1000,0) as distance	-- 单位：米（m）
from tmp_ja_device_index_09 as t1
left join tmp_ja_device_index_08 as t2
on t1.record_id = t2.record_id;


 -- 最终对数据进行插入
 begin statement set;

 -- 对执法仪的实时点位位置数据整理字段关联维表取出用户名和组织id
 insert into ja_device_track_position_doris
 select
  device_id,
  acquisition_time,
  device_type,
  acquisition_time_format,
  lng_02,
  lat_02,
  '' as lng_wgs,
  '' as lat_wgs,
  rectify_lng_lat,
  record_id,
  username,
  group_id,
  type,
  location_type,
  from_unixtime(unix_timestamp()) as update_time
 from tmp_ja_device_index_04;


  -- 对执法仪回溯数据整理字段并关联维表取出用户名和组织id
 insert into ja_device_index_doris
 select
  device_id,
  final_acquisition_time as acquisition_time,
  device_type,
  from_unixtime(final_acquisition_time) as acquisition_time_format,
  lng_02,
  lat_02,
  '' as lng_wgs,
  '' as lat_wgs,
  rectify_lng_lat,
  record_id,
  username,
  group_id,
  type,
  location_type,
  from_unixtime(final_acquisition_time - 5) as start_time,
  from_unixtime(acquisition_time/1000) as end_time,
  from_unixtime(unix_timestamp()) as update_time
 from tmp_ja_device_index_04;


 -- 无人机飞行记录全量数据入mysql
insert into dwd_device_index_all_mysql
select
    recordId                                as record_id,
    type                                 ,
    reportTimeStamp                         as report_time_stamp,
    `value`                                 ,
    locationType                            as location_type,
    reportDeviceId                          as report_device_id,
    reportDeviceType                        as report_device_type,
    acquisition_time_format				     as time_stamp_date, 		-- 采集时间戳
	username,
	group_id,
    from_unixtime(unix_timestamp())           as update_date         	-- 数据入库时间
from tmp_ja_device_index_02;


-- 无人机飞行记录全量数据入doris
insert into dwd_device_index_all_doris
select
    recordId                                as record_id,
    type                                 ,
    reportTimeStamp                         as report_time_stamp,
    `value`                                 ,
    locationType                            as location_type,
    reportDeviceId                          as report_device_id,
    reportDeviceType                        as report_device_type,
    acquisition_time_format				    as time_stamp_date, 		-- 采集时间戳
	username,
	group_id,
    from_unixtime(unix_timestamp())           as update_date
from tmp_ja_device_index_02;


-- 无人机飞行记录最新状态数据入mysql
insert into dws_device_aircraft_status_mysql
select
    recordId                          as report_device_id,
    type                                   ,
    `value`                                ,
    recordId                                as record_id,
    locationType                            as location_type,
    reportDeviceType                        as report_device_type,
    reportTimeStamp                         as report_time_stamp,
    acquisition_time_format     			as time_stamp_date,
	username,
	group_id,
    from_unixtime(unix_timestamp())        		as update_date         -- 数据入库时间
from tmp_ja_device_index_02;


-- 无人机飞行记录最新状态数据入doris
insert into dws_device_aircraft_status_doris
select
   recordId                          		as report_device_id,
    type                                   ,
    `value`                                ,
    recordId                                as record_id,
    locationType                            as location_type,
    reportDeviceType                        as report_device_type,
    reportTimeStamp                         as report_time_stamp,
    acquisition_time_format     			as time_stamp_date,
	username,
	group_id,
    from_unixtime(unix_timestamp())        	as update_date         -- 数据入库时间
from tmp_ja_device_index_02;



-- 无人机距离计算的数据入mysql
insert into ads_device_aircraft_distance_info_mysql
select
	record_id,
    device_id,
    start_time,
    end_time,
    distance,
	username,
	group_id,
    from_unixtime(unix_timestamp())   as update_date         -- 数据入库时间
from tmp_ja_device_index_10;


--距离计算入doris
insert into ads_device_aircraft_distance_info_doris
select
	record_id,
    device_id,
    start_time,
    end_time,
    distance,
	username,
	group_id,
    from_unixtime(unix_timestamp())   as update_date         -- 数据入库时间
from tmp_ja_device_index_10;

end;

