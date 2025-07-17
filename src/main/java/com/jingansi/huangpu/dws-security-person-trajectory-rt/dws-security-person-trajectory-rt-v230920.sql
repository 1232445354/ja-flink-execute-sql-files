--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2023/9/20 15:44:34
-- description: 靖安-闵行执法仪的轨迹数据入库
-- version: 1.1.0.230920
--********************************************************************--

set 'pipeline.name' = 'dws-security-person-trajectory-rt';

set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';

-- checkpoint的时间和位置
-- set 'execution.checkpointing.interval' = '100000';
-- set 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/dws-security-person-trajectory-rt';


-- 自定义函数注册
create function geo_to_gaode as 'com.jingan.udf.geohash.GeoUdf';
create function geoHash8 as 'com.jingan.udf.geohash.GeoHash8Udf';
create function aroundGeoHash8 as 'com.jingan.udf.geohash.AroundGeoHash8Udf';


---------------------

-- 数据来源kafka

---------------------

-- TTP数据来源

-- 回溯数据kafka的映射的表,无人机飞行记录数据映射kafka的表
drop table if exists ja_device_index_kafka;
create table ja_device_index_kafka (
                                       recordId            string  comment '行为id or 事件id',
                                       type                string  comment 'ELECTRIC 电量 POSITION 位置 AGL_ALTITUDE 高度',
                                       `value`             string  comment '值',
                                       locationType        string  comment '位置类型',
                                       reportDeviceId      string  comment '上报的设备id',
                                       reportDeviceType    string  comment '上报的设备类型',
                                       reportTimeStamp     bigint  comment '上报时间戳'
    -- rowtime as to_timestamp_ltz(reportTimeStamp,3),
    -- watermark for rowtime as rowtime - interval '3' second
) WITH (
      'connector' = 'kafka',
      'topic' = 'ja_device_index',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-device-index-rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1688010522000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 国标数据来源
drop table if exists ja_device_index_kafka_guobiao;
create table ja_device_index_kafka_guobiao (
                                               deviceId        string  comment '上报的设备id',
                                               deviceType      string  comment '上报的设备类型',
                                               longitude       string  comment '经度',
                                               latitude        string  comment '纬度',
                                               `timeStamp`     bigint  comment '上报时间戳'
    -- rowtime as to_timestamp_ltz(reportTimeStamp,3),
    -- watermark for rowtime as rowtime - interval '3' second
) WITH (
      'connector' = 'kafka',
      'topic' = 'ja_device_position',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-device-position-rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1688010522000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 执法仪信息表
drop table if exists dim_law_enforcement_4g_device__info;
create table dim_law_enforcement_4g_device__info(
                                                    device_id 				  string      comment '4G执法记录仪国标编号',
                                                    device_name 				  string      comment '设备',
                                                    device_alias_name 		  string      comment '设备俗称',
                                                    device_organization_id 	  string      comment '设备所属组织机构编号',
                                                    device_organization_name    string      comment '设备所属组织机构名称',
                                                    security_person_no 		  string      comment '设备归属安保人员编号（警号）',
                                                    security_person_name	 	  string      comment '设备归属安保人员姓名',
                                                    primary key (device_id) not enforced
)with (
     'connector' = 'jdbc',
     'url' ='jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja_patrol_control?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     -- 'url' ='jdbc:mysql://15.185.222.50:31306/ads?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',  -- 闵行
     'username' = 'root',
     'password' = 'jingansi110',
     -- 'password' = 'Jingansi@110',   -- 闵行
     'table-name' = 'law_enforcement_4g_device__info',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '120s',
     'lookup.max-retries' = '3'
     );



-- 人员基本信息表
drop table if exists dim_security_person_info;
create table dim_security_person_info(
                                         security_person_no 					string    comment '安保人员编号（警号）',
                                         device_id 							string    comment '拥有4G执法记录仪国标编号',
                                         security_person_card_id 				string    comment '安保人员证件号码',
                                         security_person_name 					string    comment '安保人员姓名',
                                         security_person_sex 					string    comment '安保人员性别',
                                         security_person_phone_no 				string    comment '安保人员手机号',
                                         security_person_photo_url 			string    comment '安保人员照片地址',
                                         security_person_type 					string    comment '安保人员类型 patrol 巡逻民警',
                                         security_person_type_name 			string    comment '安保人员类型名称  巡逻民警',
                                         walkie_talkie_no 						string    comment '手台号、对讲机号',
                                         security_person_driver_license_type 	string    comment '驾照类型',
                                         organization_id 						string    comment '组织机构编号 派出所编号',
                                         organization_name 					string    comment '组织机构名称 派出所名称',
                                         device_name 							string    comment '拥有4G执法记录仪设备名称',
                                         device_alias_name 					string    comment '拥有4G执法记录仪设备俗称',
                                         primary key (security_person_no) not enforced
)with (
     'connector' = 'jdbc',
     -- 'url' ='jdbc:mysql://15.185.222.50:31306/ads?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',   -- 闵行
     'url' ='jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja_patrol_control?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     'username' = 'root',
     'password' = 'jingansi110',
     -- 'password' = 'Jingansi@110',  -- 闵行
     'table-name' = 'security_person_info',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '120s',
     'lookup.max-retries' = '3'
     );



-- --doris 巡逻民警轨迹
-- drop table if exists dws_security_person_patrol_trajectory_rt;
-- create table dws_security_person_patrol_trajectory_rt(
--   security_person_no                       string  comment '安保人员编号',
--   data_source                              string  comment '数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）',
--   acquisition_time                         string  comment '采集时间',
--   acquisition_time_format                  string  comment '格式化采集时间',
--   security_person_name                     string  comment '安保人员姓名',
--   security_person_card_id                  string  comment '安保人员证件号',
--   security_person_phone_no                 string  comment '安保人员手机号',
--   security_person_photo_url                string  comment '安保人员头像url',
--   security_person_type                     string  comment '安保人员类型',
--   security_person_type_name                string  comment '安保人员类型名称 patrol 巡逻民警',
--   device_id                                string  comment '设备编号（执法仪的编号和微信的设备号）',
--   device_name                              string  comment '设备名称',
--   device_alias_name                        string  comment '设备俗称',
--   walkie_talkie_no						   string  comment '手台号、对讲机id、电台号',
--   device_organization_name                 string  comment '设备所有机构名称',
--   wechat_acquisition_type                  string  comment '警务微信的采集类型',
--   organization_id                          string  comment '机构编码',
--   organization_name                        string  comment '机构名称',
--   longitude                                double  comment '经度',
--   latitude                                 double  comment '纬度',
--   gd_longitude                             double  comment '高德经度',
--   gd_latitude                              double  comment '高德纬度',
--   gd_geohash8                              string  comment '高德geohash8',
--   gd_around_geohash8					   string  comment '高德geohash8周边区域',
--   datahub_system_time                      string  comment '上传datahub时间',
--   upload_delay_time                        bigint  comment '上传到datahub的延迟时间',
--   update_time                              string  comment '更新时间'
-- )with (
--     'connector' = 'doris',
--     -- 'fenodes' = '15.185.222.50:30030',   -- 闵行
--     -- 'table.identifier' = 'hp_ads.dws_security_person_patrol_trajectory_rt',  -- 闵行
--     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
--     'table.identifier' = 'ja_patrol_control.dws_security_person_patrol_trajectory_rt',
--     'username' = 'admin',
--     'password' = 'Jingansi@110',
--     'doris.request.tablet.size' = '1',
--     'doris.request.read.timeout.ms' = '30000',
-- 	'sink.max-retries' ='6',
--     'sink.batch.size' = '100000',
--     'sink.batch.interval' = '3s'
-- );



--轨迹全量数据表

drop table if exists dws_security_person_trajectory_rt;
CREATE TABLE `dws_security_person_trajectory_rt` (
                                                     security_person_no                    string     comment '安保人员编号',
                                                     data_source                           string     comment '数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）',
                                                     acquisition_time                      string     comment '采集时间',
                                                     acquisition_time_format               string     comment '格式化采集时间',
                                                     security_person_name                  string     comment '安保人员姓名',
                                                     security_person_card_id               string     comment '安保人员证件号',
                                                     security_person_phone_no              string     comment '安保人员手机号',
                                                     security_person_photo_url             string     comment '安保人员头像url',
                                                     security_person_type                  string     comment '安保人员类型',
                                                     security_person_type_name             string     comment  '安保人员类型名称 patrol 巡逻民警',
                                                     device_id                             string     comment '设备编号（执法仪的编号和微信的设备号）',
                                                     device_name                           string     comment '设备名称',
                                                     device_alias_name                     string     comment '设备俗称',
                                                     walkie_talkie_no						string     comment '手台号、对讲机id、电台号',
                                                     device_organization_name              string     comment '设备所有机构名称',
                                                     wechat_acquisition_type               string     comment '警务微信的采集类型',
                                                     organization_id                       string     comment '机构编码',
                                                     organization_name                     string     comment '机构名称',
                                                     longitude                             double     comment '经度',
                                                     latitude                              double     comment '纬度',
                                                     gd_longitude                          double     comment '高德经度',
                                                     gd_latitude                           double     comment '高德纬度',
                                                     gd_geohash8 							string     comment '高德geohash8',
                                                     gd_around_geohash8					string     comment '高德geohash8周边区域',
                                                     datahub_system_time                   string     comment '上传datahub时间',
                                                     upload_delay_time                     bigint     comment '上传到datahub的延迟时间',
                                                     update_time                           string     comment '更新时间'
)WITH (
     'connector' = 'doris',
     -- 'fenodes' = '15.185.222.50:30030',    -- 闵行
     -- 'table.identifier' = 'hp_ads.dws_security_person_trajectory_rt',    -- 闵行
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- ECS
     'table.identifier' = 'ja_patrol_control.dws_security_person_trajectory_rt',  -- ECS
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='60000',
     'sink.max-retries' ='6',
     'sink.batch.size'='100000',
     'sink.batch.interval'='3s'
     );


--doris 设备在线状态表
drop table if exists dws_device_status_info;
create table dws_device_status_info(
                                       device_id                                string  comment '设备编号（执法仪的编号和微信的设备号）',
                                       acquire_timestamp_format                 string  comment '采集时间',
                                       update_time                              string  comment '更新时间'
)with (
     'connector' = 'doris',
     -- 'fenodes' = '15.185.222.50:30030',   -- 闵行
     --  'table.identifier' = 'hp_ads.dws_device_status_info',    -- 闵行
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- ECS
     'table.identifier' = 'ja_patrol_control.dws_device_status_info',  -- ECS
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size' = '1',
     'doris.request.read.timeout.ms' = '30000',
     'sink.max-retries' ='6',
     'sink.batch.size' = '100000',
     'sink.batch.interval' = '3s'
     );



-------------------------

-- 数据处理

-------------------------

-- TTP
-- 对kafka的数据进行加一个关联维表的时间函数，并筛选数据
drop view if exists tmp_ja_device_index_01;
create view tmp_ja_device_index_01 as
select
    recordId           as record_id,
    type               ,
    `value`            ,
    cast(split_index(`value`,',',0) as double) as longitude				,
    cast(split_index(`value`,',',1) as double) as latitude				,
    locationType       as location_type,
    reportDeviceId     as device_id,
    reportDeviceType   as device_type,
    reportTimeStamp    as acquire_timestamp,
    -- rowtime				,
    from_unixtime(reportTimeStamp/1000) as acquire_timestamp_format,
    PROCTIME() as proctime			-- 维表关联的时间函数
from ja_device_index_kafka
where reportTimeStamp is not null
  and type = 'POSITION';


-- 将数据坐标来源84转成高德
drop table if exists tmp_dws_security_person_trajectory_rt_01;
create view tmp_dws_security_person_trajectory_rt_01 as
select
    'LawEnforcement'  as data_source, -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    device_id,
    acquire_timestamp,
    acquire_timestamp_format,
    longitude,
    latitude,
    proctime,
    cast(null as varchar) as datahub_system_time 		, -- 时间
    0 as upload_delay_time 		, -- 时间差
    geo_to_gaode(longitude,latitude) as geo_lng_lat
from tmp_ja_device_index_01;



-- 关联唯表去除相应字段
drop view if exists tmp_dws_security_person_trajectory_rt_02;
create view tmp_dws_security_person_trajectory_rt_02 as
select
    b.security_person_no 				as security_person_no                    , -- 安保人员编号
    a.data_source 					as data_source                           , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    a.acquire_timestamp 				as acquisition_time                      , -- 采集时间
    a.acquire_timestamp_format 		as acquisition_time_format               , -- 格式化采集时间
    b.security_person_name 			as security_person_name                  , -- 安保人员姓名
    b.security_person_card_id 		as security_person_card_id               , -- 安保人员证件号
    a.device_id 						as device_id                             , -- 设备编号（执法仪的编号和微信的设备号）
    c.device_name 					as device_name                           , -- 设备名称
    c.device_alias_name 				as device_alias_name                     , -- 设备俗称
    b.organization_name 				as device_organization_name              , -- 设备所有机构名称
    b.organization_id  				as organization_id                       , -- 人员组织id
    b.organization_name 				as organization_name                     , -- 人员组织名称
    b.walkie_talkie_no 				as walkie_talkie_no                      , -- 手台号
    b.security_person_type 			as security_person_type                  , -- 安保人员类型 patrol 巡逻民警
    b.security_person_type_name 		as security_person_type_name             , -- 安保人员类型名称 patrol 巡逻民警
    b.security_person_phone_no 		as security_person_phone_no              , -- 人员手机号
    a.longitude 						as longitude                             , -- 经度
    a.latitude 						as latitude                              , -- 纬度
    cast(split_index(geo_lng_lat,',',0) as double) 	as gd_longitude          , -- 高德经度
    cast(split_index(geo_lng_lat,',',1) as double)	as gd_latitude           , -- 高德纬度
    datahub_system_time                   , -- 上传datahub时间
    upload_delay_time                      -- 上传到datahub的延迟时间
from tmp_dws_security_person_trajectory_rt_01 a
         left join dim_security_person_info FOR SYSTEM_TIME as of a.proctime as b
                   on a.device_id=b.device_id
         left join dim_law_enforcement_4g_device__info FOR SYSTEM_TIME as of a.proctime as c
                   on a.device_id=c.device_id
where b.device_id is not null;


-- 产生geohash8和周围的geohash 8个位置
drop view if exists tmp_dws_security_person_trajectory_rt_02_01;
create view tmp_dws_security_person_trajectory_rt_02_01 as
select
    *,
    geoHash8(gd_longitude,gd_latitude) as gd_geohash8 ,
    aroundGeoHash8(gd_longitude,gd_latitude) as gd_around_geohash8
from tmp_dws_security_person_trajectory_rt_02
where longitude>0;



-- 国标
-- 对kafka的数据进行加一个关联维表的时间函数，并筛选数据----- -- 将数据坐标来源84转成高德
drop view if exists tmp_dws_security_person_trajectory_rt_01_guobiao;
create view tmp_dws_security_person_trajectory_rt_01_guobiao as
select
    'LawEnforcement'                   as data_source, -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    cast(longitude as double)          as longitude,
    cast(latitude as double)           as latitude,
    deviceId                           as device_id,
    deviceType                         as device_type,
    `timeStamp`                        as acquire_timestamp,
    from_unixtime(`timeStamp`/1000)    as acquire_timestamp_format,
    PROCTIME()                         as proctime	,-- 维表关联的时间函数
    cast(null as varchar)              as datahub_system_time , -- 时间
    0 as upload_delay_time 		       , -- 时间差
    geo_to_gaode(cast(longitude as double),cast(latitude as double)) as geo_lng_lat
from ja_device_index_kafka_guobiao
where `timeStamp` is not null
  and cast(longitude as double) > 0
  and cast(latitude as double) > 0;



-- 关联唯表去除相应字段
drop view if exists tmp_dws_security_person_trajectory_rt_02_guobiao;
create view tmp_dws_security_person_trajectory_rt_02_guobiao as
select
    b.security_person_no 				as security_person_no                    , -- 安保人员编号
    a.data_source 					as data_source                           , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    a.acquire_timestamp 				as acquisition_time                      , -- 采集时间
    a.acquire_timestamp_format 		as acquisition_time_format               , -- 格式化采集时间
    b.security_person_name 			as security_person_name                  , -- 安保人员姓名
    b.security_person_card_id 		as security_person_card_id               , -- 安保人员证件号
    a.device_id 						as device_id                             , -- 设备编号（执法仪的编号和微信的设备号）
    c.device_name 					as device_name                           , -- 设备名称
    c.device_alias_name 				as device_alias_name                     , -- 设备俗称
    b.organization_name 				as device_organization_name              , -- 设备所有机构名称
    b.organization_id  				as organization_id                       , -- 人员组织id
    b.organization_name 				as organization_name                     , -- 人员组织名称
    b.walkie_talkie_no 				as walkie_talkie_no                      , -- 手台号
    b.security_person_type 			as security_person_type                  , -- 安保人员类型 patrol 巡逻民警
    b.security_person_type_name 		as security_person_type_name             , -- 安保人员类型名称 patrol 巡逻民警
    b.security_person_phone_no 		as security_person_phone_no              , -- 手机号
    a.longitude 						as longitude                             , -- 经度
    a.latitude 						as latitude                              , -- 纬度
    cast(split_index(geo_lng_lat,',',0) as double) 	as gd_longitude          , -- 高德经度
    cast(split_index(geo_lng_lat,',',1) as double)	as gd_latitude           , -- 高德纬度
    datahub_system_time                   , -- 上传datahub时间
    upload_delay_time                      -- 上传到datahub的延迟时间
from tmp_dws_security_person_trajectory_rt_01_guobiao a
         left join dim_security_person_info FOR SYSTEM_TIME as of a.proctime as b
                   on a.device_id=b.device_id
         left join dim_law_enforcement_4g_device__info FOR SYSTEM_TIME as of a.proctime as c
                   on a.device_id=c.device_id
where b.device_id is not null;



-- 产生geohash8和周围的geohash 8个位置
drop view if exists tmp_dws_security_person_trajectory_rt_02_01_guobiao;
create view tmp_dws_security_person_trajectory_rt_02_01_guobiao as
select
    *,
    geoHash8(gd_longitude,gd_latitude) as gd_geohash8 ,
    aroundGeoHash8(gd_longitude,gd_latitude) as gd_around_geohash8
from tmp_dws_security_person_trajectory_rt_02_guobiao
where longitude>0;



-----------------------

-- 数据插入

-----------------------


begin statement set;

-- TTP 数据入库
insert into dws_security_person_trajectory_rt
select
    security_person_no                       , -- 安保人员编号
    data_source                              , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
--  cast(acquisition_time as varchar) as acquisition_time                         , -- 采集时间
    acquisition_time_format as acquisition_time,
    acquisition_time_format                  , -- 格式化采集时间
    security_person_name                     , -- 安保人员姓名
    security_person_card_id                  , -- 安保人员证件号
    security_person_phone_no                 , -- 安保人员手机号
    '' as security_person_photo_url                , -- 安保人员头像url
    security_person_type                     , -- 安保人员类型
    security_person_type_name                , -- 安保人员类型名称 patrol 巡逻民警
    device_id                                , -- 设备编号（执法仪的编号和微信的设备号）
    device_name                              , -- 设备名称
    device_alias_name                        , -- 设备俗称
    walkie_talkie_no						   , -- 手台号、对讲机id、电台号
    device_organization_name                 , -- 设备所有机构名称
    '' as wechat_acquisition_type                  , -- 警务微信的采集类型
    organization_id                          , -- 机构编码
    organization_name                        , -- 机构名称
    longitude                                , -- 经度
    latitude                                 , -- 纬度
    gd_longitude                             , -- 高德经度
    gd_latitude                              , -- 高德纬度
    if(gd_longitude>0,geoHash8(gd_longitude,gd_latitude),'') as gd_geohash8 ,
    if(gd_longitude>0,aroundGeoHash8(gd_longitude,gd_latitude),'') as gd_around_geohash8 ,
    datahub_system_time                      , -- 上传datahub时间
    upload_delay_time                        , -- 上传到datahub的延迟时间
    from_unixtime(unix_timestamp()) as update_time        -- 更新时间
from tmp_dws_security_person_trajectory_rt_02;



-- -- 巡逻民警轨迹
-- insert into dws_security_person_patrol_trajectory_rt
-- select
--   security_person_no                       , -- 安保人员编号
--   data_source                              , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
--   -- cast(acquisition_time as varchar) as  acquisition_time, -- 采集时间
--   acquisition_time_format as acquisition_time,
--   acquisition_time_format                  , -- 格式化采集时间
--   security_person_name                     , -- 安保人员姓名
--   security_person_card_id                  , -- 安保人员证件号
--   security_person_phone_no                 , -- 安保人员手机号
--   '' as security_person_photo_url                , -- 安保人员头像url
--   security_person_type                     , -- 安保人员类型
--   security_person_type_name                , -- 安保人员类型名称 patrol 巡逻民警
--   device_id                                , -- 设备编号（执法仪的编号和微信的设备号）
--   device_name                              , -- 设备名称
--   device_alias_name                        , -- 设备俗称
--   walkie_talkie_no						   , -- 手台号、对讲机id、电台号
--   device_organization_name                 , -- 设备所有机构名称
--   '' as wechat_acquisition_type                  , -- 警务微信的采集类型
--   organization_id                          , -- 机构编码
--   organization_name                        , -- 机构名称
--   longitude                                , -- 经度
--   latitude                                 , -- 纬度
--   gd_longitude                             , -- 高德经度
--   gd_latitude                              , -- 高德纬度
--   gd_geohash8                              , -- 高德geohash8
--   gd_around_geohash8                       , -- 高德geohash8周边区域
--   datahub_system_time                      , -- 上传datahub时间
--   upload_delay_time                        , -- 上传到datahub的延迟时间
--   from_unixtime(unix_timestamp()) as update_time        -- 更新时间
-- from tmp_dws_security_person_trajectory_rt_02_01;
-- -- where security_person_type='patrol';



-- 设备在线逻辑控制
insert into dws_device_status_info
select
    reportDeviceId     as device_id,
    from_unixtime(`reportTimeStamp`/1000) as acquire_timestamp_format,
    from_unixtime(unix_timestamp()) as update_time        -- 更新时间
from ja_device_index_kafka
where `reportTimeStamp` is not null;




-- 国标 数据入库
insert into dws_security_person_trajectory_rt
select
    security_person_no                       , -- 安保人员编号
    data_source                              , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
--  cast(acquisition_time as varchar) as acquisition_time                         , -- 采集时间
    acquisition_time_format as acquisition_time,
    acquisition_time_format                  , -- 格式化采集时间
    security_person_name                     , -- 安保人员姓名
    security_person_card_id                  , -- 安保人员证件号
    security_person_phone_no                 , -- 安保人员手机号
    '' as security_person_photo_url                , -- 安保人员头像url
    security_person_type                     , -- 安保人员类型
    security_person_type_name                , -- 安保人员类型名称 patrol 巡逻民警
    device_id                                , -- 设备编号（执法仪的编号和微信的设备号）
    device_name                              , -- 设备名称
    device_alias_name                        , -- 设备俗称
    walkie_talkie_no						   , -- 手台号、对讲机id、电台号
    device_organization_name                 , -- 设备所有机构名称
    '' as wechat_acquisition_type                  , -- 警务微信的采集类型
    organization_id                          , -- 机构编码
    organization_name                        , -- 机构名称
    longitude                                , -- 经度
    latitude                                 , -- 纬度
    gd_longitude                             , -- 高德经度
    gd_latitude                              , -- 高德纬度
    if(gd_longitude>0,geoHash8(gd_longitude,gd_latitude),'') as gd_geohash8 ,
    if(gd_longitude>0,aroundGeoHash8(gd_longitude,gd_latitude),'') as gd_around_geohash8 ,
    datahub_system_time                      , -- 上传datahub时间
    upload_delay_time                        , -- 上传到datahub的延迟时间
    from_unixtime(unix_timestamp()) as update_time        -- 更新时间
from tmp_dws_security_person_trajectory_rt_02_guobiao;


-- -- 巡逻民警轨迹
-- insert into dws_security_person_patrol_trajectory_rt
-- select
--   security_person_no                       , -- 安保人员编号
--   data_source                              , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
--   -- cast(acquisition_time as varchar) as  acquisition_time, -- 采集时间
--   acquisition_time_format as acquisition_time,
--   acquisition_time_format                  , -- 格式化采集时间
--   security_person_name                     , -- 安保人员姓名
--   security_person_card_id                  , -- 安保人员证件号
--   security_person_phone_no                 , -- 安保人员手机号
--   '' as security_person_photo_url                , -- 安保人员头像url
--   security_person_type                     , -- 安保人员类型
--   security_person_type_name                , -- 安保人员类型名称 patrol 巡逻民警
--   device_id                                , -- 设备编号（执法仪的编号和微信的设备号）
--   device_name                              , -- 设备名称
--   device_alias_name                        , -- 设备俗称
--   walkie_talkie_no						   , -- 手台号、对讲机id、电台号
--   device_organization_name                 , -- 设备所有机构名称
--   '' as wechat_acquisition_type                  , -- 警务微信的采集类型
--   organization_id                          , -- 机构编码
--   organization_name                        , -- 机构名称
--   longitude                                , -- 经度
--   latitude                                 , -- 纬度
--   gd_longitude                             , -- 高德经度
--   gd_latitude                              , -- 高德纬度
--   gd_geohash8                              , -- 高德geohash8
--   gd_around_geohash8                       , -- 高德geohash8周边区域
--   datahub_system_time                      , -- 上传datahub时间
--   upload_delay_time                        , -- 上传到datahub的延迟时间
--   from_unixtime(unix_timestamp()) as update_time        -- 更新时间
-- from tmp_dws_security_person_trajectory_rt_02_01_guobiao;
-- -- where security_person_type='patrol';


-- 设置在线逻辑控制
insert into dws_device_status_info
select
    deviceId     as device_id,
    from_unixtime(`timeStamp`/1000) as acquire_timestamp_format,
    from_unixtime(unix_timestamp()) as update_time        -- 更新时间
from ja_device_index_kafka_guobiao
where `timeStamp` is not null;

end;


-- drop table if exists ods_location_law_enforcement;
-- create table ods_location_law_enforcement(
--   `time`         bigint,
--   deviceid         STRING,
--   longitude         DOUBLE,
--   latitude          DOUBLE,
--   height            DOUBLE,
--   speed             DOUBLE,
--   status            string,
--   proctime as proctime(),
--   `system-time`     TIMESTAMP metadata virtual
-- ) with (
--   'connector' = 'datahub',
--   'endPoint' = 'http://datahub.cn-shanghai-hpspfx-d01.dh.cloud.hpq.ga.sh',
--   'project' = 'datahub_jakj',
--   'topic' = 'ods_location_law_enforcement',
--   'subId' = '1642949925175U095H',
--   'startTime' = '2022-03-04 14:10:00',
--   'accessId' = 'UZXByV3IxGKVCUWb',
--   'accessKey' = 'sOrxpZCBvE3mXAh8BZDi4nOyYOYQg7',
--   'maxBufferSize' = '50000',
--   'maxFetchSize' = '10000'
-- );



-- -- doris
-- drop table if exists dim_hp_card_on_duty_rt;
-- create table dim_hp_card_on_duty_rt(
--   security_person_no             varchar(20)  comment '警号,跟4A保持一致',
--   organization_id                varchar(20)  comment '排岗组织编码,跟4A保持一致',
--   post_start_time_pt             varchar(20)  comment '分区岗位开始时间',
--   post_start_time                BIGINT       comment '岗位开始时间',
--   post_start_time_formart        varchar(20)  comment '格式化岗位开始时间',
--   post_end_time                  BIGINT       comment '岗位结束时间',
--   post_end_time_format           varchar(20)  comment '格式化岗位结束时间',
--   post_type_id                   varchar(10)  comment '岗位类型ID ，该字段值详见岗位类型字典',
--   whether_punch                  BIGINT       comment '是否打卡：0-未打卡；1-打卡',
--   gpsx                           varchar(50)  comment '打卡x坐标',
--   gpsy                           varchar(50)  comment '打卡Y坐标',
--   last_update_time               BIGINT       comment '最近打卡时间',
--   last_update_time_format        varchar(20)  comment '格式化最近打卡时间',
--   equip_type                     varchar(50)  comment '携枪状态：1-携枪；2-未携枪',
--   trip_mode                      varchar(50)  comment '出行方式：1-步行；2-自行车；3-摩托；4-汽车',
--   create_time                    BIGINT       comment '创建时间',
--   create_time_format             varchar(20)  comment '格式化创建时间',
--   first_update_time              BIGINT       comment '首次打卡时间',
--   first_update_time_format       varchar(20)  comment '格式化首次打卡时间',
--   first_update_hour              BIGINT       comment '首次打卡小时',
--   deleted                        BIGINT       comment '是否删除：0-未删除；1-删除',
--   post_start_date                BIGINT       comment '岗位开始日期，时分秒均为“00”',
--   post_start_date_format         varchar(20)  comment '格式化岗位开始日期，时分秒均为“00”',
--   post_end_date                  BIGINT       comment '岗位结束日期，时分秒均为“00”',
--   post_end_date_format           varchar(20)  comment '格式化岗位结束日期，时分秒均为“00”',
--   last_update_hour               BIGINT       comment '最近打卡小时',
--   send_time                      BIGINT       comment '消息生产时间,用来标记消息批次',
--   send_time_format               varchar(20)  comment '格式化消息生产时间,用来标记消息批次',
--   datahub_system_time            varchar(50)  comment '消息生产时间,用来标记消息批次',
--   update_time                    varchar(20)  comment '更新时间'
--  )with (
--     'connector' = 'doris',
--     'fenodes' = '15.185.194.106:30030',
--     'table.identifier' = 'hp_ads.dim_hp_card_on_duty_rt',
--     'username' = 'admin',
--     'password' = 'Jingansi@110',
--     'doris.request.tablet.size' = '1',
--     'doris.request.read.timeout.ms' = '30000',
-- 	'sink.max-retries' ='6',
--     'sink.batch.size' = '100000',
--     'sink.batch.interval' = '3s'
-- );



-- -- drop view if exists tmp_dws_security_person_trajectory_rt_01;
-- -- create view tmp_dws_security_person_trajectory_rt_01 as
-- -- select
-- --   'LawEnforcement'  as data_source                           , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
-- --   from_unixtime(`time`/1000) as acquisition_time                      , -- 采集时间
-- --   from_unixtime(`time`/1000) as acquisition_time_format               , -- 格式化采集时间
-- --   deviceid as device_id                             , -- 设备编号（执法仪的编号和微信的设备号）
-- --   longitude as longitude                             , -- 经度
-- --   latitude as latitude                              , -- 纬度
-- --   split_index(geo,',',0) as gd_longitude                          , -- 高德经度
-- --   split_index(geo,',',1) as gd_latitude                           , -- 高德纬度
-- --   cast(`system-time` as varchar(20)) as datahub_system_time                   , -- 上传datahub时间
-- --   unix_timestamp(substring(cast(`system-time` as string),1,19))*1000+cast(substring(cast(`system-time` as string),21,3)as bigint) -`time`/1000  as upload_delay_time                     , -- 上传到datahub的延迟时间
-- --   proctime
-- -- from (select *,geo_to_gaode(longitude,latitude) as geo from ods_location_law_enforcement) a;




-- drop view if exists tmp_dws_security_person_trajectory_rt_03;
-- create view tmp_dws_security_person_trajectory_rt_03 as
-- select
--   jgzjh as security_person_no                    , -- 安保人员编号
--   'WeChat' as data_source                           , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
--   from_unixtime(cjsj_int) as acquisition_time                      , -- 采集时间
--   from_unixtime(cjsj_int)as acquisition_time_format               , -- 格式化采集时间
--   xm as security_person_name                  , -- 安保人员姓名
--   zjhm as security_person_card_id               , -- 安保人员证件号
--   coalesce(sjhm,b.security_person_phone_no) as security_person_phone_no              , -- 安保人员手机号
--   sbbh as device_id                             , -- 设备编号（执法仪的编号和微信的设备号）
--   b.device_name as device_name                           , -- 设备名称
--   b.device_alias_name as device_alias_name                     , -- 设备俗称
--   b.organization_name as device_organization_name              , -- 设备所有机构名称
--   cjlx as wechat_acquisition_type               , -- 警务微信的采集类型
--   dwdm as organization_id                       , -- 机构编码
--   dwmc as organization_name                     , -- 机构名称
--   x as longitude                             , -- 经度
--   y as latitude                              , -- 纬度
--   ddgd_jd as gd_longitude                          , -- 高德经度
--   ddgd_wd as gd_latitude                           , -- 高德纬度
--   cast(`system-time` as varchar(20)) as datahub_system_time                   , -- 上传datahub时间
--   unix_timestamp(cast(`system-time` as varchar(20)))-cjsj_int as upload_delay_time,                      -- 上传到datahub的延迟时间
--   b.walkie_talkie_no as walkie_talkie_no,
--   b.security_person_type as security_person_type                , -- 安保人员类型 patrol 巡逻民警
--   b.security_person_type_name as security_person_type_name            -- 安保人员类型名称 patrol 巡逻民警
-- from dwd_sj_xw_kqwzxx84_di a left join dim_security_person_info FOR SYSTEM_TIME as of a.proctime as b
-- on a.jgzjh=b.security_person_no
-- where dwdm like '310101%'  -- 20*24*60*60=1728000
--   and cjsj_int>=unix_timestamp()-1728000 ;



-- -- 警情数据
-- DROP TABLE IF EXISTS dwd_sj_aj_110_sjd_di;
-- CREATE  TABLE IF NOT EXISTS  dwd_sj_aj_110_sjd_di(
--     dwd_zjid        STRING ,            --  主键ID
--     sjdbh           STRING,                --  事件编号
--     bjr             STRING,                --  报警人
--     bjrxb            STRING,                --  报警人性别
--     scbjsj          bigint,                --  报警时间
--     bjhm            STRING,                --  报警号码
--     lxdh            STRING,                --  联系电话
--     lxdz            STRING,                --  联系地址
--     sjxq            STRING,                --  事件详情
--     sfdz            STRING,                --  事发地址
--     xzqh            STRING,                --  行政区划
--     xzqhmc             STRING,                --  行政区划名称
--     ssfjmc            STRING,                --  分局名称
--     ssxqmc             STRING,                --  派出所
--     gd_jd           STRING,                --  高德-经度
--     gd_wd           STRING,                --  高德-纬度
--     gd_geohash        STRING,                --  高德-geohash
--     jjygh            STRING,                --  接警人警号
--     jjyxm            STRING,                --  接警员性名
--     cjdw            STRING,                --  出警单位
--     scfknr            STRING,                --  出警反馈
--     aymc            STRING,                --  事件类型
--     aqjb            STRING,                --  级别
--     aqlbmc            STRING,                --  xx分类
--     sjztmc            STRING,                --  事件状态名称
--     `system-time`    TIMESTAMP metadata virtual
-- ) with(
--     'connector' = 'datahub',
--     'endPoint' = 'http://datahub.cn-shanghai-shga-d01.dh.alicloud.ga.sh',
--     'project' = 'shga_dwd',
--     'topic' = 'dwd_sj_aj_110_sjd_di',
--     'subId' = '1639129148508LHK6C',
--     'startTime' = '2022-03-04 14:10:00',
--     'accessId' ='aJce7Pe99lOf1PAi',
--     'accessKey' = '1LbS2M2nPpVFx3ZGhC5NnLfmeLjK18',
--     'maxBufferSize' = '50000',
--     'maxFetchSize' = '10000'
-- );



-- DROP TABLE IF EXISTS dwd_event_110_rt;
-- CREATE TABLE IF NOT EXISTS dwd_event_110_rt(
--     event_num        STRING        COMMENT    '事件编号',
--     initiator_id    STRING         COMMENT '发起人编号',
--     initiator_name    STRING        COMMENT '发起人姓名',
--     initiator_sex    STRING         COMMENT '发起人性别',
--     initiator_phone STRING        COMMENT    '发起人电话',
--     initiator_address STRING    COMMENT '发起人联系地址',
--     event_time             bigint     COMMENT '事件时间',
--     event_title         STRING     COMMENT    '事件标题',
--     event_detail         STRING    COMMENT '事件详情',
--     event_address         STRING    COMMENT '事件地址',
--     event_lng            STRING    COMMENT '事件经度',
--     event_lat             STRING    COMMENT    '事件纬度',
--     event_geohash         STRING    COMMENT 'geohash',
--     receiver_id            STRING    COMMENT    '受理人id',
--     receiver_name         STRING     COMMENT    '受理人姓名',
--     receiver_org        STRING     COMMENT '受理人组织',
--     handler_id            STRING    COMMENT '处理人ID',
--     handler_name        STRING     COMMENT '处理人姓名',
--     handler_org         STRING     COMMENT '处理人组织',
--     event_feedback        STRING     COMMENT '事件反馈',
--     event_level            STRING     COMMENT '事件级别',
--     event_status         STRING    COMMENT '事件状态',
--     event_type            STRING     COMMENT '事件类型',
--     update_time         varchar(20)  comment '更新时间'
-- )with (
--       'connector' = 'doris',
--       'fenodes' = '15.112.169.228:8030',
--       'table.identifier' = 'hp_ads.dwd_event_110_rt',
--       'username' = 'root',
--       'password' = 'Jingansi@110',
--       'doris.request.tablet.size'='1',
--      'doris.request.read.timeout.ms'='30000',
-- 	 'sink.max-retries' ='6',
--      'sink.batch.size'='100000',
--      'sink.batch.interval'='3s'
-- );




-- -- 出警数据
-- DROP TABLE IF EXISTS dwd_sj_aj_110_cjdxx_di;
-- CREATE  TABLE IF NOT EXISTS dwd_sj_aj_110_cjdxx_di(
--     cjdbh            STRING ,            --  出警单编号
--     sjdbh             STRING,                --  事件编号
--     cjkssj            bigint,            --  出警开始时间
--     cjygh            STRING,                --  出警员警号
--     cjyxm            STRING,                --  出警员姓名
--     cjjssj            bigint,            --  出警结束时间
--     gxdwmc         STRING,                --  出警单位名称
--     `system-time`     TIMESTAMP metadata virtual
-- ) with(

--     'connector' = 'datahub',
--     'endPoint' = 'http://datahub.cn-shanghai-shga-d01.dh.alicloud.ga.sh',
--     'project' = 'shga_dwd',
--     'topic' = 'dwd_sj_aj_110_cjdxx_di',
--     'subId' = '1639188417172WPDPJ',
--     'startTime' = '2022-03-04 14:10:00',
--     'accessId' ='aJce7Pe99lOf1PAi',
--     'accessKey' = '1LbS2M2nPpVFx3ZGhC5NnLfmeLjK18',
--     'maxBufferSize' = '50000',
--     'maxFetchSize' = '10000'
-- );


-- -- 事件处置
-- DROP TABLE IF EXISTS dwd_event_110_dispose_rt;
-- CREATE TABLE IF NOT EXISTS dwd_event_110_dispose_rt(
--     dispose_num        STRING        COMMENT    '处置编号',
--     event_num        STRING        COMMENT    '事件编号',
--     disposer_name    STRING        COMMENT '处置人员姓名',
--     disposer_id        STRING        COMMENT '处置人员ID',
--     disposer_org    STRING        COMMENT '处理人组织',
--     dispose_start_time     BIGINT    COMMENT    '处置开始时间',
--     dispose_end_time     BIGINT    COMMENT '处置结束时间',
--     dispose_feedback    STRING    COMMENT '处置反馈',
--     create_time         STRING     COMMENT '创建时间'
-- )with (
--     'connector' = 'doris',
--     'fenodes' = '15.112.169.228:8030',
--     'table.identifier' = 'hp_ads.dwd_event_110_dispose_rt',
--     'username' = 'root',
--     'password' = 'Jingansi@110',
--     'doris.request.tablet.size' = '1',
--     'doris.request.read.timeout.ms' = '30000',
-- 	'sink.max-retries' ='6',
--     'sink.batch.size' = '100000',
--     'sink.batch.interval' = '3s'
-- );

-- -- 排班打卡信息表
-- drop table if exists hp_cardOnDuty;
-- create table hp_cardOnDuty(
--   personcode        STRING    comment '警号,跟4A保持一致',
--   orgcode           STRING    comment '排岗组织编码,跟4A保持一致',
--   poststarttime     BIGINT    comment '岗位开始时间',
--   postendtime       BIGINT    comment '岗位结束时间',
--   posttypeid        STRING    comment '岗位类型ID ，该字段值详见岗位类型字典',
--   whetherpunch      BIGINT    comment '是否打卡：0-未打卡；1-打卡',
--   gpsx              STRING    comment '打卡x坐标',
--   gpsy              STRING    comment '打卡Y坐标',
--   lastupdatetime    BIGINT    comment '最近打卡时间',
--   equiptype         STRING    comment '携枪状态：1-携枪；2-未携枪',
--   tripmode          STRING    comment '出行方式：1-步行；2-自行车；3-摩托；4-汽车',
--   createtime        BIGINT    comment '创建时间',
--   firstupdatetime   BIGINT    comment '首次打卡时间',
--   firstupdatehour   BIGINT    comment '首次打卡小时',
--   deleted           BIGINT    comment '是否删除：0-未删除；1-删除',
--   poststartdate     BIGINT    comment '岗位开始日期，时分秒均为“00”',
--   postenddate       BIGINT    comment '岗位结束日期，时分秒均为“00”',
--   lastupdatehour    BIGINT    comment '最近打卡小时',
--   sendtime          BIGINT    comment '消息生产时间,用来标记消息批次',
--   proctime as proctime(),
--   --rt as TO_TIMESTAMP_LTZ(data_time/1000,3),
--   --watermark for rt as rt - interval '1' minute,
--   `system-time`     TIMESTAMP metadata virtual
-- ) with (
--   'connector' = 'datahub',
--   'endPoint' = 'http://datahub.cn-shanghai-shga-d01.dh.alicloud.ga.sh',
--   'project' = 'zhb_qwglxt',
--   'topic' = 'hp_cardOnDuty',
--   'subId' = '1640436841920SKSLF',
--   'startTime' = '2022-03-04 14:10:00',
--   'accessId' = '3TdhjjbSNeKQHCor',
--   'accessKey' = 'RFgeN3IjxmRnyKZUYwvVGRI6JACgzP',
--   'maxBufferSize' = '50000',
--   'maxFetchSize' = '10000'
-- );




-- insert into dws_security_person_trajectory_rt
--     select
--       security_person_no       as security_person_no                    , -- 安保人员编号
--       data_source              as data_source                           , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
--       acquisition_time         as acquisition_time                      , -- 采集时间
--       acquisition_time_format  as acquisition_time_format               , -- 格式化采集时间
--       security_person_name     as security_person_name                  , -- 安保人员姓名
--       security_person_card_id  as security_person_card_id               , -- 安保人员证件号
--       security_person_phone_no as security_person_phone_no              , -- 安保人员手机号
-- 	  'null' as security_person_photo_url             , -- 安保人员头像url
--       security_person_type as security_person_type                  , -- 安保人员类型
-- 	  security_person_type_name as security_person_type_name                  , -- 安保人员类型
--       device_id                as device_id                             , -- 设备编号（执法仪的编号和微信的设备号）
--       'null'              as device_name                           , -- 设备名称
--       'null'        as device_alias_name                     , -- 设备俗称
-- 	  walkie_talkie_no        as walkie_talkie_no                     , -- 设备俗称
--       'null' as device_organization_name              , -- 设备所有机构名称
--       wechat_acquisition_type  as wechat_acquisition_type               , -- 警务微信的采集类型
--       organization_id          as organization_id                       , -- 机构编码
--       organization_name        as organization_name                     , -- 机构名称
--       longitude                as longitude                             , -- 经度
--       latitude                 as latitude                              , -- 纬度
--       gd_longitude             as gd_longitude                          , -- 高德经度
--       gd_latitude              as gd_latitude                           , -- 高德纬度
-- 	  if(gd_longitude>0,geoHash8(gd_longitude,gd_latitude),'') as gd_geohash8 ,
--       if(gd_longitude>0,aroundGeoHash8(gd_longitude,gd_latitude),'') as gd_around_geohash8 ,
--       datahub_system_time      as datahub_system_time                   , -- 上传datahub时间
--       upload_delay_time        as upload_delay_time                     , -- 上传到datahub的延迟时间,
-- 	  from_unixtime(unix_timestamp()) as update_time                -- 更新时间
--     from tmp_dws_security_person_trajectory_rt_03;



--insert into dwd_event_110_rt
--select
--    sjdbh            as event_num,            -- 事件ID
--    ''                    as initiator_id,        -- 发起人ID
--    bjr                 as initiator_name,        -- 发起人姓名
--   bjrxb                 as initiator_sex,        -- 发起人性别
--    lxdh                as initiator_phone,        -- 发起人联系电话
--    lxdz                as initiator_address,    -- 发起人联系地址
--    scbjsj              as event_time,            -- 发起电话
--    '110报警'            as event_title,            -- 事件标题
--    replace(sjxq,'	','')               as event_detail,        -- 事件详情
--    sfdz                 as event_address,        -- 事发地址
--   gd_jd                as event_lng,            -- 经度
--    gd_wd                as event_lat,            -- 纬度
--    gd_geohash            as event_geohash,        -- geohash
--    jjygh                as receiver_id,            -- 受理人id
--    jjyxm                 as receiver_name,        -- 受理人姓名
--    ''                    as receiver_org,        -- 受理人组织
--    ''                    as handler_id,            -- 处理人id
--   ''                    as handler_name,        -- 处理人姓名
--	cjdw                as handler_org,            -- 处理人单位
--    replace(scfknr,'	','')  as event_feedback       , -- 事件反馈
--    aqjb as event_level                 , -- 事件级别
--    sjztmc as event_status             , --事件状态
--    replace(aymc,'	','') as event_type  , -- 事件类型
--    from_unixtime(unix_timestamp()) as update_time                -- 更新时间
--from dwd_sj_aj_110_sjd_di
--where cjdw like '%黄浦分局%';


--insert into dwd_event_110_dispose_rt
--select
-- cjdbh                as dispose_num,            -- 处置编号
-- sjdbh                as event_num,            -- 事件编号
-- cjyxm                as disposer_name,         -- 处置人员姓名
-- cjygh                as disposer_id,            -- 处置人员编号
--gxdwmc                as disposer_org,        -- 处置人员组织
-- cjkssj                as dispose_start_time,     -- 处置开始时间
-- cjjssj                as dispose_end_time ,   -- 处置结束时间
-- ''                    as dispose_feedback,    -- 处置反馈
-- from_unixtime(unix_timestamp()) as create_time
--from dwd_sj_aj_110_cjdxx_di
--where gxdwmc='黄浦分局';


-- insert into dim_hp_card_on_duty_rt
-- select
--   personcode as security_person_no             , -- 警号,跟4A保持一致
--   orgcode as organization_id                , -- 排岗组织编码,跟4A保持一致
--   from_unixtime(poststarttime/1000) as post_start_time_formart        , -- 格式化岗位开始时间
--   poststarttime as post_start_time                , -- 岗位开始时间
--   from_unixtime(poststarttime/1000) as post_start_time_formart        , -- 格式化岗位开始时间
--   postendtime as post_end_time                  , -- 岗位结束时间
--   from_unixtime(postendtime/1000) as post_end_time_format           , -- 格式化岗位结束时间
--   posttypeid as post_type_id                   , -- 岗位类型ID ，该字段值详见岗位类型字典
--   whetherpunch as whether_punch                  , -- 是否打卡：0-未打卡；1-打卡
--   gpsx as gpsx                           , -- 打卡x坐标
--   gpsy as gpsy                           , -- 打卡Y坐标
--   lastupdatetime as last_update_time               , -- 最近打卡时间
--   from_unixtime(lastupdatetime/1000) as last_update_time_format        , -- 格式化最近打卡时间
--   equiptype as equip_type                     , -- 携枪状态：1-携枪；2-未携枪
--   tripmode as trip_mode                      , -- 出行方式：1-步行；2-自行车；3-摩托；4-汽车
--   createtime as create_time                    , -- 创建时间
--   from_unixtime(createtime/1000) as create_time_format             , -- 格式化创建时间
--   firstupdatetime as first_update_time              , -- 首次打卡时间
--   from_unixtime(firstupdatetime/1000) as first_update_time_format       , -- 格式化首次打卡时间
--   firstupdatehour as first_update_hour              , -- 首次打卡小时
--   deleted as deleted                        , -- 是否删除：0-未删除；1-删除
--   poststartdate as post_start_date                , -- 岗位开始日期，时分秒均为“00”
--   from_unixtime(poststartdate/1000) as post_start_date_format         , -- 格式化岗位开始日期，时分秒均为“00”
--   postenddate as post_end_date                  , -- 岗位结束日期，时分秒均为“00”
--   from_unixtime(postenddate/1000) as post_end_date_format           , -- 格式化岗位结束日期，时分秒均为“00”
--   lastupdatehour as last_update_hour               , -- 最近打卡小时
--   sendtime as send_time                      , -- 消息生产时间,用来标记消息批次
--   from_unixtime(sendtime/1000) as send_time_format               , -- 格式化消息生产时间,用来标记消息批次
--   cast(`system-time` as string) as datahub_system_time            , -- 消息生产时间,用来标记消息批次
--   from_unixtime(unix_timestamp()) as update_time                     -- 更新时间
-- from hp_cardOnDuty;









