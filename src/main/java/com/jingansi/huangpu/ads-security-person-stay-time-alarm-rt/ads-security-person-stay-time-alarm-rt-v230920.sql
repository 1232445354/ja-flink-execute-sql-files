--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2023/9/20 17:35:20
-- description: 靖安-闵行执法仪告警数据入库
-- version: 1.1.0.230920
--********************************************************************--
set 'pipeline.name' = 'ads-security-person-stay-time-alarm-rt';


set 'parallelism.default' = '1';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';


-- checkpoint的时间和位置
-- set 'execution.checkpointing.interval' = '100000';
-- set 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ads-security-person-stay-time-alarm-rt';


create function geo_to_gaode as 'com.jingan.udf.geohash.GeoUdf';
create function geoHash8 as 'com.jingan.udf.geohash.GeoHash8Udf';
create function aroundGeoHash8 as 'com.jingan.udf.geohash.AroundGeoHash8Udf';


---------------------

-- 数据来源kafka

---------------------

-- TTP
-- 回溯数据kafka的映射的表,无人机飞行记录数据映射kafka的表
drop table if exists ja_device_index_kafka;
create table ja_device_index_kafka (
                                       recordId            string  comment '行为id or 事件id',
                                       type                string  comment 'ELECTRIC 电量 POSITION 位置 AGL_ALTITUDE 高度',
                                       `value`             string  comment '值',
                                       locationType        string  comment '位置类型',
                                       reportDeviceId      string  comment '上报的设备id',
                                       reportDeviceType    string  comment '上报的设备类型',
                                       reportTimeStamp     bigint  comment '上报时间戳',
                                       rt as to_timestamp_ltz(reportTimeStamp,3),
                                       watermark for rt as rt - interval '1' minute
) WITH (
      'connector' = 'kafka',
      'topic' = 'ja_device_index',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-device-index-rt-alarm222',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1695520490000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 国标
drop table if exists ja_device_index_kafka_guobiao;
create table ja_device_index_kafka_guobiao (
                                               deviceId        string  comment '上报的设备id',
                                               deviceType      string  comment '上报的设备类型',
                                               longitude       string  comment '经度',
                                               latitude        string  comment '纬度',
                                               `timeStamp`     bigint  comment '上报时间戳',
                                               rt as to_timestamp_ltz(`timeStamp`,3),
                                               watermark for rt as rt - interval '1' minute
) WITH (
      'connector' = 'kafka',
      'topic' = 'ja_device_position',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-device-position-rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1695520490000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 人员基本信息表
drop table if exists dim_device_person_info;
create table dim_device_person_info(
                                       security_person_no 					string  comment '安保人员编号（警号）',
                                       security_person_card_id 				string  comment '安保人员证件号码',
                                       security_person_name 					string  comment '安保人员姓名',
                                       security_person_sex 					string  comment '安保人员性别',
                                       security_person_phone_no 				string  comment '安保人员手机号',
                                       security_person_photo_url 			string  comment '安保人员照片地址',
                                       security_person_type 					string  comment '安保人员类型 patrol 巡逻民警',
                                       security_person_type_name 			string  comment '安保人员类型名称  巡逻民警',
                                       walkie_talkie_no 						string  comment '手台号、对讲机号',
                                       security_person_driver_license_type 	string  comment '驾照类型',
                                       organization_id 						string  comment '组织机构编号 派出所编号',
                                       organization_name 					string  comment '组织机构名称 派出所名称',
                                       device_id 							string  comment '拥有4G执法记录仪国标编号',
                                       device_name 							string  comment '拥有4G执法记录仪设备名称',
                                       device_alias_name 					string  comment '拥有4G执法记录仪设备俗称',
                                       primary key (security_person_no) not enforced
)with (
     'connector' = 'jdbc',
     -- 'url' ='jdbc:mysql://15.185.222.50:31306/ads?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',  -- 闵行
     'url' ='jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja_patrol_control?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     'username' = 'root',
     'password' = 'jingansi110',
     -- 'password' = 'Jingansi@110',   -- 闵行
     'table-name' = 'security_person_info',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '120s',
     'lookup.max-retries' = '3'
     );



-- 停滞告警信息表
drop table if exists ads_security_person_stay_time_alarm_rt;
create table ads_security_person_stay_time_alarm_rt(
                                                       security_person_no        varchar(20)  comment '安保人员编号',
                                                       data_source               varchar(20)  comment '数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）',
                                                       stay_start_time           varchar(20)  comment '停留开始时间',
                                                       stay_end_time             varchar(20)  comment '停留结束时间',
                                                       security_person_name      varchar(50)  comment '安保人员姓名',
                                                       security_person_card_id   varchar(50)  comment '安保人员证件号',
                                                       security_person_phone_no  varchar(30)  comment '安保人员手机号',
                                                       security_person_photo_url varchar(200) comment '安保人员头像url',
                                                       security_person_type      varchar(30)  comment '安保人员类型',
                                                       device_id                 varchar(50)  comment '设备编号（执法仪的编号和微信的设备号）',
                                                       device_name               varchar(100) comment '设备名称',
                                                       device_alias_name         varchar(100) comment '设备俗称',
                                                       walkie_talkie_no          varchar(20)  comment '手台号',
                                                       organization_id           varchar(20)  comment '机构编码',
                                                       organization_name         varchar(30)  comment '机构名称',
                                                       gd_longitude              double       comment '高德经度',
                                                       gd_latitude               double       comment '高德纬度',
                                                       gd_geohash8               varchar(10)  comment '高德geohash8',
                                                       stay_time                 int          comment '停留时间',
                                                       window_start              varchar(20)  comment '窗口开始时间	',
                                                       window_end                varchar(20)  comment '窗口结束时间	',
                                                       insert_time               varchar(20)  comment '插入时间',
                                                       primary key (security_person_no,data_source,stay_start_time) not enforced
)with (
     'connector' = 'jdbc',
     -- 'url' ='jdbc:mysql://15.185.222.50:31306/ads?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',     -- 闵行
     'url' ='jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja_patrol_control?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     'username' = 'root',
     -- 'password' = 'Jingansi@110',     -- 闵行
     'password' = 'jingansi110',
     'table-name' = 'ads_security_person_stay_time_alarm_rt',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '3'
     );



-------------------------

-- 数据处理

-------------------------

-- TTP 数据处理
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
    cast(reportTimeStamp as varchar)    as acquire_timestamp,
    rt				,
    from_unixtime(reportTimeStamp/1000) as acquire_timestamp_format,
    PROCTIME() as proctime			-- 维表关联的时间函数
from ja_device_index_kafka
where reportTimeStamp is not null
  and type = 'POSITION';



-- 将数据坐标来源84转成高德
drop table if exists tmp_01;
create view tmp_01 as
select
    'LawEnforcement'  as data_source, -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    device_id,
    acquire_timestamp,
    acquire_timestamp_format,
    longitude,
    latitude,
    proctime,
    rt,
    cast(null as varchar) as datahub_system_time 		, -- 时间
    0 as upload_delay_time 		, -- 时间差
    geo_to_gaode(longitude,latitude) as geo_lng_lat
from tmp_ja_device_index_01;


-- 关联人员信息表
drop view if exists tmp_02;
create view tmp_02 as
select
    b.security_person_no as security_person_no                    , -- 安保人员编号
    a.data_source as data_source                           , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    a.acquire_timestamp_format as acquisition_time                      , -- 采集时间
    a.acquire_timestamp_format as acquisition_time_format               , -- 格式化采集时间
    b.security_person_name as security_person_name                  , -- 安保人员姓名
    b.security_person_card_id as security_person_card_id               , -- 安保人员证件号
    a.device_id as device_id                             , -- 设备编号（执法仪的编号和微信的设备号）
    b.device_name as device_name                           , -- 设备名称
    b.device_alias_name as device_alias_name                     , -- 设备俗称
    b.organization_name as device_organization_name              , -- 设备所有机构名称
    b.organization_id  as organization_id,
    b.organization_name as organization_name,
    b.walkie_talkie_no as walkie_talkie_no,
    b.security_person_type as security_person_type                , -- 安保人员类型 patrol 巡逻民警
    b.security_person_type_name as security_person_type_name            , -- 安保人员类型名称 patrol 巡逻民警
    b.security_person_phone_no as security_person_phone_no,
    a.longitude as longitude                             , -- 经度
    a.latitude as latitude                              , -- 纬度
    cast(split_index(a.geo_lng_lat,',',0) as double) 	as gd_longitude                          , -- 高德经度
    cast(split_index(a.geo_lng_lat,',',1) as double)	as gd_latitude                           , -- 高德纬度
    datahub_system_time                   , -- 上传datahub时间
    rt,
    upload_delay_time                      -- 上传到datahub的延迟时间
from tmp_01 a
         left join dim_device_person_info FOR SYSTEM_TIME as of a.proctime as b
                   on a.device_id=b.device_id
where b.device_id is not null;


-- 计算geohash8位、周围的geohash
drop view if exists tmp_03;
create view tmp_03 as
select
    *,
    geoHash8(gd_longitude,gd_latitude) as gd_geohash8 ,
    aroundGeoHash8(gd_longitude,gd_latitude) as gd_around_geohash8
from tmp_02
where longitude>0;


-- 计算数据距离机器时间告警
drop view if exists tmp_04;
create view tmp_04 as
select
    window_start,
    window_end,
    security_person_no                    , -- 安保人员编号
    data_source                           , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    security_person_name                  , -- 安保人员姓名
    security_person_card_id               , -- 安保人员证件号
    device_id                             , -- 设备编号（执法仪的编号和微信的设备号）
    device_name                           , -- 设备名称
    device_alias_name                     , -- 设备俗称
    organization_id,
    organization_name,
    security_person_type,
    security_person_phone_no,
    walkie_talkie_no,
    avg(gd_longitude) as gd_longitude,
    avg(gd_latitude) as gd_latitude,
    max(gd_geohash8) as gd_geohash8,
    min(acquisition_time) as stay_start_time,
    max(acquisition_time) as stay_end_time,
    unix_timestamp(max(acquisition_time))-unix_timestamp(min(acquisition_time)) as stay_time
from table(
        hop(table tmp_03,descriptor(rt),interval '1' minutes,interval '48' minutes)) -- 48
group by
    window_start,
    window_end,
    security_person_no,
    data_source,
    security_person_name,
    security_person_card_id,
    organization_id,
    organization_name,
    security_person_type,
    security_person_phone_no,
    walkie_talkie_no,
    device_id,
    device_name,
    device_alias_name
having count(distinct gd_geohash8)<=9
   and count(*)>200
   -- 400
   and unix_timestamp(max(acquisition_time))-unix_timestamp(min(acquisition_time))>1800;
-- 2700




-- 国标 数据处理
-- 对kafka的数据进行加一个关联维表的时间函数，并筛选数据----- -- 将数据坐标来源84转成高德
drop view if exists tmp_01_guobiao;
create view tmp_01_guobiao as
select
    'LawEnforcement'  as data_source, -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    cast(longitude as double) as longitude,
    cast(latitude as double) as latitude,
    deviceId     as device_id,
    deviceType   as device_type,
    `timeStamp`    as acquire_timestamp,
    from_unixtime(`timeStamp`/1000) as acquire_timestamp_format,
    PROCTIME() as proctime			,-- 维表关联的时间函数
    cast(null as varchar) as datahub_system_time 		, -- 时间
    0 as upload_delay_time 		, -- 时间差
    geo_to_gaode(cast(longitude as double),cast(latitude as double)) as geo_lng_lat,
    rt
from ja_device_index_kafka_guobiao
where `timeStamp` is not null
  and cast(longitude as double) > 0
  and cast(latitude as double) > 0;


-- 关联人员信息表
drop view if exists tmp_02_guobiao;
create view tmp_02_guobiao as
select
    b.security_person_no as security_person_no                    , -- 安保人员编号
    a.data_source as data_source                           , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    a.acquire_timestamp_format as acquisition_time                      , -- 采集时间
    a.acquire_timestamp_format as acquisition_time_format               , -- 格式化采集时间
    b.security_person_name as security_person_name                  , -- 安保人员姓名
    b.security_person_card_id as security_person_card_id               , -- 安保人员证件号
    a.device_id as device_id                             , -- 设备编号（执法仪的编号和微信的设备号）
    b.device_name as device_name                           , -- 设备名称
    b.device_alias_name as device_alias_name                     , -- 设备俗称
    b.organization_name as device_organization_name              , -- 设备所有机构名称
    b.organization_id  as organization_id,
    b.organization_name as organization_name,
    b.walkie_talkie_no as walkie_talkie_no,
    b.security_person_type as security_person_type                , -- 安保人员类型 patrol 巡逻民警
    b.security_person_type_name as security_person_type_name            , -- 安保人员类型名称 patrol 巡逻民警
    b.security_person_phone_no as security_person_phone_no,
    a.longitude as longitude                             , -- 经度
    a.latitude as latitude                              , -- 纬度
    cast(split_index(a.geo_lng_lat,',',0) as double) 	as gd_longitude                          , -- 高德经度
    cast(split_index(a.geo_lng_lat,',',1) as double)	as gd_latitude                           , -- 高德纬度
    datahub_system_time                   , -- 上传datahub时间
    rt,
    upload_delay_time                      -- 上传到datahub的延迟时间
from tmp_01_guobiao a left join dim_device_person_info FOR SYSTEM_TIME as of a.proctime as b
                                on a.device_id=b.device_id
where b.device_id is not null;



-- 计算geohash8、周围的geohash
drop view if exists tmp_03_guobiao;
create view tmp_03_guobiao as
select
    *,
    geoHash8(gd_longitude,gd_latitude) as gd_geohash8 ,
    aroundGeoHash8(gd_longitude,gd_latitude) as gd_around_geohash8
from tmp_02_guobiao
where longitude>0;



-- 计算距离及其时间进行告警
drop view if exists tmp_04_guobiao;
create view tmp_04_guobiao as
select
    window_start,
    window_end,
    security_person_no                    , -- 安保人员编号
    data_source                           , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    security_person_name                  , -- 安保人员姓名
    security_person_card_id               , -- 安保人员证件号
    device_id                             , -- 设备编号（执法仪的编号和微信的设备号）
    device_name                           , -- 设备名称
    device_alias_name                     , -- 设备俗称
    organization_id,
    organization_name,
    security_person_type,
    security_person_phone_no,
    walkie_talkie_no,
    avg(gd_longitude) as gd_longitude,
    avg(gd_latitude) as gd_latitude,
    max(gd_geohash8) as gd_geohash8,
    min(acquisition_time) as stay_start_time,
    max(acquisition_time) as stay_end_time,
    unix_timestamp(max(acquisition_time))-unix_timestamp(min(acquisition_time)) as stay_time
from table(
        hop(table tmp_03_guobiao,descriptor(rt),interval '1' minutes,interval '48' minutes)) -- 48
group by
    window_start,
    window_end,
    security_person_no,
    data_source,
    security_person_name,
    security_person_card_id,
    organization_id,
    organization_name,
    security_person_type,
    security_person_phone_no,
    walkie_talkie_no,
    device_id,
    device_name,
    device_alias_name
having count(distinct gd_geohash8)<=9
   and count(*)>200
   -- 400
   and unix_timestamp(max(acquisition_time))-unix_timestamp(min(acquisition_time))>1800;
-- 2700



-----------------------

-- 数据插入

-----------------------


begin statement set;

-- 数据告警入库
insert into ads_security_person_stay_time_alarm_rt
select
    security_person_no        , -- 安保人员编号
    data_source               , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    stay_start_time           , -- 停留开始时间
    stay_end_time             , -- 停留结束时间
    security_person_name      , -- 安保人员姓名
    security_person_card_id   , -- 安保人员证件号
    security_person_phone_no  , -- 安保人员手机号
    '' as security_person_photo_url , -- 安保人员头像url
    security_person_type as security_person_type      , -- 安保人员类型
    device_id                 , -- 设备编号（执法仪的编号和微信的设备号）
    device_name               , -- 设备名称
    device_alias_name         , -- 设备俗称
    walkie_talkie_no as walkie_talkie_no          , -- 手台号
    organization_id           , -- 机构编码
    organization_name         , -- 机构名称
    gd_longitude              , -- 高德经度
    gd_latitude               , -- 高德纬度
    gd_geohash8,
    cast(stay_time as int) as stay_time                , -- 停留时间
    cast(window_start as string)    window_start          , -- 窗口开始时间
    cast(window_end as string) as   window_end              , -- 窗口结束时间
    from_unixtime(unix_timestamp()) as insert_time                 -- 插入时间
from tmp_04;



-- 数据告警入库
insert into ads_security_person_stay_time_alarm_rt
select
    security_person_no        , -- 安保人员编号
    data_source               , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    stay_start_time           , -- 停留开始时间
    stay_end_time             , -- 停留结束时间
    security_person_name      , -- 安保人员姓名
    security_person_card_id   , -- 安保人员证件号
    security_person_phone_no  , -- 安保人员手机号
    '' as security_person_photo_url , -- 安保人员头像url
    security_person_type as security_person_type      , -- 安保人员类型
    device_id                 , -- 设备编号（执法仪的编号和微信的设备号）
    device_name               , -- 设备名称
    device_alias_name         , -- 设备俗称
    walkie_talkie_no as walkie_talkie_no          , -- 手台号
    organization_id           , -- 机构编码
    organization_name         , -- 机构名称
    gd_longitude              , -- 高德经度
    gd_latitude               , -- 高德纬度
    gd_geohash8,
    cast(stay_time as int) as stay_time                , -- 停留时间
    cast(window_start as string)    window_start          , -- 窗口开始时间
    cast(window_end as string) as   window_end              , -- 窗口结束时间
    from_unixtime(unix_timestamp()) as insert_time                 -- 插入时间
from tmp_04_guobiao;

end;


-- -- 执法记录仪维表
--  drop table if exists dim_device_person_info;
--  create table dim_device_person_info(
--   device_id                  varchar(50)       comment '设备国标编号',
--   device_name                varchar(30)       comment '设备名称',
--   device_alias_name          varchar(20)       comment '设备别名俗称',
--   security_person_no		 varchar(20)       comment '安保人员编号 警号',
--   security_person_card_id	 varchar(20)       comment '安保人员证件编号，省份证号',
--   security_person_name		 varchar(20)       comment '安保人员姓名',
--   device_organization_name	 varchar(30)       comment '单位名称',
--   security_person_phone_no   varchar(30)       comment '安保人员手机号',
--   security_person_type                 varchar(30)  comment  '安保人员类型 patrol 巡逻民警',
--   security_person_type_name            varchar(30)  comment  '安保人员类型名称 patrol 巡逻民警',
--   walkie_talkie_no                     varchar(10)  comment  '手台id、电台、对讲机id',
--   security_person_driver_license_type  varchar(30)  comment  '安保人员驾照类型',
--   organization_id			 varchar(20)       comment '安保人员机构编码',
--   organization_name          varchar(100) 	   comment '安保人员机构名称',
--   remarks					 varchar(100)      comment '备注',
--   primary key (device_id) not enforced
--  )with (
-- 	 'connector' = 'jdbc',
--      'url' = 'jdbc:mysql://15.185.222.50:31030/hp_ads?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
-- 	 'username' = 'admin',
-- 	 'password' = 'Jingansi@110',
-- 	 'table-name' = 'dim_device_person_info',
-- 	 'driver' = 'com.mysql.cj.jdbc.Driver',
-- 	 'lookup.cache.max-rows' = '5000',
-- 	 'lookup.cache.ttl' = '3600s',
-- 	 'lookup.max-retries' = '3'
-- );



-- drop view if exists tmp_01;
-- create view tmp_01 as
-- select
--   'LawEnforcement'  as data_source                           , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
--   from_unixtime(`time`/1000) as acquisition_time                      , -- 采集时间
--   from_unixtime(`time`/1000) as acquisition_time_format               , -- 格式化采集时间
--   deviceid as device_id                             , -- 设备编号（执法仪的编号和微信的设备号）
--   longitude as longitude                             , -- 经度
--   latitude as latitude                              , -- 纬度
--   split_index(geo,',',0) as gd_longitude                          , -- 高德经度
--   split_index(geo,',',1) as gd_latitude                           , -- 高德纬度
--   cast(`system-time` as varchar(20)) as datahub_system_time                   , -- 上传datahub时间
--   unix_timestamp(substring(cast(`system-time` as string),1,19))*1000+cast(substring(cast(`system-time` as string),21,3)as bigint) -`time`/1000  as upload_delay_time                     , -- 上传到datahub的延迟时间
--   rt,
--   proctime
-- from (select *,geo_to_gaode(longitude,latitude) as geo from ods_location_law_enforcement) a;







