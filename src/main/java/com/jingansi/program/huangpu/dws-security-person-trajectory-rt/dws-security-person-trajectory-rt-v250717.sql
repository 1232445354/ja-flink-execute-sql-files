--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2023/9/20 15:44:34
-- description: 靖安-闵行执法仪的轨迹数据入库
-- version: dws-security-person-trajectory-rt-v240717
--********************************************************************--

set 'pipeline.name' = 'dws-security-person-trajectory-rt';

set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '100000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/dws-security-person-trajectory-rt';


-- 自定义函数注册
-- 经纬度转换，84转换高德
-- create function geo_to_gaode as 'com.jingan.udf.geohash.GeoUdf';

-- 计算经纬度的geo8位
create function geoHash8 as 'com.jingan.udf.geohash.GeoHash8Udf';

-- 计算周围的8个geo8
create function aroundGeoHash8 as 'com.jingan.udf.geohash.AroundGeoHash8Udf';


---------------------

-- 数据来源kafka

---------------------

-- TTP数据来源

-- 回溯数据kafka的映射的表,无人机飞行记录数据映射kafka的表
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
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-device-index-rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1688010522000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 国标数据来源
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
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-device-position-rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1688010522000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 执法仪信息表
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
     'url' ='jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja_patrol_control?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
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
     'url' ='jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja_patrol_control?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'username' = 'root',
     'password' = 'jingansi110',
     -- 'password' = 'Jingansi@110',  -- 闵行
     'table-name' = 'security_person_info',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '120s',
     'lookup.max-retries' = '3'
     );



--轨迹全量数据表
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

-- ***************************************** TTP *****************************************

-- 对kafka的数据进行加一个关联维表的时间函数，并筛选数据
create view tmp_ja_device_index_01 as
select
    recordId           as record_id,
    type               ,
    `value`            ,
    cast(split_index(`value`,',',0) as double) as longitude				,   -- 高德
    cast(split_index(`value`,',',1) as double) as latitude				,   -- 高德
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
    'LawEnforcement'  as data_source,     -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    device_id,
    acquire_timestamp,
    acquire_timestamp_format,
    longitude,
    latitude,
    proctime,
    cast(null as varchar) as datahub_system_time 		, -- 时间
    -- geo_to_gaode(longitude,latitude) as geo_lng_lat,
    0 as upload_delay_time 		 -- 时间差

from tmp_ja_device_index_01;



-- 关联唯表去除相应字段
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
    a.longitude                       as gd_longitude,
    a.latitude                        as gd_latitude,
    -- cast(split_index(geo_lng_lat,',',0) as double) 	as gd_longitude          , -- 高德经度
    -- cast(split_index(geo_lng_lat,',',1) as double)	    as gd_latitude           , -- 高德纬度
    datahub_system_time                   , -- 上传datahub时间
    upload_delay_time                      -- 上传到datahub的延迟时间
from tmp_dws_security_person_trajectory_rt_01 a
         left join dim_security_person_info FOR SYSTEM_TIME as of a.proctime as b
                   on a.device_id=b.device_id
         left join dim_law_enforcement_4g_device__info FOR SYSTEM_TIME as of a.proctime as c
                   on a.device_id=c.device_id
where b.device_id is not null;


-- 产生geohash8和周围的geohash 8个位置
create view tmp_dws_security_person_trajectory_rt_02_01 as
select
    *,
    geoHash8(gd_longitude,gd_latitude) as gd_geohash8 ,
    aroundGeoHash8(gd_longitude,gd_latitude) as gd_around_geohash8
from tmp_dws_security_person_trajectory_rt_02
where longitude > 0;



-- ***************************************** 国标 *****************************************


-- 对kafka的数据进行加一个关联维表的时间函数，并筛选数据----- -- 将数据坐标来源84转成高德
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
    -- geo_to_gaode(cast(longitude as double),cast(latitude as double)) as geo_lng_lat,
    0 as upload_delay_time 		        -- 时间差

from ja_device_index_kafka_guobiao
where `timeStamp` is not null
  and cast(longitude as double) > 0
  and cast(latitude as double) > 0;



-- 关联唯表去除相应字段
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
    a.longitude                       as gd_longitude,
    a.latitude                        as gd_latitude,
    -- cast(split_index(geo_lng_lat,',',0) as double) 	as gd_longitude          , -- 高德经度
    -- cast(split_index(geo_lng_lat,',',1) as double)	    as gd_latitude           , -- 高德纬度
    datahub_system_time                   , -- 上传datahub时间
    upload_delay_time                      -- 上传到datahub的延迟时间
from tmp_dws_security_person_trajectory_rt_01_guobiao a
         left join dim_security_person_info FOR SYSTEM_TIME as of a.proctime as b
                   on a.device_id=b.device_id
         left join dim_law_enforcement_4g_device__info FOR SYSTEM_TIME as of a.proctime as c
                   on a.device_id=c.device_id
where b.device_id is not null;



-- 产生geohash8和周围的geohash 8个位置
create view tmp_dws_security_person_trajectory_rt_02_01_guobiao as
select
    *,
    geoHash8(gd_longitude,gd_latitude) as gd_geohash8 ,
    aroundGeoHash8(gd_longitude,gd_latitude) as gd_around_geohash8
from tmp_dws_security_person_trajectory_rt_02_guobiao
where longitude > 0;



-----------------------

-- 数据插入

-----------------------


begin statement set;

-- *****************************************  TTP 数据入库 *****************************************
insert into dws_security_person_trajectory_rt
select
    security_person_no                       , -- 安保人员编号
    data_source                              , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    acquisition_time_format        as acquisition_time,
    acquisition_time_format                  , -- 格式化采集时间
    security_person_name                     , -- 安保人员姓名
    security_person_card_id                  , -- 安保人员证件号
    security_person_phone_no                 , -- 安保人员手机号
    cast(null as varchar)      as security_person_photo_url          , -- 安保人员头像url
    security_person_type                     , -- 安保人员类型
    security_person_type_name                , -- 安保人员类型名称 patrol 巡逻民警
    device_id                                , -- 设备编号（执法仪的编号和微信的设备号）
    device_name                              , -- 设备名称
    device_alias_name                        , -- 设备俗称
    walkie_talkie_no						   , -- 手台号、对讲机id、电台号
    device_organization_name                 , -- 设备所有机构名称
    cast(null as varchar) as wechat_acquisition_type                  , -- 警务微信的采集类型
    organization_id                          , -- 机构编码
    organization_name                        , -- 机构名称
    longitude                                , -- 经度
    latitude                                 , -- 纬度
    gd_longitude                             , -- 高德经度
    gd_latitude                              , -- 高德纬度
    if(gd_longitude>0,geoHash8(gd_longitude,gd_latitude),'')       as gd_geohash8 ,
    if(gd_longitude>0,aroundGeoHash8(gd_longitude,gd_latitude),'') as gd_around_geohash8,
    datahub_system_time                      , -- 上传datahub时间
    upload_delay_time                        , -- 上传到datahub的延迟时间
    from_unixtime(unix_timestamp()) as update_time        -- 更新时间
from tmp_dws_security_person_trajectory_rt_02;


-- 设备在线逻辑控制
insert into dws_device_status_info
select
    reportDeviceId     as device_id,
    from_unixtime(`reportTimeStamp`/1000) as acquire_timestamp_format,
    from_unixtime(unix_timestamp()) as update_time        -- 更新时间
from ja_device_index_kafka
where `reportTimeStamp` is not null;


-- *****************************************  国标 数据入库 *****************************************

insert into dws_security_person_trajectory_rt
select
    security_person_no                       , -- 安保人员编号
    data_source                              , -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    acquisition_time_format as acquisition_time,
    acquisition_time_format                  , -- 格式化采集时间
    security_person_name                     , -- 安保人员姓名
    security_person_card_id                  , -- 安保人员证件号
    security_person_phone_no                 , -- 安保人员手机号
    cast(null as varchar) as security_person_photo_url                , -- 安保人员头像url
    security_person_type                     , -- 安保人员类型
    security_person_type_name                , -- 安保人员类型名称 patrol 巡逻民警
    device_id                                , -- 设备编号（执法仪的编号和微信的设备号）
    device_name                              , -- 设备名称
    device_alias_name                        , -- 设备俗称
    walkie_talkie_no						   , -- 手台号、对讲机id、电台号
    device_organization_name                 , -- 设备所有机构名称
    cast(null as varchar) as wechat_acquisition_type                  , -- 警务微信的采集类型
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




-- 设置在线逻辑控制
insert into dws_device_status_info
select
    deviceId     as device_id,
    from_unixtime(`timeStamp`/1000) as acquire_timestamp_format,
    from_unixtime(unix_timestamp()) as update_time        -- 更新时间
from ja_device_index_kafka_guobiao
where `timeStamp` is not null;

end;





