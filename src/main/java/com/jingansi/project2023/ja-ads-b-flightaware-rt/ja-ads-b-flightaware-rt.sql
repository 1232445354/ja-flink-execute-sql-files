--********************************************************************--
-- author:      write your name here
-- create time: 2023/2/20 16:29:13
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ja-ads-b-flightaware-rt';
set 'table.exec.state.ttl' = '600000';
SET 'parallelism.default' = '5';

SET 'execution.checkpointing.interval' = '10000';
SET 'state.checkpoints.dir' = 's3://flink/ja-ads-b-flightaware-rt';
-- 空闲分区不用等待
set 'table.exec.source.idle-timeout' = '3s';

drop table  if exists flightaware_collect_item;
create table flightaware_collect_item (
  type     string ,
  collectTime bigint,
  geometry row(
           type string,
           coordinates array<double>
           ),
  properties row(
             flight_id string,
             altitude int,
             altitudeChange string,
             prefix string,
             ident string,
             icon string,
             groundspeed int,
             flightType string,
             type  string,
             projected int,
             ga boolean,
             direction int,
             prominence int,
             landingTimes row(
                     estimated bigint
             ),
             origin row(
                    isUSAirport boolean,
                    iata string,
                    icao string
             ),
             destination row(
                         isUSAirport boolean,
                         iata string,
                         icao string,
                         TZ string
             )
  ),
  -- ts TIMESTAMP(3) METADATA FROM 'timestamp',
  ts as to_timestamp_ltz(collectTime,3) ,
  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'flightaware_collect_item',
  'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
  'properties.group.id' = 'ja-ads-b-flightaware-rt',
  -- 'scan.startup.mode' = 'latest-offset',
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp-millis' = '1677643207000',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);


-- 数据入库全量(Sink:doris）
drop table if exists dwd_ads_b_flightaware_rt;
create table dwd_ads_b_flightaware_rt (
  flight_id						string 	    COMMENT '飞行ID',
  request_time					timestamp	COMMENT '时间',
  request_timestamp				bigint		COMMENT '时间戳',
  lng							double		COMMENT '经度',
  lat							double		COMMENT '纬度',
  ident							string		COMMENT '航班编号',
  prefix						string		COMMENT '飞行编号前缀',
  direction						double		COMMENT '方向、航向',
  aircraft_type					string		COMMENT '飞机类型',
  ga							boolean		COMMENT '是否通用航空',
  landing_times_estimated		timestamp	COMMENT '预计着陆时间',
  origin_icao					string		COMMENT '民航组织代码 三字代码',
  origin_iata					string		COMMENT '国际航协会代码 四字代码',
  origin_is_us_airport			boolean		COMMENT '是否是美国机场',
  destination_icao				string		COMMENT '民航组织代码 三字代码',
  destination_iata				string		COMMENT '国际航协会代码 四字代码',
  destination_tz				string		COMMENT '时区',
  destination_is_us_airport		boolean		COMMENT '是否是美国机场',
  prominence					bigint		COMMENT '凸出度',
  flight_type					string		COMMENT '飞行类型',
  projected						int			COMMENT '预测值',
  altitude						int			COMMENT '海拔高度',
  altitude_change				string		COMMENT '海拔高度变化',
  ground_speed					int			COMMENT '地速(飞机相对于地面的速度)',
  icon							string		COMMENT '图标',
  geometry_type					string		COMMENT '几何类型',
  type							string		COMMENT '几何类型',
  update_time					timestamp	COMMENT '更新时间'
)WITH (
'connector' = 'doris',
'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:30030',
  'fenodes' = '172.21.30.202:30030',
'table.identifier' = 'kesadaran_situasional.dwd_ads_b_flightaware_all_rt',
'username' = 'admin',
'password' = 'admin',
'doris.request.tablet.size'='1',
'doris.request.read.timeout.ms'='30000',
'sink.batch.size'='100000',
'sink.batch.interval'='5s'
);


-- 数据入库精确到分钟(Sink:doris)
drop table if exists dwd_ads_b_flightaware_rt_minute;
create table dwd_ads_b_flightaware_rt_minute (
  flight_id						string 	    COMMENT '飞行ID',
  request_time					string	    COMMENT '时间',
  request_timestamp				bigint		COMMENT '时间戳',
  lng							double		COMMENT '经度',
  lat							double		COMMENT '纬度',
  ident							string		COMMENT '航班编号',
  prefix						string		COMMENT '飞行编号前缀',
  direction						double		COMMENT '方向、航向',
  aircraft_type					string		COMMENT '飞机类型',
  ga							boolean		COMMENT '是否通用航空',
  landing_times_estimated		timestamp	COMMENT '预计着陆时间',
  origin_icao					string		COMMENT '民航组织代码 三字代码',
  origin_iata					string		COMMENT '国际航协会代码 四字代码',
  origin_is_us_airport			boolean		COMMENT '是否是美国机场',
  destination_icao				string		COMMENT '民航组织代码 三字代码',
  destination_iata				string		COMMENT '国际航协会代码 四字代码',
  destination_tz				string		COMMENT '时区',
  destination_is_us_airport		boolean		COMMENT '是否是美国机场',
  prominence					bigint		COMMENT '凸出度',
  flight_type					string		COMMENT '飞行类型',
  projected						int			COMMENT '预测值',
  altitude						int			COMMENT '海拔高度',
  altitude_change				string		COMMENT '海拔高度变化',
  ground_speed					int			COMMENT '地速(飞机相对于地面的速度)',
  icon							string		COMMENT '图标',
  geometry_type					string		COMMENT '几何类型',
  type							string		COMMENT '几何类型',
  update_time					timestamp	COMMENT '更新时间'
)WITH (
'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:30030',
  'fenodes' = '172.21.30.202:30030',
'table.identifier' = 'kesadaran_situasional.dwd_ads_b_flightaware_rt',
'username' = 'admin',
'password' = 'admin',
'doris.request.tablet.size'='1',
'doris.request.read.timeout.ms'='30000',
'sink.batch.size'='100000',
'sink.batch.interval'='5s'
);


-- 数据处理
drop view if exists tmp_data_01;
create view tmp_data_01 as
select
  *
  from (
  select
    properties.flight_id                    as flight_id				    , -- 飞行ID'
    -- from_unixtime（collectTime,'yyyy-MM-dd HH:mm'）as request_time_minute,
    to_timestamp_ltz(collectTime,3)         as request_time					, -- 时间',
    collectTime/1000                        as request_timestamp			, -- 时间戳',
    geometry.coordinates[1]                 as lng							, -- 经度',
    geometry.coordinates[2]                 as lat							, -- 纬度',
    properties.ident                        as ident						, -- 航班编号',
    properties.prefix                       as prefix						, -- 飞行编号前缀',
    properties.direction                    as direction					, -- 方向、航向',
    properties.type                         as aircraft_type				, -- 飞机类型',
    properties.ga                           as ga							, -- 是否通用航空',
    to_timestamp_ltz(properties.landingTimes.estimated,0) as landing_times_estimated		, -- 预计着陆时间',
    properties.origin.icao                  as origin_icao					, -- 民航组织代码 三字代码',
    properties.origin.iata                  as origin_iata					, -- 国际航协会代码 四字代码',
    properties.origin.isUSAirport           as origin_is_us_airport			, -- 是否是美国机场',
    properties.destination.icao             as destination_icao				, -- 民航组织代码 三字代码',
    properties.destination.iata             as destination_iata				, -- 国际航协会代码 四字代码',
    properties.destination.TZ               as destination_tz				, -- 时区',
    properties.destination.isUSAirport      as destination_is_us_airport	, -- 是否是美国机场',
    properties.prominence                   as prominence					, -- 凸出度',
    properties.flightType                   as flight_type					, -- 飞行类型',
    properties.projected                    as projected				    , -- 预测值',
    properties.altitude                     as altitude						, -- 海拔高度',
    properties.altitudeChange               as altitude_change				, -- 海拔高度变化',
    properties.groundspeed                  as ground_speed					, -- 地速(飞机相对于地面的速度)',
    properties.icon                         as icon							, -- 图标',
    geometry.type                           as geometry_type				, -- 几何类型',
    type							        as type                         , -- 几何类型',
    -- to_timestamp_ltz(unix_timestamp(),0)    as update_time			      -- 更新时间'
    count(*)over(partition by flight_id,geometry.coordinates[1],geometry.coordinates[2] order by ts) as cnt
  from flightaware_collect_item
where collectTime is not null
  ) as t1
  where cnt = 1;


-- 数据插入精确到分钟  2023-02-23 16:30:52.084
begin statement set;

insert into dwd_ads_b_flightaware_rt_minute
select
   flight_id				    , -- 飞行ID',
   substring(cast(request_time as string),1,16) as  request_time,
   -- request_time				, -- 时间',
   request_timestamp		    , -- 时间戳',
   lng							, -- 经度',
   lat							, -- 纬度',
   ident						, -- 航班编号',
   prefix						, -- 飞行编号前缀',
   direction				    , -- 方向、航向',
   aircraft_type			    , -- 飞机类型',
   ga							, -- 是否通用航空',
   landing_times_estimated		, -- 预计着陆时间',
   origin_icao					, -- 民航组织代码 三字代码',
   origin_iata					, -- 国际航协会代码 四字代码',
   origin_is_us_airport			, -- 是否是美国机场',
   destination_icao				, -- 民航组织代码 三字代码',
   destination_iata				, -- 国际航协会代码 四字代码',
   destination_tz				, -- 时区',
   destination_is_us_airport    , -- 是否是美国机场',
   prominence					, -- 凸出度',
   flight_type					, -- 飞行类型',
   projected				    , -- 预测值',
   altitude						, -- 海拔高度',
   altitude_change				, -- 海拔高度变化',
   ground_speed					, -- 地速(飞机相对于地面的速度)',
   icon							, -- 图标',
   geometry_type				, -- 几何类型',
   type                         , -- 几何类型',
  to_timestamp_ltz(unix_timestamp(),0)    as update_time					  -- 更新时间'
from tmp_data_01;


-- 全量的数据插入
insert into dwd_ads_b_flightaware_rt
select
   flight_id				    , -- 飞行ID',
   request_time					, -- 时间',
   request_timestamp		    , -- 时间戳',
   lng							, -- 经度',
   lat							, -- 纬度',
   ident						, -- 航班编号',
   prefix						, -- 飞行编号前缀',
   direction				    , -- 方向、航向',
   aircraft_type			    , -- 飞机类型',
   ga							, -- 是否通用航空',
   landing_times_estimated		, -- 预计着陆时间',
   origin_icao					, -- 民航组织代码 三字代码',
   origin_iata					, -- 国际航协会代码 四字代码',
   origin_is_us_airport			, -- 是否是美国机场',
   destination_icao				, -- 民航组织代码 三字代码',
   destination_iata				, -- 国际航协会代码 四字代码',
   destination_tz				, -- 时区',
   destination_is_us_airport    , -- 是否是美国机场',
   prominence					, -- 凸出度',
   flight_type					, -- 飞行类型',
   projected				    , -- 预测值',
   altitude						, -- 海拔高度',
   altitude_change				, -- 海拔高度变化',
   ground_speed					, -- 地速(飞机相对于地面的速度)',
   icon							, -- 图标',
   geometry_type				, -- 几何类型',
   type                         , -- 几何类型',
  to_timestamp_ltz(unix_timestamp(),0)   as update_time					  -- 更新时间'
from tmp_data_01;

end;