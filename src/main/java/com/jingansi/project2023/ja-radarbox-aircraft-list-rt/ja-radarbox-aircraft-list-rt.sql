--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2023/5/13 11:09:25
-- description: radarbox网站的飞机数据
--********************************************************************--
set 'pipeline.name' = 'ja-radarbox-aircraft-list-rt';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'parallelism.default' = '4';
SET 'execution.checkpointing.interval' = '1200000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-radarbox-aircraft-list-rt-checkpoint' ;


 -----------------------

 -- 数据结构

 -----------------------


-- radarbox网站的飞机数据（Source：kafka）
drop table if exists radarbox_aircraft_list_kafka;
create table radarbox_aircraft_list_kafka(
                                             flightTraceId               string  ,  -- 航班跟踪ID
                                             flightNo                    string  ,  -- 航班号
                                             latitude                    string  ,  -- 飞机当前的纬度
                                             longitude                   string  ,  -- 飞机当前的经度
                                             acquireTimestamp            string  ,  -- 航班当前位置记录时间
                                             altitude                    string  ,  -- 航班当前高度，单位为（ft）
                                             flightType                  string  ,  -- 机型，表示飞行机的具体型号
                                             speed                       string  ,  -- 飞行当时的速度（单位：节）
                                             heading                     string  ,  -- 航班的飞行航向
                                             dataSource                  string  ,  -- 数据源，表示提供应记录的数据源类型
                                             registration                string  ,  -- 飞机的注册编号
                                             originAirport3Code          string  ,  -- 起飞机场的IATA代码
                                             destinationAirport3Code     string  ,  -- 目标机场的 IATA 代码
                                             airlinesIcao                string  ,  -- 航空公司的IATA代码
                                             num                         string  ,  --
                                             station                     string  ,  -- 航班级跟踪记录ID
                                             source                      string  ,  --
                                             flightSpecialFlag           boolean ,  -- 飞机是否在地面上
                                             sourcePosition  array <                -- 来源地坐标
                                                 double
                                                 >,
                                             destinationPosition  array <           -- 目的地坐标
                                                 double
                                                 >,
                                             flightStatus                string  ,  -- 航班当前状态
                                             num2                        string  ,  --
                                             expectedLandingTime         string  ,  -- 预计降落时间
                                             flightPhoto                 string  ,  -- 飞机的图片
                                             flightDepartureTime         string  ,  -- 表示航班起飞时间
                                             unKonwn                     string  ,  --
                                             toDestinationDistance       string  ,  -- 离目的地距离
                                             estimatedLandingDuration    string  ,  -- 预计还要多久着陆
                                             sMode                       string  ,   -- s模式
                                             estimatedLandingDurationFormat row(
                                                 flag string,
                                                 `hour` string,
                                                 `minute` string
                                                 )
) with (
      'connector' = 'kafka',
      'topic' = 'radarbox_aircraft_list',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'radarbox-aircraft-list-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1687340400000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );




-- radarbox网站的飞机数据-全量数据（Sink：kafka）
drop table if exists radarbox_aircraft_list_all_rt;
create table radarbox_aircraft_list_all_rt (
                                               flight_trace_id                string        comment '航班跟踪ID',
                                               acquire_timestamp_format       string        comment '航班当前位置记录时间格式化',
                                               flight_no                      string        comment '航班号',
                                               acquire_timestamp              string        comment '航班当前位置记录时间',
                                               latitude                       string        comment '飞机当前的纬度',
                                               longitude                      string        comment '飞机当前的经度',
                                               altitude                       string        comment '航班当前高度，单位为（ft）',
                                               altitude_m		               double        comment '航班当前高度，单位为（m）',
                                               flight_type                    string        comment '机型，表示飞行机的具体型号',
                                               speed                          string        comment '飞行当时的速度（单位：节）',
                                               speed_km		               double        comment '飞行当时的速度（单位：km/h）',
                                               heading                        string        comment '航班的飞行航向',
                                               data_source                    string        comment '数据源，表示提供应记录的数据源类型',
                                               registration                   string        comment '飞机的注册编号',
                                               origin_airport3_code           string        comment '起飞机场的IATA代码',
                                               destination_airport3_code      string        comment '目标机场的 IATA 代码',
                                               airlines_icao                  string        comment '航空公司的IATA代码',
                                               airlines_name 				   string        comment '航空公司中文',
                                               country_code 				   string        comment '飞机所属国家代码',
                                               num                            string        comment '',
                                               station                        string        comment '航班级跟踪记录ID',
                                               source                         string        comment '',
                                               flight_special_flag            boolean       comment '飞机是否在地面上',
                                               source_longitude               double  	     comment '来源机场经度',
                                               source_latitude                double  	     comment '来源机场纬度',
                                               destination_longitude          double        comment '目的地坐标经度',
                                               destination_latitude           double        comment '目的地坐标纬度',
                                               flight_status                  string        comment '航班当前状态',
                                               num2                           string        comment '',
                                               expected_landing_time          string        comment '预计降落时间',
                                               expected_landing_time_format   string        comment '预计降落时间格式化',
                                               flight_photo                   string        comment '飞机的图片',
                                               flight_departure_time          string        comment '表示航班起飞时间',
                                               flight_departure_time_format   string        comment '航班起飞时间格式化',
                                               un_konwn                       string        comment '',
                                               to_destination_distance        string        comment '离目的地距离',
                                               estimated_landing_duration     string        comment '预计还要多久着陆',
                                               s_mode                         string        comment 's模式',
                                               position_country_code2         string        comment '位置所在的国家',
                                               friend_foe 					   string        comment '敌我类型',
                                               update_time                    string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.27.95.211:30030',
      'table.identifier' = 'sa.dwd_aircraft_list_all_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='5000',
      'sink.batch.interval'='10s'
      );


-- radarbox网站的飞机数据-状态数据（Sink：kafka）
drop table if exists radarbox_aircraft_status_rt;
create table radarbox_aircraft_status_rt (
                                             flight_trace_id                string        comment '航班跟踪ID',
                                             acquire_timestamp_format       string        comment '航班当前位置记录时间格式化',
                                             flight_no                      string        comment '航班号',
                                             acquire_timestamp              string        comment '航班当前位置记录时间',
                                             latitude                       string        comment '飞机当前的纬度',
                                             longitude                      string        comment '飞机当前的经度',
                                             altitude                       string        comment '航班当前高度，单位为（ft）',
                                             altitude_m		               double        comment '航班当前高度，单位为（m）',
                                             flight_type                    string        comment '机型，表示飞行机的具体型号',
                                             speed                          string        comment '飞行当时的速度（单位：节）',
                                             speed_km		               double        comment '飞行当时的速度（单位：km/h）',
                                             heading                        string        comment '航班的飞行航向',
                                             data_source                    string        comment '数据源，表示提供应记录的数据源类型',
                                             registration                   string        comment '飞机的注册编号',
                                             origin_airport3_code           string        comment '起飞机场的IATA代码',
                                             destination_airport3_code      string        comment '目标机场的 IATA 代码',
                                             airlines_icao                  string        comment '航空公司的IATA代码',
                                             airlines_name 				   string        comment '航空公司中文',
                                             country_code 				   string        comment '飞机所属国家代码',
                                             num                            string        comment '',
                                             station                        string        comment '航班级跟踪记录ID',
                                             source                         string        comment '',
                                             flight_special_flag            boolean       comment '飞机是否在地面上',
                                             source_longitude               double  	     comment '来源机场经度',
                                             source_latitude                double  	     comment '来源机场纬度',
                                             destination_longitude          double        comment '目的地坐标经度',
                                             destination_latitude           double        comment '目的地坐标纬度',
                                             flight_status                  string        comment '航班当前状态',
                                             num2                           string        comment '',
                                             expected_landing_time          string        comment '预计降落时间',
                                             expected_landing_time_format   string        comment '预计降落时间格式化',
                                             flight_photo                   string        comment '飞机的图片',
                                             flight_departure_time          string        comment '航班起飞时间',
                                             flight_departure_time_format   string        comment '航班起飞时间格式化',
                                             un_konwn                       string        comment '',
                                             to_destination_distance        string        comment '离目的地距离',
                                             estimated_landing_duration     string        comment '预计还要多久着陆',
                                             s_mode                         string        comment 's模式',
                                             position_country_code2         string        comment '位置所在的国家',
                                             friend_foe 					   string        comment '敌我类型',
                                             update_time                    string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.27.95.211:30030',
      'table.identifier' = 'sa.dws_aircraft_list_status_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='5000',
      'sink.batch.interval'='10s'
      );



-- 航空公司匹配库国家表（Source：doris）
drop table if exists dim_airline_3_2_code_info;
create table dim_airline_3_2_code_info (
                                           code3                    string     comment '三字码',
                                           airlines_chinese_name    string     comment '航空公司中文名中文名称',
                                           country_name             string     comment '国家',
                                           primary key (code3) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_airline_3_2_code_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );


-- 位置所在的国家代码转换（Source：doris）
drop table if exists dim_country_info;
create table dim_country_info (
                                  code2              string        comment '地区国家两位编码',
                                  code3              string        comment '地区国家三位编码',
                                  primary key (code3) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );

-- 航空器国籍登记代码表
drop table if exists dim_aircraft_country_prefix_code;
create table dim_aircraft_country_prefix_code (
                                                  prefix_code 	string  COMMENT '代码前缀',
                                                  country_code 	string  COMMENT '国家代码',
                                                  primary key (prefix_code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_aircraft_country_prefix_code',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );

-- create function getCountry as 'GetCountryFromLngLat.getCountryFromLngLat' language python ;

create function getCountry as 'com.jingan.udf.GetCountryFromLngLat';

---------------

-- 数据处理

---------------


-- 对数据字段进行处理筛选
drop table if exists tmp_radarbox_aircraft_01;
create view tmp_radarbox_aircraft_01 as
select
    flightTraceId             as flight_trace_id,
    flightNo                  as flight_no,
    from_unixtime(cast(acquireTimestamp as bigint)/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format, -- 时间戳格式化
    to_timestamp(from_unixtime(cast(acquireTimestamp as bigint)/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format_date, -- 时间类型的年月日时分秒
    acquireTimestamp          as acquire_timestamp,
    latitude                  as latitude,
    longitude                 as longitude,
    altitude                  as altitude,
    flightType                as flight_type,
    speed                     as speed,
    heading                   as heading,
    dataSource                as data_source,
    registration              as registration,
    originAirport3Code        as origin_airport3_code,
    destinationAirport3Code   as destination_airport3_code,
    airlinesIcao              as airlines_icao,
    num                       as num,
    station                   as station,
    source                    as source,
    flightSpecialFlag         as flight_special_flag,
    sourcePosition[1]         as source_longitude,
    sourcePosition[2]         as source_latitude,
    destinationPosition[1]    as destination_longitude,
    destinationPosition[2]    as destination_latitude,
    flightStatus              as flight_status,
    num2                      as num2,
    expectedLandingTime       as expected_landing_time,  -- 到达
    flightPhoto               as flight_photo,
    flightDepartureTime       as flight_departure_time,  -- 出发
    unKonwn                   as un_konwn,
    toDestinationDistance     as to_destination_distance,
    estimatedLandingDuration  as estimated_landing_duration,
    sMode                     as s_mode,
    estimatedLandingDurationFormat as estimated_landing_duration_format,
    split_index(expectedLandingTime,':',0) as expected_landing_time_hour,
    split_index(expectedLandingTime,':',1) as expected_landing_time_minute,
    split_index(flightDepartureTime,':',0) as flight_departure_time_hour,
    split_index(flightDepartureTime,':',1) as flight_departure_time_minute,
    getCountry(cast(longitude as double),cast(latitude as double)) as country_code3, -- 经纬度位置转换国家
    if(instr(registration,'-')>0,substring(registration,1,2),concat(substring(registration,1,1),'-')) as prefix_code2,
    if(instr(registration,'-')>0,substring(registration,1,3),concat(substring(registration,1,2),'-')) as prefix_code3,
    if(instr(registration,'-')>0,substring(registration,1,4),concat(substring(registration,1,3),'-')) as prefix_code4,
    if(instr(registration,'-')>0,substring(registration,1,5),concat(substring(registration,1,4),'-')) as prefix_code5,
    PROCTIME()  as proctime
from radarbox_aircraft_list_kafka
where acquireTimestamp is not null
  and flightTraceId is not null
  and flightTraceId <> '';




-- 对数据进行处理，加减时间得到起飞时间和到达时间
drop table if exists tmp_radarbox_aircraft_02_pre_00;
create view tmp_radarbox_aircraft_02_pre_00 as
select
    t1.*,
    if(estimated_landing_duration_format.flag = 'm',
       cast(timestampadd(minute,cast(estimated_landing_duration_format.`minute` as int),acquire_timestamp_format_date)as string),
       cast(timestampadd(hour,cast(estimated_landing_duration_format.`hour` as int),timestampadd(minute,cast(estimated_landing_duration_format.`minute` as int),acquire_timestamp_format_date)) as string)
        ) as expected_landing_time_format,
    case
        when flight_departure_time_hour < expected_landing_time_hour then concat(cast(CURRENT_DATE as string),' ',flight_departure_time,':00')
        when flight_departure_time_hour = expected_landing_time_hour and flight_departure_time_minute < expected_landing_time_minute then concat(cast(CURRENT_DATE as string),' ',flight_departure_time,':00')
        when flight_departure_time_hour > expected_landing_time_hour then concat(cast(timestampadd(day,-1,CURRENT_DATE) as string),' ',flight_departure_time,':00')
        end as flight_departure_time_format,
    t2.airlines_chinese_name as airlines_name,
    t2.country_name,
    t3.code2 as position_country_2code,
    -- 这是一个flank bug 这样取不到值
    -- coalesce(t7.country_code,t6.country_code,t5.country_code,t4.country_code) as country_code,
    t7.country_code as country_code7,
    t6.country_code as country_code6,
    t5.country_code as country_code5,
    t4.country_code as country_code4
from tmp_radarbox_aircraft_01 as t1
         left join dim_airline_3_2_code_info
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.airlines_icao = t2.code3
         left join dim_country_info
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.country_code3=t3.code3
         left join dim_aircraft_country_prefix_code
    FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.prefix_code2=t4.prefix_code
         left join dim_aircraft_country_prefix_code
    FOR SYSTEM_TIME AS OF t1.proctime as t5
                   on t1.prefix_code3=t5.prefix_code
         left join dim_aircraft_country_prefix_code
    FOR SYSTEM_TIME AS OF t1.proctime as t6
                   on t1.prefix_code4=t6.prefix_code
         left join dim_aircraft_country_prefix_code
    FOR SYSTEM_TIME AS OF t1.proctime as t7
                   on t1.prefix_code5=t7.prefix_code;

drop table if exists tmp_radarbox_aircraft_02_pre;
create view tmp_radarbox_aircraft_02_pre as
select
    *,
    if(position_country_2code is null
           and ((cast(longitude as double) between 107.491636 and 124.806089 and cast(latitude as double) between 20.522241 and 40.799277)
            or
                (cast(longitude as double) between 107.491636 and 121.433286 and cast(latitude as double) between 3.011639 and 20.522241)
           )
        ,'CN', position_country_2code) as position_country_code2,
    -- 这样才能取到值
    coalesce(country_code7,country_code6,country_code5,country_code4) as country_code
from tmp_radarbox_aircraft_02_pre_00;

-- 敌我识别
drop table if exists tmp_radarbox_aircraft_02;
create view tmp_radarbox_aircraft_02 as
select
    *,
    if(country_code='CN','FRIENDLY_SIDE','NEUTRALITY') as friend_foe
from tmp_radarbox_aircraft_02_pre;


-----------------------

-- 数据插入

-----------------------

begin statement set;

-- radarbox网站的飞机数据入库全量数据
insert into radarbox_aircraft_list_all_rt
select
    flight_trace_id,
    acquire_timestamp_format,
    flight_no,
    acquire_timestamp,
    latitude,
    longitude,
    altitude,
    altitude*0.3048 as altitude_m,
    flight_type,
    speed,
    speed*1.852 as speed_km,
    heading,
    data_source,
    registration,
    origin_airport3_code,
    destination_airport3_code,
    airlines_icao,
    airlines_name,
    country_code,
    num,
    station,
    source,
    flight_special_flag,
    source_longitude,
    source_latitude,
    destination_longitude,
    destination_latitude,
    flight_status,
    num2,
    expected_landing_time,
    expected_landing_time_format,
    flight_photo,
    flight_departure_time,
    flight_departure_time_format,
    un_konwn,
    to_destination_distance,
    estimated_landing_duration,
    s_mode,
    position_country_code2,
    friend_foe,
    from_unixtime(unix_timestamp()) as update_time
from tmp_radarbox_aircraft_02;



-- radarbox网站的飞机数据入库全量数据
insert into radarbox_aircraft_status_rt
select
    flight_trace_id,
    acquire_timestamp_format,
    flight_no,
    acquire_timestamp,
    latitude,
    longitude,
    altitude,
    flight_type,
    speed,
    heading,
    data_source,
    registration,
    origin_airport3_code,
    destination_airport3_code,
    airlines_icao,
    airlines_name,
    country_code,
    num,
    station,
    source,
    flight_special_flag,
    source_longitude,
    source_latitude,
    destination_longitude,
    destination_latitude,
    flight_status,
    num2,
    expected_landing_time,
    expected_landing_time_format,
    flight_photo,
    flight_departure_time,
    flight_departure_time_format,
    un_konwn,
    to_destination_distance,
    estimated_landing_duration,
    s_mode,
    position_country_code2,
    friend_foe,
    from_unixtime(unix_timestamp()) as update_time
from tmp_radarbox_aircraft_02;


end;


-- -- flightaware飞机数据入库精确到分钟
-- insert into dwd_ads_b_flightaware_rt_minute
-- select
--   flight_no                         as flight_id,
--   acquire_timestamp_format          as request_time,
--   cast(acquire_timestamp as bigint) as request_timestamp,
--   cast(longitude as double)         as lng,
--   cast(latitude as double)          as lat,
--   flight_no                         as ident,
--   cast(null as varchar)             as prefix,
--   cast(heading as double)           as direction,
--   cast(null as varchar)             as aircraft_type,
--   cast(null as boolean)             as ga,
--   cast(null as varchar)             as landing_times_estimated,

--   -- concat(substring(cast(from_unixtime(unix_timestamp()) as string),0,11),expected_landing_time,':00')
--   --            as landing_times_estimated,

--   origin_airport3_code              as origin_icao,
--   cast(null as varchar)             as origin_iata,
--   cast(null as boolean)             as origin_is_us_airport,
--   destination_airport3_code         as destination_icao,
--   cast(null as varchar)             as destination_iata,
--   cast(null as varchar)             as destination_tz,
--   cast(null as boolean)             as destination_is_us_airport,
--   cast(null as bigint)              as prominence,
--   cast(null as varchar)             as flight_type,
--   cast(null as int)                 as projected,
--   cast(altitude as int)             as altitude,
--   cast(null as varchar)             as altitude_change,
--   cast(speed as int)                as ground_speed,
--   cast(null as varchar)             as icao,
--   cast(null as varchar)             as geometry_type,
--   cast(null as varchar)             as type,
--   from_unixtime(unix_timestamp()) as update_time
-- from tmp_radarbox_aircraft_01
--   where flight_no is not null
--     and data_source <> 'ADSB';









create table dim_aircraft_model_info (
                                         id                             string        comment '飞机型号',
                                         link                           string        comment '详细信息地址',
                                         date_input                     string        comment '未知',
                                         img_acquire_address            string        comment 'img图片地址',
                                         minio_url                      string        comment 'minio地址',
                                         update_time                    timestamp     comment '创建时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.27.95.211:30031',
      'table.identifier' = 'sa.dim_aircraft_model_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='100000',
      'sink.batch.interval'='10s'
      );