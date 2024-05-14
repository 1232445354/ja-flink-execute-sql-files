set 'pipeline.name' = 'fly-radarbox-real-data';

set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'sql-client.execution.result-mode' = 'TABLEAU';


SET 'table.exec.state.ttl' = '300000';
SET 'parallelism.default' = '4';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '1200000';
set 'state.checkpoints.dir' = 's3://ja-bice2/flink-checkpoints/fly-radarbox-real-data';


-- radarbox数据
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
                                                 ),
                                             proctime          as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'radarbox_aircraft_list',
      'properties.bootstrap.servers' = '47.111.155.82:30097',
      'properties.group.id' = 'radarbox-ceshi1',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- doris全量表
drop table if exists ods_flight_all_track;
create table ods_flight_all_track (
                                      id                      string  , -- 飞机id
                                      flag                    int  , --  标识
                                      time1                   string  , --  采集时间
                                      s_mode                  string  , --  s模式
                                      registration_number     string  , --  注册号
                                      flight_number           string  , --  航班号
                                      model                   string  , --  型号
                                      `type`                  string  , --  类型
                                      longitude02             double  , --  经度
                                      latitude02              double  , --  纬度
                                      speed_km                double  , --  速度
                                      height                  double  , --  高度
                                      heading                 double  , --  方向
                                      fly_status              string  , --   状态
                                      country_flag            string  , --  国家代码
                                      country_chinese_name    string  , --  国家名称
                                      origin                  string  , --  来源
                                      airline_code            string  , --  航空公司的icao代码
                                      responder_code          string  , --  应答机代码
                                      take_off_code           string  , --  起飞机场的iata代码
                                      origin_longitude02      double  , --  来源机场经度
                                      origin_latitude02       double  , --  来源机场纬度
                                      land_code               string  , --  目标机场的 iata 代码
                                      destination_longitude02 double  , --  目的地坐标经度
                                      destination_latitude02  double  , --  目的地坐标纬度
                                      `image`                 string  , --  飞机的图片
                                      take_off_time           string  , --  航班起飞时间
                                      land_time               string  , --  预计降落时间
                                      during                  string  , --  预计多久着陆
                                      gmt_create              string   --  入库时间
) with (
      'connector' = 'doris',
      'fenodes' = '8.130.39.51:8030',
      'table.identifier' = 'global_entity.ods_flight_all_track',
      'username' = 'admin',
      'password' = 'yshj@yshj',
      'sink.enable.batch-mode'='true',
      'sink.buffer-flush.max-rows'='10000',
      'sink.buffer-flush.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- doris状态表
drop table if exists dwd_flight_all_track;
create table dwd_flight_all_track (
                                      id                      string  , -- 飞机id
                                      flag                    int  , --  标识
                                      time1                   string  , --  采集时间
                                      s_mode                  string  , --  s模式
                                      registration_number     string  , --  注册号
                                      flight_number           string  , --  航班号
                                      model                   string  , --  型号
                                      `type`                  string  , --  类型
                                      longitude02             double  , --  经度
                                      latitude02              double  , --  纬度
                                      speed_km                double  , --  速度
                                      height                  double  , --  高度
                                      heading                 double  , --  方向
                                      fly_status              string  , --   状态
                                      country_flag            string  , --  国家代码
                                      country_chinese_name    string  , --  国家名称
                                      origin                  string  , --  来源
                                      airline_code            string  , --  航空公司的icao代码
                                      responder_code          string  , --  应答机代码
                                      take_off_code           string  , --  起飞机场的iata代码
                                      origin_longitude02      double  , --  来源机场经度
                                      origin_latitude02       double  , --  来源机场纬度
                                      land_code               string  , --  目标机场的 iata 代码
                                      destination_longitude02 double  , --  目的地坐标经度
                                      destination_latitude02  double  , --  目的地坐标纬度
                                      `image`                 string  , --  飞机的图片
                                      take_off_time           string  , --  航班起飞时间
                                      land_time               string  , --  预计降落时间
                                      during                  string  , --  预计多久着陆
                                      gmt_create              string   --  入库时间
) with (
      'connector' = 'doris',
      'fenodes' = '8.130.39.51:8030',
      'table.identifier' = 'global_entity.dwd_flight_all_track',
      'username' = 'admin',
      'password' = 'yshj@yshj',
      'sink.enable.batch-mode'='true',
      'sink.buffer-flush.max-rows'='10000',
      'sink.buffer-flush.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- 飞机航班表1
drop table if exists dws_flight_number_info1;
create table dws_flight_number_info1 (
                                         id      string  , -- 飞机id
                                         sms     string  , --  s模式
                                         zch     string  , --  注册号
                                         hbh     string  , --  航班号
                                         hb_id   string  , -- 航班id
                                         xh      string  , --  型号
                                         cjsj    string  , --  采集时间
                                         rksj    string   --  入库时间
) with (
      'connector' = 'doris',
      'fenodes' = '8.130.39.51:8030',
      'table.identifier' = 'global_entity.dws_flight_number_info1',
      'username' = 'admin',
      'password' = 'yshj@yshj',
      'sink.enable.batch-mode'='true',
      'sink.buffer-flush.max-rows'='10000',
      'sink.buffer-flush.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 航空器国籍登记代码表
drop table if exists dim_aircraft_prefix_code;
create table dim_aircraft_prefix_code (
                                          prefix_code 	string  COMMENT '代码前缀',
                                          country_code 	string  COMMENT '国家代码',
                                          primary key (prefix_code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://8.130.39.51:9030/global_entity?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'yshj@yshj',
      'table-name' = 'dim_prefix',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );


-- ---------------------
-- 数据处理
-- ---------------------

-- 对数据进行整理，数据过滤
drop view if exists tmp_radarbox_aircraft_01;
create view tmp_radarbox_aircraft_01 as
select
    flightTraceId                                                                              as flight_trace_id,
    if(flightNo in ('BLOCKED','VARIOUS','TACTICAL',''),cast(null as varchar),flightNo)                                           as flight_no,
    from_unixtime(cast(acquireTimestamp as bigint)/1000,'yyyy-MM-dd HH:mm:ss')                 as acquire_timestamp_format, -- 时间戳格式化
    to_timestamp(from_unixtime(cast(acquireTimestamp as bigint)/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format_date, -- 时间类型的年月日时分秒
    acquireTimestamp                                                                           as acquire_timestamp,
    latitude                                                                                   as latitude,
    longitude                                                                                  as longitude,
    altitude                                                                                   as altitude,
    if(flightType = '',cast(flightType as varchar),flightType)                       as flight_type,
    if(speed = '',cast(null as varchar),speed)                                                 as speed,
    if(heading = '',cast(null as varchar),heading)                                             as heading,
    dataSource                                                                                 as data_source,
    if(registration in ('BLOCKED','VARIOUS','TACTICAL',''),cast(null as varchar),registration) as registration,
    if(originAirport3Code = '',cast(null as varchar),originAirport3Code)                       as origin_airport3_code,
    if(destinationAirport3Code='',cast(null as varchar),destinationAirport3Code)               as destination_airport3_code,
    if(airlinesIcao = '',cast(null as varchar),airlinesIcao)                                   as airlines_icao,
    if(num = '',cast(null as varchar),num)                                                     as num,
    if(station = '',cast(null as varchar),station)                                             as station,
    source                                                                                     as source,
    flightSpecialFlag                                                                          as flight_special_flag,
    sourcePosition[1]                                                                          as source_longitude,
    sourcePosition[2]                                                                          as source_latitude,
    destinationPosition[1]                                                                     as destination_longitude,
    destinationPosition[2]                                                                     as destination_latitude,
    flightStatus                                                                               as flight_status,
    if(num2 = '',cast(null as varchar),num2)                                                   as squawk_code, -- 当前应答机代码
    expectedLandingTime                                                                        as expected_landing_time,  -- 到达
    if(flightPhoto='',cast(null as varchar),flightPhoto)                                       as flight_photo,
    flightDepartureTime                                                                        as flight_departure_time,  -- 出发
    unKonwn                                                                                    as un_konwn,
    toDestinationDistance                                                                      as to_destination_distance,
    estimatedLandingDuration                                                                   as estimated_landing_duration,
    if(sMode='',cast(null as varchar),sMode)                                                   as s_mode,
    estimatedLandingDurationFormat                                                             as estimated_landing_duration_format,
    split_index(expectedLandingTime,':',0)                                                     as expected_landing_time_hour,
    split_index(expectedLandingTime,':',1)                                                     as expected_landing_time_minute,
    split_index(flightDepartureTime,':',0)                                                     as flight_departure_time_hour,
    split_index(flightDepartureTime,':',1)                                                     as flight_departure_time_minute,
    proctime
from radarbox_aircraft_list_kafka
where acquireTimestamp is not null
  and flightTraceId is not null
  and flightTraceId <> '';



-- 切分注册号
drop view if exists tmp_radarbox_aircraft_02;
create view tmp_radarbox_aircraft_02 as
select
    *,
    if(instr(registration,'-')>0,substring(registration,1,2),concat(substring(registration,1,1),'-')) as prefix_code2,
    if(instr(registration,'-')>0,substring(registration,1,3),concat(substring(registration,1,2),'-')) as prefix_code3,
    if(instr(registration,'-')>0,substring(registration,1,4),concat(substring(registration,1,3),'-')) as prefix_code4,
    if(instr(registration,'-')>0,substring(registration,1,5),concat(substring(registration,1,4),'-')) as prefix_code5
from tmp_radarbox_aircraft_01;



-- 加减时间得到起飞时间和到达时间
drop view if exists tmp_radarbox_aircraft_03;
create view tmp_radarbox_aircraft_03 as
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

    t7.country_code as country_code7,
    t6.country_code as country_code6,
    t5.country_code as country_code5,
    t4.country_code as country_code4
from tmp_radarbox_aircraft_02 as t1

         left join dim_aircraft_prefix_code
    FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.prefix_code2=t4.prefix_code
         left join dim_aircraft_prefix_code
    FOR SYSTEM_TIME AS OF t1.proctime as t5
                   on t1.prefix_code3=t5.prefix_code
         left join dim_aircraft_prefix_code
    FOR SYSTEM_TIME AS OF t1.proctime as t6
                   on t1.prefix_code4=t6.prefix_code
         left join dim_aircraft_prefix_code
    FOR SYSTEM_TIME AS OF t1.proctime as t7
                   on t1.prefix_code5=t7.prefix_code;


-- 规整字段
drop view if exists tmp_radarbox_aircraft_04;
create view tmp_radarbox_aircraft_04 as
select
    if(s_mode is null,flight_trace_id,s_mode)       as id,
    acquire_timestamp_format                        as time1,
    1                                               as flag,
    s_mode                                          as s_mode,
    registration                                    as registration_number,
    flight_no                                       as flight_number,
    flight_trace_id                                 as trace_id,
    flight_type                                     as model,
    cast(null as varchar)                           as `type`,
    cast(longitude as double)                       as longitude02,
    cast(latitude as double)                        as latitude02,
    cast(speed as double) * 1.852                   as speed_km,
    cast(altitude as double) * 0.3048               as height,
    cast(heading as double)                         as heading,
    flight_status                                   as fly_status,
    if(registration is null,cast(null as varchar),coalesce(country_code7,country_code6,country_code5,country_code4))  as country_flag,
    cast(null as varchar)                           as country_chinese_name,
    `data_source`                                   as origin,
    airlines_icao                                   as airline_code,
    squawk_code                                     as responder_code,
    origin_airport3_code                            as take_off_code,
    source_longitude                                as origin_longitude02,
    source_latitude                                 as origin_latitude02,
    destination_airport3_code                       as land_code,
    destination_longitude                           as destination_longitude02,
    destination_latitude                            as destination_latitude02,
    flight_photo                                    as `image`,
    flight_departure_time_format                    as take_off_time,
    expected_landing_time_format                    as land_time,
    estimated_landing_duration                      as during,
    from_unixtime(unix_timestamp())                 as gmt_create    -- 数据入库时间
from tmp_radarbox_aircraft_03;


-- ---------------------
-- 数据入库
-- ---------------------

begin statement set;

-- 轨迹表
insert into ods_flight_all_track
select
    id
     ,flag
     ,time1
     ,s_mode
     ,registration_number
     ,flight_number
     ,model
     ,`type`
     ,longitude02
     ,latitude02
     ,speed_km
     ,height
     ,heading
     ,fly_status
     ,country_flag
     ,country_chinese_name
     ,origin
     ,airline_code
     ,responder_code
     ,take_off_code
     ,origin_longitude02
     ,origin_latitude02
     ,land_code
     ,destination_longitude02
     ,destination_latitude02
     ,`image`
     ,take_off_time
     ,land_time
     ,during
     ,gmt_create
from tmp_radarbox_aircraft_04;


-- 状态表
insert into dwd_flight_all_track
select
    id
     ,flag
     ,time1
     ,s_mode
     ,registration_number
     ,flight_number
     ,model
     ,`type`
     ,longitude02
     ,latitude02
     ,speed_km
     ,height
     ,heading
     ,fly_status
     ,country_flag
     ,country_chinese_name
     ,origin
     ,airline_code
     ,responder_code
     ,take_off_code
     ,origin_longitude02
     ,origin_latitude02
     ,land_code
     ,destination_longitude02
     ,destination_latitude02
     ,`image`
     ,take_off_time
     ,land_time
     ,during
     ,gmt_create
from tmp_radarbox_aircraft_04;


insert into dws_flight_number_info1
select
    id,
    s_mode,
    registration_number,
    flight_number,
    trace_id,
    model,
    time1,
    gmt_create
from tmp_radarbox_aircraft_04;

end;