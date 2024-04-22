set 'pipeline.name' = 'fly-radarbox-real-data';

set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'sql-client.execution.result-mode' = 'TABLEAU';


SET 'table.exec.state.ttl' = '300000';
SET 'parallelism.default' = '6';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '1200000';
set 'state.checkpoints.dir' = 's3://ja-bice1/flink-checkpoints/fly-radarbox-real-data';


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
drop table if exists dwd_fly_full_data;
create table dwd_fly_full_data (
                                   id      string  , -- 飞机id
                                   bs      int  , --  标识
                                   cjsj    string  , --  采集时间
                                   sms     string  , --  s模式
                                   zch     string  , --  注册号
                                   hbh     string  , --  航班号
                                   xh      string  , --  型号
                                   lx      string  , --  类型
                                   jd      double  , --  经度
                                   wd      double  , --  纬度
                                   sd      double  , --  速度
                                   gd      double  , --  高度
                                   fx      double  , --  方向
                                   zt      string  , --   状态
                                   gjdm    string  , --  国家代码
                                   gjmc    string  , --  国家名称
                                   ly      string  , --  来源
                                   hkgsdm  string  , --  航空公司的icao代码
                                   ydjdm   string  , --  应答机代码
                                   lyjcdm  string  , --  起飞机场的iata代码
                                   lyjcjd  double  , --  来源机场经度
                                   lyjcwd  double  , --  来源机场纬度
                                   mdjcdm  string  , --  目标机场的 iata 代码
                                   mdjcjd  double  , --  目的地坐标经度
                                   mdjcwd  double  , --  目的地坐标纬度
                                   fjtp    string  , --  飞机的图片
                                   qfsj    string  , --  航班起飞时间
                                   jlsj    string  , --  预计降落时间
                                   yjdjzl  string  , --  预计多久着陆
                                   rksj    string   --  入库时间
) with (
      'connector' = 'doris',
      'fenodes' = '47.92.158.88:8031',
      'table.identifier' = 'situation.dwd_fly_full_data',
      'username' = 'admin',
      'password' = 'dawu@110',
      'sink.enable.batch-mode'='true',
      'sink.buffer-flush.max-rows'='10000',
      'sink.buffer-flush.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- doris状态表
drop table if exists dws_fly_real_data;
create table dws_fly_real_data (
                                   id      string  , -- 飞机id
                                   bs      int     , --  标识
                                   cjsj    string  , --  采集时间
                                   sms     string  , --  s模式
                                   zch     string  , --  注册号
                                   hbh     string  , --  航班号
                                   xh      string  , --  型号
                                   lx      string  , --  类型
                                   jd      double  , --  经度
                                   wd      double  , --  纬度
                                   sd      double  , --  速度
                                   gd      double  , --  高度
                                   fx      double  , --  方向
                                   zt      string , --   状态
                                   gjdm    string  , --  国家代码
                                   gjmc    string  , --  国家名称
                                   ly      string  , --  来源
                                   hkgsdm  string  , --  航空公司的icao代码
                                   ydjdm   string  , --  应答机代码
                                   lyjcdm  string  , --  起飞机场的iata代码
                                   lyjcjd  double  , --  来源机场经度
                                   lyjcwd  double  , --  来源机场纬度
                                   mdjcdm  string  , --  目标机场的 iata 代码
                                   mdjcjd  double  , --  目的地坐标经度
                                   mdjcwd  double  , --  目的地坐标纬度
                                   fjtp    string  , --  飞机的图片
                                   qfsj    string  , --  航班起飞时间
                                   jlsj    string  , --  预计降落时间
                                   yjdjzl  string  , --  预计多久着陆
                                   rksj    string   --  入库时间
) with (
      'connector' = 'doris',
      'fenodes' = '47.92.158.88:8031',
      'table.identifier' = 'situation.dws_fly_real_data',
      'username' = 'admin',
      'password' = 'dawu@110',
      'sink.enable.batch-mode'='true',
      'sink.buffer-flush.max-rows'='10000',
      'sink.buffer-flush.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- 飞机航班表1
drop table if exists dws_flight_info1;
create table dws_flight_info1 (
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
      'fenodes' = '47.92.158.88:8031',
      'table.identifier' = 'situation.dws_flight_info1',
      'username' = 'admin',
      'password' = 'dawu@110',
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
      'url' = 'jdbc:mysql://47.92.158.88:9031/situation?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'dawu@110',
      'table-name' = 'dim_aircraft_prefix_code',
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
    acquire_timestamp_format                        as cjsj,
    1                                               as bs,
    s_mode                                          as sms,
    registration                                    as zch,
    flight_no                                       as hbh,
    flight_trace_id                                 as hb_id,
    flight_type                                     as xh,
    cast(null as varchar)                           as lx,
    cast(longitude as double)                       as jd,
    cast(latitude as double)                        as wd,
    cast(speed as double) * 1.852                   as sd,
    cast(altitude as double) * 0.3048               as gd,
    cast(heading as double)                         as fx,
    flight_status                                   as zt,
    if(registration is null,cast(null as varchar),coalesce(country_code7,country_code6,country_code5,country_code4))  as gjdm,
    cast(null as varchar)                           as gjmc,
    `data_source`                                   as ly,
    airlines_icao                                   as hkgsdm,
    squawk_code                                     as ydjdm,
    origin_airport3_code                            as lyjcdm,
    source_longitude                                as lyjcjd,
    source_latitude                                 as lyjcwd,
    destination_airport3_code                       as mdjcdm,
    destination_longitude                           as mdjcjd,
    destination_latitude                            as mdjcwd,
    flight_photo                                    as fjtp,
    flight_departure_time_format                    as qfsj,
    expected_landing_time_format                    as jlsj,
    estimated_landing_duration                      as yjdjzl,
    from_unixtime(unix_timestamp())                 as rksj    -- 数据入库时间
from tmp_radarbox_aircraft_03;


-- ---------------------
-- 数据入库
-- ---------------------

begin statement set;

insert into dwd_fly_full_data
select
    id
     ,bs
     ,cjsj
     ,sms
     ,zch
     ,hbh
     ,xh
     ,lx
     ,jd
     ,wd
     ,sd
     ,gd
     ,fx
     ,zt
     ,gjdm
     ,gjmc
     ,ly
     ,hkgsdm
     ,ydjdm
     ,lyjcdm
     ,lyjcjd
     ,lyjcwd
     ,mdjcdm
     ,mdjcjd
     ,mdjcwd
     ,fjtp
     ,qfsj
     ,jlsj
     ,yjdjzl
     ,rksj
from tmp_radarbox_aircraft_04;



insert into dws_fly_real_data
select
    id
     ,bs
     ,cjsj
     ,sms
     ,zch
     ,hbh
     ,xh
     ,lx
     ,jd
     ,wd
     ,sd
     ,gd
     ,fx
     ,zt
     ,gjdm
     ,gjmc
     ,ly
     ,hkgsdm
     ,ydjdm
     ,lyjcdm
     ,lyjcjd
     ,lyjcwd
     ,mdjcdm
     ,mdjcjd
     ,mdjcwd
     ,fjtp
     ,qfsj
     ,jlsj
     ,yjdjzl
     ,rksj
from tmp_radarbox_aircraft_04;

insert into dws_flight_info1
select
    id,
    sms,
    zch,
    hbh,
    hb_id,
    xh,
    cjsj,
    rksj
from tmp_radarbox_aircraft_04;

end;