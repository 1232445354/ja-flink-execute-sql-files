set 'pipeline.name' = 'fly-flightrader24-real-data';


SET 'table.exec.state.ttl' = '300000';
SET 'parallelism.default' = '4';


-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '300000';
set 'state.checkpoints.dir' = 's3://ja-bice2/flink-checkpoints/fly-flightrader24-real-data';


-- f24数据
drop table if exists flightradar24_aircraft_list;
create table flightradar24_aircraft_list(
                                            id                        string,
                                            ground_speed              double,
                                            airline_iata              string,
                                            number                    string,
                                            vertical_speed            double,
                                            aircraft_code             string,
                                            on_ground                 int,
                                            heading                   double,
                                            icao_24bit                string,
                                            altitude                  double,
                                            longitude                 double,
                                            squawk                    string,
                                            `time`                    bigint,
                                            airline_icao              string,
                                            callsign                  string,
                                            registration              string,
                                            origin_airport_iata       string,
                                            destination_airport_iata  string,
                                            latitude                  double,
                                            proctime                  as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'flightradar24_aircraft_list',
      'properties.bootstrap.servers' = '115.231.236.106:30090',
      'properties.group.id' = 'ja-flightrader24-ceshi2',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1703606400000',
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
      'sink.buffer-flush.max-rows'='20000',
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
      'sink.buffer-flush.max-rows'='20000',
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
      'sink.buffer-flush.max-rows'='20000',
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
      'url' = 'jdbc:mysql://8.130.39.51:9030/global_entity?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'yshj@yshj',
      'table-name' = 'dim_prefix',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );


-- ---------------------
-- 数据处理
-- ---------------------


-- 数据晒选处理
drop table if exists tmp_fr24_aircraft_01;
create view tmp_fr24_aircraft_01 as
select
    id                             , -- fr24网站飞机编号
    from_unixtime(`time`) as acquire_time                   , -- 采集时间
    `time`                           , -- 时间，unixtime
    if(number='N/A',cast(null as string),number) as number                         , -- 航班号
    if(icao_24bit='N/A',cast(null as string),icao_24bit) as icao_24bit                     , -- 飞机的24位ICAO地址，用于在航空通信中唯一标识飞机
    if(registration='N/A',cast(null as string),registration) as  registration                   , -- 飞机的注册号，唯一标识特定飞机
    if(aircraft_code='N/A',cast(null as string),aircraft_code) as aircraft_code                  , -- 飞机型号代码
    latitude                       , -- 纬度
    longitude                      , -- 经度
    vertical_speed                 , -- 飞机的垂直速度，单位是英尺/分钟
    ground_speed                   , -- 飞机的地面速度，单位是节
    heading                        , -- 飞机的航向，表示飞机指向的方向
    altitude                       , -- 飞机的飞行高度
    if(squawk='N/A',cast(null as string),squawk) as squawk                         , -- Mode-3/A 应答机代码，通常为 4 位八进制数，
    if(origin_airport_iata='N/A',cast(null as string),origin_airport_iata) as origin_airport_iata            , -- 起始机场的IATA代码
    if(destination_airport_iata='N/A',cast(null as string),destination_airport_iata) as destination_airport_iata       , -- 目的地机场的IATA代码
    if(airline_iata='N/A',cast(null as string),airline_iata) as airline_iata                   , -- 航空公司的IATA代码
    if(airline_icao='N/A',cast(null as string),airline_icao) as airline_icao                   , -- 航空公司的ICAO代码
    proctime
from flightradar24_aircraft_list;



-- 切分注册号
drop view if exists tmp_fr24_aircraft_02;
create view tmp_fr24_aircraft_02 as
select
    *,
    if(instr(registration,'-')>0,substring(registration,1,2),concat(substring(registration,1,1),'-')) as prefix_code2,
    if(instr(registration,'-')>0,substring(registration,1,3),concat(substring(registration,1,2),'-')) as prefix_code3,
    if(instr(registration,'-')>0,substring(registration,1,4),concat(substring(registration,1,3),'-')) as prefix_code4,
    if(instr(registration,'-')>0,substring(registration,1,5),concat(substring(registration,1,4),'-')) as prefix_code5
from tmp_fr24_aircraft_01;



-- 对数据进行处理，计算国家代码
drop view if exists tmp_fr24_aircraft_03;
create view tmp_fr24_aircraft_03 as
select
    t1.*,
    t7.country_code as country_code7,
    t6.country_code as country_code6,
    t5.country_code as country_code5,
    t4.country_code as country_code4
from tmp_fr24_aircraft_02 as t1

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


-- 飞机国家计算
drop table if exists tmp_fr24_aircraft_04;
create view tmp_fr24_aircraft_04 as
select
    if(icao_24bit is null,id,icao_24bit)                            as id,
    acquire_time                                                    as time1,
    3                                                               as flag,
    if(char_length(icao_24bit)=6,icao_24bit,cast(null as string))   as s_mode,
    registration                                                    as registration_number,
    number                                                          as flight_number,
    aircraft_code                                                   as model,
    cast(null as varchar)                                           as `type`,
    longitude                                                       as longitude02,
    latitude                                                        as latitude02,
    ground_speed * 1.852                                            as speed_km,
    altitude * 0.3048                                               as height,
    cast(heading as double)                                         as heading,
    cast(null as varchar)                                           as fly_status,
    if(registration is null,cast(null as varchar),coalesce(country_code7,country_code6,country_code5,country_code4)) as country_flag,
    cast(null as varchar)                                           as country_chinese_name,
    cast(null as varchar)                                           as origin,
    airline_icao                                                    as airline_code,
    squawk                                                          as responder_code,
    origin_airport_iata                                             as take_off_code,
    cast(null as double)                                           as origin_longitude02,
    cast(null as double)                                           as origin_latitude02,
    destination_airport_iata                                        as land_code,
    cast(null as double)                                           as destination_longitude02,
    cast(null as double)                                           as destination_latitude02,
    cast(null as varchar)                                           as `image`,
    cast(null as varchar)                                           as take_off_time,
    cast(null as varchar)                                           as land_time,
    cast(null as varchar)                                           as during,
    from_unixtime(unix_timestamp())                                 as gmt_create    -- 数据入库时间
from tmp_fr24_aircraft_03;


-- ---------------------
-- 数据入库
-- ---------------------

begin statement set;

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
from tmp_fr24_aircraft_04;



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
from tmp_fr24_aircraft_04;


insert into dws_flight_number_info1
select
    id,
    s_mode,
    registration_number,
    flight_number,
    cast(null as varchar) as trace_id,
    model,
    time1,
    gmt_create
from tmp_fr24_aircraft_04;

end;


