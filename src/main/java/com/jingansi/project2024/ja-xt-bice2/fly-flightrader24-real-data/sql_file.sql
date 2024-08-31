set 'pipeline.name' = 'fly-flightrader24-real-data';


SET 'table.exec.state.ttl' = '600000';
SET 'parallelism.default' = '4';


-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '300000';
set 'state.checkpoints.dir' = 's3://ja-bice1/flink-checkpoints/fly-flightrader24-real-data';


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
      'properties.group.id' = 'ja-flightrader24-ceshi1',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1719384687000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );




-- doris全量表
drop table if exists dwd_fly_full_data;
create table dwd_fly_full_data (
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
      'fenodes' = '172.19.80.4:8030',
      'table.identifier' = 'situation.dwd_fly_full_data',
      'username' = 'admin',
      'password' = 'dawu@110',
      'sink.enable.batch-mode'='true',
      'sink.buffer-flush.max-rows'='10000',
      'sink.buffer-flush.interval'='20s',
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
      'fenodes' = '172.19.80.4:8030',
      'table.identifier' = 'situation.dws_fly_real_data',
      'username' = 'admin',
      'password' = 'dawu@110',
      'sink.enable.batch-mode'='true',
      'sink.buffer-flush.max-rows'='10000',
      'sink.buffer-flush.interval'='20s',
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
      'fenodes' = '172.19.80.4:8030',
      'table.identifier' = 'situation.dws_flight_info1',
      'username' = 'admin',
      'password' = 'dawu@110',
      'sink.enable.batch-mode'='true',
      'sink.buffer-flush.max-rows'='10000',
      'sink.buffer-flush.interval'='20s',
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
      'url' = 'jdbc:mysql://172.19.80.4:9030/situation?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'dawu@110',
      'table-name' = 'dim_aircraft_prefix_code',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );



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
    acquire_time                                                    as cjsj,
    3                                                               as bs,
    if(char_length(icao_24bit)=6,icao_24bit,cast(null as string))   as sms,
    registration                                                    as zch,
    number                                                          as hbh,
    aircraft_code                                                   as xh,
    cast(null as varchar)                                           as lx,
    longitude                                                       as jd,
    latitude                                                        as wd,
    ground_speed * 1.852                                            as sd,
    altitude * 0.3048                                               as gd,
    cast(heading as double)                                         as fx,
    cast(null as varchar)                                           as zt,
    if(registration is null,cast(null as varchar),coalesce(country_code7,country_code6,country_code5,country_code4)) as gjdm,
    cast(null as varchar)                                           as gjmc,
    cast(null as varchar)                                           as ly,
    airline_icao                                                    as hkgsdm,
    squawk                                                          as ydjdm,
    origin_airport_iata                                             as lyjcdm,
    cast(null as double)                                           as lyjcjd,
    cast(null as double)                                           as lyjcwd,
    destination_airport_iata                                        as mdjcdm,
    cast(null as double)                                           as mdjcjd,
    cast(null as double)                                           as mdjcwd,
    cast(null as varchar)                                           as fjtp,
    cast(null as varchar)                                           as qfsj,
    cast(null as varchar)                                           as jlsj,
    cast(null as varchar)                                           as yjdjzl,
    from_unixtime(unix_timestamp())                                 as rksj    -- 数据入库时间
from tmp_fr24_aircraft_03;


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
from tmp_fr24_aircraft_04;



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
from tmp_fr24_aircraft_04;


insert into dws_flight_info1
select
    id,
    sms,
    zch,
    hbh,
    cast(null as varchar) as hb_id,
    xh,
    cjsj,
    rksj
from tmp_fr24_aircraft_04;

end;


