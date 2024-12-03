--********************************************************************--
-- author:      write your name here
-- create time: 2023/11/14 17:08:09
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ja-flightrader24-aircraft-rt';


set 'parallelism.default' = '8';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '600000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-flightrader24-aircraft-rt';


-- 创建kafka来源的数据源表
create table if not exists flightradar24_aircraft_list(
                                                          id                   string,
                                                          ground_speed         double,
                                                          airline_iata         string,
                                                          number               string,
                                                          vertical_speed       double,
                                                          aircraft_code        string,
                                                          on_ground            int,
                                                          heading              double,
                                                          icao_24bit           string,
                                                          altitude             double,
                                                          longitude            double,
                                                          latitude             double,
                                                          squawk               string,
                                                          `time`               bigint,
                                                          airline_icao         string,
                                                          callsign             string,
                                                          registration         string,
                                                          origin_airport_iata  string,
                                                          destination_airport_iata string,
                                                          proctime             as PROCTIME()
    ) with (
          'connector' = 'kafka',
          'topic' = 'flightradar24_aircraft_list',
          'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
          'properties.group.id' = 'ja-flightrader24-aircraft-rt-idc',
          -- 'scan.startup.mode' = 'group-offsets',
          'scan.startup.mode' = 'latest-offset',
          -- 'scan.startup.mode' = 'timestamp',
          -- 'scan.startup.timestamp-millis' = '1724759244000',
          'format' = 'json',
          'json.fail-on-missing-field' = 'false',
          'json.ignore-parse-errors' = 'true'
          );



-- 创建写入doris的f24单独入库的表
create table if not exists dwd_fr24_aircraft_list_rt (
                                                         id                             string        comment 'fr24网站飞机编号',
                                                         acquire_time                   string        comment '采集时间',
                                                         `time`                         bigint        comment '时间，unixtime',
                                                         number                         string        comment '航班号',
                                                         icao_24bit                     string        comment '飞机的24位ICAO地址，用于在航空通信中唯一标识飞机',
                                                         registration                   string        comment '飞机的注册号，唯一标识特定飞机',
                                                         aircraft_code                  string        comment '飞机型号代码',
                                                         callsign                       string        comment '飞机的呼号，通常是航班号',
                                                         latitude                       double        comment '纬度',
                                                         longitude                      double        comment '经度',
                                                         vertical_speed                 double        comment '飞机的垂直速度，单位是英尺/分钟',
                                                         ground_speed                   double        comment '飞机的地面速度，单位是节',
                                                         heading                        double        comment '飞机的航向，表示飞机指向的方向',
                                                         altitude                       double        comment '飞机的飞行高度',
                                                         on_ground                      int           comment '表示飞机是否在地面上。0表示飞机正在飞行，1表示飞机在地面上。',
                                                         squawk                         string        comment 'Mode-3/A 应答机代码，通常为 4 位八进制数，',
                                                         origin_airport_iata            string        comment '起始机场的IATA代码',
                                                         destination_airport_iata       string        comment '目的地机场的IATA代码',
                                                         airline_iata                   string        comment '航空公司的IATA代码',
                                                         airline_icao                   string        comment '航空公司的ICAO代码',
                                                         update_time                    string        comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dwd_fr24_aircraft_list_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='100000',
      'sink.batch.interval'='10s'
      );



-- 飞机实体表（Source：doris）
drop table if exists dws_et_aircraft_info;
create table dws_et_aircraft_info (
                                      flight_id           string        comment '飞机id',
                                      icao_code           string        comment '飞机的 24 位 ICAO 标识符，为 6 个十六进制数字 大写',
                                      registration        string        comment '地区国家三位编码',
                                      flight_type         string        comment '飞机的机型型码，用于标识不同类型的飞机',
                                      is_military         int           comment '是否军用飞机',
                                      primary key (flight_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_aircraft_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );


-- 机场名称
drop table if exists dws_et_airport_info;
create table dws_et_airport_info (
                                     id               string comment 'icao',
                                     iata 			 string comment 'iata',
                                     e_name 		     string comment '机场英文名称',
                                     c_name 	         string comment '机场中文名称',
                                     latitude         double COMMENT '纬度',
                                     longitude        double COMMENT '经度',
                                     primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_airport_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 飞机类型
drop table if exists dim_aircraft_type_category;
create table dim_aircraft_type_category (
                                            id 			        string comment '飞机机型',
                                            category_code       string comment '飞机类型代码',
                                            category_c_name 	string comment '飞机类型名称',
                                            primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_aircraft_type_category',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 航空公司匹配库国家表（Source：doris）
drop table if exists dim_airline_list_info;
create table dim_airline_list_info (
                                       icao             string     comment '航空三字码',
                                       e_name           string     comment '航空公司英文名称',
                                       c_name           string     comment '航空公司中文名称',
                                       primary key (icao) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_airline_list_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );



-- 海域表
drop table if exists dim_sea_area;
create table dim_sea_area (
                              id 			varchar(5) COMMENT '海域编号',
                              name 		varchar(60) COMMENT '名称',
                              c_name 		varchar(60) COMMENT '中文名称',
                              primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_sea_area',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 国家数据匹配库（Source：doris）
drop table if exists dim_country_code_name_info;
create table dim_country_code_name_info (
                                            id                        string        comment '国家英文-id',
                                            source                    string        comment '来源',
                                            e_name                    string        comment '国家的英文',
                                            c_name                    string        comment '国家的中文',
                                            country_code2             string        comment '国家的编码2',
                                            country_code3             string        comment '国家的编码3',
                                            primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_code_name_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 飞机注册号前缀
drop table if exists dim_aircraft_country_prefix_code;
create table dim_aircraft_country_prefix_code (
                                                  prefix_code 	string  COMMENT '代码前缀',
                                                  country_code 	string  COMMENT '国家代码',
                                                  country_name  string  comment '国家名称',
                                                  primary key (prefix_code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_aircraft_country_prefix_code',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );




-- **************************** 规则引擎写入数据 aircraft_source ******************************** --

drop table if exists aircraft_source;
create table aircraft_source(
                                id                         string, -- id
                                srcCode                    bigint, --网站标识
                                acquireTime                string, -- 采集事件年月日时分秒
                                icaoCode                   string, -- icao
                                registration               string, -- 注册号
                                flightNo                   string, -- 航班号
                                callsign                   string, -- 呼号
                                flightType                 string, -- 飞机型号
                                isMilitary                 bigint, -- 是否军用
                                pkType                     string, -- flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex
                                srcPk                      string, -- 源网站主键
                                flightCategory             string, -- 飞机类别
                                flightCategoryName         string, -- 飞机类别名称
                                lng                        double, -- 经度
                                lat                        double, -- 纬度
                                speed                      double, -- 速度节
                                speedKm                    double, -- 速度 km
                                altitudeBaro               double, -- 气压高度 海拔 航班当前高度，单位为（ft）
                                altitudeBaroM              double, -- 气压高度 海拔 单位米
                                altitudeGeom               double, -- 海拔高度 海拔 航班当前高度，单位为（ft）
                                altitudeGeomM              double, -- 海拔高度 海拔 单位米
                                heading                    double, -- 方向
                                squawkCode                 string, -- 应答器代码
                                flightStatus               bigint, -- 飞机状态
                                special                    int,    -- 是否有特殊情况
                                originAirport3Code         string, -- 起飞机场的iata代码
                                originAirportEName         string, -- 来源机场英文
                                originAirportCName         string, -- 来源机场中文
                                originLng                  double, -- 来源机场经度
                                originLat                  double, -- 来源机场纬度
                                destAirport3Code           string, -- 目的机场3字代码
                                destAirportEName           string, -- 目的机场英文
                                destAirportCName           string, -- 目的机场中文
                                destLng                    double, -- 目的地坐标经度
                                destLat                    double, -- 目的地坐标纬度
                                flightPhoto                string, -- 飞机的图片
                                flightDepartureTime        string, -- 飞机起飞时间
                                expectedLandingTime        string, -- 预计降落时间
                                toDestinationDistance      double, -- 距离目的地距离
                                estimatedLandingDuration   string, -- 预计还要多久着陆
                                airlinesIcao               string, -- 航空公司icao
                                airlinesEName              string, -- 航空公司英文
                                airlinesCName              string, -- 航空公司中文
                                countryCode                string, -- 国家代码
                                countryName                string, -- 国家名称
                                dataSource                 string, -- 数据来源的系统
                                source                     string, -- 来源
                                positionCountryCode2       string, -- 所处国家
                                positionCountryName        string, -- 所处国家名称
                                friendFoe                  string, -- 敌我代码
                                seaId                      string, -- 海域id
                                seaName                    string, -- 海域名称
                                h3Code                     string, -- 位置h3编码
                                extendInfo                 string, -- 扩展信息 json 串
                                targetType                 string, -- 实体类型 固定值 AIRCRAFT
                                updateTime                 string  -- flink处理时间
) with (
      'connector' = 'kafka',
      'topic' = 'aircraft_source',
      'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
      'properties.group.id' = 'f24_aircraft_source_idc1',
      'format' = 'json',
      'key.format' = 'json',
      'key.fields' = 'id'
      );


create function getCountry as 'com.jingan.udf.sea.GetCountryFromLngLat';
create function getSeaArea as 'com.jingan.udf.sea.GetSeaArea';


-----------------------

-- 数据处理

-----------------------


-- 1.来源数据字段整理 2.关联实体表取字段 3. 海域国家计算
drop table if exists tmp_fr24_aircraft_01;
create view tmp_fr24_aircraft_01 as
select
    id,                                                                  -- fr24网站飞机编号
    from_unixtime(`time`)                                        as acquire_time,   -- 采集时间
    `time`, -- 时间，unixtime
    if(number='N/A',cast(null as string),number)                 as number,          -- 航班号
    if(icao_24bit='N/A',cast(null as string),icao_24bit)         as icao_24bit,      -- 飞机的24位ICAO地址，用于在航空通信中唯一标识飞机
    coalesce(if(a.registration='N/A',cast(null as string),a.registration),b.registration) as registration, -- 飞机的注册号，唯一标识特定飞机
    coalesce(if(aircraft_code='N/A',cast(null as string),aircraft_code),b.flight_type) as aircraft_code,     -- 飞机型号代码
    if(callsign='N/A',cast(null as string),callsign) as callsign,                    -- 飞机的呼号，通常是航班号
    latitude                       , -- 纬度
    longitude                      , -- 经度
    vertical_speed                 , -- 飞机的垂直速度，单位是英尺/分钟
    ground_speed                   , -- 飞机的地面速度，单位是节
    heading                        , -- 飞机的航向，表示飞机指向的方向
    altitude                       , -- 飞机的飞行高度
    on_ground                      , -- 表示飞机是否在地面上。0表示飞机正在飞行，1表示飞机在地面上。
    if(squawk='N/A',cast(null as string),squawk)                                     as squawk, -- Mode-3/A 应答机代码，通常为 4 位八进制数，
    if(origin_airport_iata='N/A',cast(null as string),origin_airport_iata)           as origin_airport_iata, -- 起始机场的IATA代码
    if(destination_airport_iata='N/A',cast(null as string),destination_airport_iata) as destination_airport_iata, -- 目的地机场的IATA代码
    if(airline_iata='N/A',cast(null as string),airline_iata)                         as airline_iata, -- 航空公司的IATA代码
    if(airline_icao='N/A',cast(null as string),airline_icao)                         as airline_icao, -- 航空公司的ICAO代码
    b.is_military,      -- 是否军用
    if(longitude is not null, getCountry(longitude,latitude),cast(null as string))   as country_code3, -- 经纬度位置转换国家
    if(longitude is not null, getSeaArea(longitude,latitude),cast(null as string))   as sea_id,        -- 计算海域id
    proctime
from flightradar24_aircraft_list a
         left join dws_et_aircraft_info FOR SYSTEM_TIME AS OF a.proctime as b
                   on if(a.icao_24bit is null,a.id,a.icao_24bit) = b.flight_id;



-- 1.关联机场 2.关联航空公司表取出所属航空公司 3.关联海域取出海域名称 4.切分注册号 5.所处国家3字代码转2字代码,所处国家名称
drop view if exists tmp_fr24_aircraft_02;
create view tmp_fr24_aircraft_02 as
select
    a.*,
    coalesce(b.e_name,c.e_name)                  as origin_airport_e_name , -- 来源机场英文,
    coalesce(b.c_name,c.c_name)        as origin_airport_c_name , -- 来源机场中文,
    coalesce(b.longitude,c.longitude)              as origin_lng,
    coalesce(b.latitude,c.latitude)                as origin_lat,
    coalesce(d.e_name,f.e_name)                  as dest_airport_e_name,
    coalesce(d.c_name,f.c_name)        as dest_airport_c_name,
    coalesce(d.longitude,f.longitude)              as dest_lng,
    coalesce(d.latitude,f.latitude)                as dest_lat,
    g.category_code                                as flight_category,         -- 飞机类型
    g.category_c_name                              as flight_category_name,    -- 飞机类型名称
    t2.e_name                                      as airlines_e_name,         -- 航空公司英文名称
    t2.c_name                                      as airlines_c_name,         -- 航空公司中文名称
    t3.country_code2                               as position_country_2code,  -- 当前所处的区域
    t3.c_name                                      as position_country_name,
    t4.c_name                                      as sea_name,                -- 关联查询海域名称

    if(instr(registration,'-')>0,substring(registration,1,2),concat(substring(registration,1,1),'-')) as prefix_code2,
    if(instr(registration,'-')>0,substring(registration,1,3),concat(substring(registration,1,2),'-')) as prefix_code3,
    if(instr(registration,'-')>0,substring(registration,1,4),concat(substring(registration,1,3),'-')) as prefix_code4,
    if(instr(registration,'-')>0,substring(registration,1,5),concat(substring(registration,1,4),'-')) as prefix_code5,
    CHAR_LENGTH(replace(registration,'-','')) as registration_len -- 所有长度

from tmp_fr24_aircraft_01 a
         left join dws_et_airport_info FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.origin_airport_iata = b.id

         left join dws_et_airport_info FOR SYSTEM_TIME AS OF a.proctime as c
                   on a.origin_airport_iata = c.iata

         left join dws_et_airport_info FOR SYSTEM_TIME AS OF a.proctime as d
                   on a.destination_airport_iata = d.id

         left join dws_et_airport_info FOR SYSTEM_TIME AS OF a.proctime as f
                   on a.destination_airport_iata = f.iata

         left join dim_aircraft_type_category FOR SYSTEM_TIME AS OF a.proctime as g
                   on a.aircraft_code = g.id

         left join dim_airline_list_info FOR SYSTEM_TIME AS OF a.proctime as t2
                   on a.airline_icao = t2.icao

         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF a.proctime as t3
                   on a.country_code3 = t3.country_code3
                       and 'COMMON' = t3.source

         left join dim_sea_area FOR SYSTEM_TIME AS OF a.proctime as t4
                   on a.sea_id = t4.id;



-- 1.判断区域 2.关联注册号前缀取出所属国家code
drop view if exists tmp_fr24_aircraft_03;
create view tmp_fr24_aircraft_03 as
select
    t1.*,
    if(position_country_2code is null
           and ((longitude between 107.491636 and 124.806089 and latitude between 20.522241 and 40.799277)
            or
                (longitude between 107.491636 and 121.433286 and latitude between 3.011639 and 20.522241)
           )
        ,'CN', position_country_2code) as position_country_code2,

    t7.country_code as country_code7,
    t6.country_code as country_code6,
    t5.country_code as country_code5,
    t4.country_code as country_code4,
    t7.country_name as country_name7,
    t6.country_name as country_name6,
    t5.country_name as country_name5,
    t4.country_name as country_name4

from tmp_fr24_aircraft_02 t1
         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.prefix_code2=t4.prefix_code

         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t5
                   on t1.prefix_code3=t5.prefix_code

         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t6
                   on t1.prefix_code4=t6.prefix_code

         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t7
                   on t1.prefix_code5=t7.prefix_code;



-- 1.判断敌我 2.关联所属国家名称、取出国家名称
drop table if exists tmp_fr24_aircraft_04;
create view tmp_fr24_aircraft_04 as
select
    t1.*,
    case
        when t1.country_code in('IN','US','JP','AU','TW') and is_military = 1 then 'ENENY'  -- 美日..军机 敌方
        when t1.country_code in ('CN','HK','MO') and is_military = 1 then 'OUR_SIDE' -- 中国的军机 我方
        when t1.country_code in ('CN','HK','MO') and is_military = 0 then 'FRIENDLY_SIDE' -- 中国的非军机 友方
        else 'NEUTRALITY' end as friend_foe,  -- 由国家判断敌我

    t2.c_name as country_name  -- 所属国家中文名称

from (
         select
             *,
             -- 这样才能取到值
             if (coalesce(country_code7,country_code6,country_code5,country_code4) = 'CN' and registration_len = 6,
                 'TW',
                 coalesce(country_code7,country_code6,country_code5,country_code4)) as country_code,    -- 去除第一位B，剩余5位就是台湾

             coalesce(country_name7,country_name6,country_name5,country_name4) as country_name  -- 国家代码

         from tmp_fr24_aircraft_03
     ) t1
         left join dim_country_code_name_info
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.country_code = t2.country_code2 and 'COMMON' = t2.source;



-- 1.整合字段
drop table if exists tmp_fr24_aircraft_05;
create view tmp_fr24_aircraft_05 as
select
    if(icao_24bit is null,id,icao_24bit)                          as id,
    acquire_time                                                  as acquireTime,
    3                                                             as srcCode,
    if(char_length(icao_24bit)=6,icao_24bit,cast(null as string)) as icaoCode,
    registration,
    number                              as flightNo,
    callsign,
    aircraft_code                       as flightType,
    is_military                         as isMilitary,
    if(icao_24bit is null,'id','hex')   as pkType,
    id                                  as srcPk,
    flight_category                     as flightCategory,
    flight_category_name                as flightCategoryName,
    longitude                           as lng,
    latitude                            as lat,
    ground_speed                        as speed,
    ground_speed * 1.852                as speedKm,
    altitude                            as altitudeBaro,
    altitude * 0.3048                   as altitudeBaroM,
    cast(null as double)                as altitudeGeom,
    cast(null as double)                as altitudeGeomM,
    heading,
    squawk                              as squawkCode,
    cast(on_ground as bigint)           as flightStatus,
    cast(null as int)                   as special, -- 是否有特殊情况
    origin_airport_iata                 as originAirport3Code,
    origin_airport_e_name               as originAirportEName,
    origin_airport_c_name               as originAirportCName,
    origin_lng                          as originLng,
    origin_lat                          as originLat,
    destination_airport_iata            as destAirport3Code,
    dest_airport_e_name                 as destAirportEName,
    dest_airport_c_name                 as destAirportCName,
    dest_lng                            as destLng,
    dest_lat                            as destLat,
    cast(null as string)                as flightPhoto,
    cast(null as string)                as flightDepartureTime,
    cast(null as string)                as expectedLandingTime,
    cast(null as double)                as toDestinationDistance,
    cast(null as string)                as estimatedLandingDuration,
    airline_icao                        as airlinesIcao,
    airlines_e_name                     as airlinesEName,
    airlines_c_name                     as airlinesCName,
    country_code                        as countryCode,
    country_name                        as countryName,
    cast(null as string)                as dataSource,
    cast(null as string)                as source,
    position_country_code2              as positionCountryCode2,
    position_country_name               as positionCountryName,
    friend_foe                          as friendFoe,
    sea_id                              as seaId,
    sea_name                            as seaName,
    cast(null as varchar)               as h3Code,
    concat('{',
           if(vertical_speed is null,'',concat('"vertical_speed":',cast(vertical_speed as string),',')),
           if(on_ground is null,'',concat('"on_ground":',cast(on_ground as string),',')),
           if(airline_iata is null,'',concat('"airline_iata":"',cast(on_ground as string),'",')),
           if(`time` is null,'',concat('"time":',cast(`time` as string))),
           '}')                                 as extendInfo
from tmp_fr24_aircraft_04 ;



begin statement set;

insert into dwd_fr24_aircraft_list_rt
select
    id                             , -- fr24网站飞机编号
    acquire_time                   , -- 采集时间
    `time`                           , -- 时间，unixtime
    number                         , -- 航班号
    icao_24bit                     , -- 飞机的24位ICAO地址，用于在航空通信中唯一标识飞机
    registration                   , -- 飞机的注册号，唯一标识特定飞机
    aircraft_code                  , -- 飞机型号代码
    callsign                       , -- 飞机的呼号，通常是航班号
    latitude                       , -- 纬度
    longitude                      , -- 经度
    vertical_speed                 , -- 飞机的垂直速度，单位是英尺/分钟
    ground_speed                   , -- 飞机的地面速度，单位是节
    heading                        , -- 飞机的航向，表示飞机指向的方向
    altitude                       , -- 飞机的飞行高度
    on_ground                      , -- 表示飞机是否在地面上。0表示飞机正在飞行，1表示飞机在地面上。
    squawk                         , -- Mode-3/A 应答机代码，通常为 4 位八进制数，
    origin_airport_iata            , -- 起始机场的IATA代码
    destination_airport_iata       , -- 目的地机场的IATA代码
    airline_iata                   , -- 航空公司的IATA代码
    airline_icao                   , -- 航空公司的ICAO代码
    from_unixtime(unix_timestamp()) as update_time                      -- 更新时间'
from tmp_fr24_aircraft_01;



insert into aircraft_source
select
    id,
    srcCode,
    acquireTime,
    icaoCode,
    registration,
    flightNo,
    callsign,
    flightType,
    isMilitary,
    pkType,
    srcPk,
    flightCategory,
    flightCategoryName,
    lng,
    lat,
    speed,
    speedKm,
    altitudeBaro,
    altitudeBaroM,
    altitudeGeom,
    altitudeGeomM,
    heading,
    squawkCode,
    flightStatus,
    special,
    originAirport3Code,
    originAirportEName,
    originAirportCName,
    originLng,
    originLat,
    destAirport3Code,
    destAirportEName,
    destAirportCName,
    destLng,
    destLat,
    flightPhoto,
    flightDepartureTime,
    expectedLandingTime,
    toDestinationDistance,
    estimatedLandingDuration,
    airlinesIcao,
    airlinesEName,
    airlinesCName,
    countryCode,
    countryName,
    dataSource,
    source,
    positionCountryCode2,
    positionCountryName,
    friendFoe,
    seaId,
    seaName,
    h3Code,
    extendInfo,
    'AIRCRAFT'                      as targetType,
    from_unixtime(unix_timestamp()) as updateTime -- 数据入库时间
from tmp_fr24_aircraft_05;


end;
