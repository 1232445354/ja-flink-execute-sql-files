--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/07/30 10:19:45
-- description:  radarbox网站的飞机数据入融合表
-- version:ja-radarbox-aircraft-combine-rt-v250325
--********************************************************************--

set 'pipeline.name' = 'ja-radarbox-aircraft-combine-rt';


SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'parallelism.default' = '15';
SET 'execution.checkpointing.interval' = '1200000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-radarbox-aircraft-combine-rt';


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
                                                 ),
                                             proctime          as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'radarbox_aircraft_list_bak',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'radarbox-aircraft-list-bak-rt-idc',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1738796400000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
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
      'lookup.cache.max-rows' = '20000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );



-- 飞机实体表（Source：doris）
create table dws_et_aircraft_info (
                                      flight_id           string        comment '',
                                      icao_code           string        comment '飞机的 24 位 ICAO 标识符，为 6 个十六进制数字 大写',
                                      registration        string        comment '地区国家三位编码',
                                      flight_type         string        comment '飞机的机型型码，用于标识不同类型的飞机',
                                      is_military         int           comment '是否军用飞机',
    -- category_code       string,
    -- category_name       string,
    -- country_code        string,
    -- country_name        string,
    -- airlines_icao       string,
    -- airlines_e_name     string,
    -- airlines_c_name     string,
    -- friend_foe          string,
                                      primary key (flight_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_aircraft_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '700000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );


-- 航空器国籍登记代码表
drop table if exists dim_aircraft_country_prefix_code;
create table dim_aircraft_country_prefix_code (
                                                  prefix_code 	string  comment '代码前缀',
                                                  country_code 	string  comment '国家代码',
                                                  country_name  string  comment '国家名称',
                                                  primary key (prefix_code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_aircraft_country_prefix_code',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '20000',
      'lookup.cache.ttl' = '84000s',
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
      'lookup.cache.max-rows' = '20000',
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
      'lookup.cache.max-rows' = '20000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );



-- 机场名称
drop table if exists dws_et_airport_info;
create table dws_et_airport_info (
                                     id              string comment 'icao',
                                     iata 			string comment 'iata',
                                     e_name 		    string comment '机场英文名称',
                                     c_name 	        string comment '机场中文名称',
                                     primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_airport_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '20000',
      'lookup.cache.ttl' = '84000s',
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
      'lookup.cache.max-rows' = '20000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
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
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'radarbox_aircraft_source_idc1',
      'format' = 'json',
      'key.format' = 'json',
      'key.fields' = 'id'
      );


-- create function getCountry as 'GetCountryFromLngLat.getCountryFromLngLat' language python;
create function getCountry as 'com.jingan.udf.sea.GetCountryFromLngLat';
create function getSeaArea as 'com.jingan.udf.sea.GetSeaArea';



---------------

-- 数据处理

---------------


-- 筛选，关联飞机实体表，取部分注册号、机型、是否军用,字段转换
drop table if exists tmp_radarbox_aircraft_01;
create view tmp_radarbox_aircraft_01 as
select
    flightTraceId                                                                              as flight_trace_id,
    if(flightNo in ('BLOCKED','VARIOUS','TACTICAL',''),cast(null as varchar),flightNo)         as flight_no,
    from_unixtime(cast(acquireTimestamp as bigint)/1000,'yyyy-MM-dd HH:mm:ss')                 as acquire_timestamp_format, -- 时间戳格式化
    to_timestamp(from_unixtime(cast(acquireTimestamp as bigint)/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format_date, -- 时间类型的年月日时分秒
    acquireTimestamp                                                                           as acquire_timestamp,
    cast(latitude as double)                                                                   as latitude,
    cast(longitude as double)                                                                  as longitude,
    cast(altitude as double)                                                                   as altitude,
    if(t1.flightType = '' or t1.flightType is null,t2.flight_type,t1.flightType)               as flight_type,
    if(speed = '',cast(null as double),cast(speed as double))                                  as speed,
    if(heading = '',cast(null as double),cast(heading as double))                              as heading,
    dataSource                                                                                 as data_source,

    if(t1.registration is null or t1.registration in ('BLOCKED','VARIOUS','TACTICAL',''),t2.registration,t1.registration) as registration,

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
    if(num2 = '',cast(null as varchar),num2)                                                   as squawk_code,      -- 当前应答机代码
    expectedLandingTime                                                                        as expected_landing_time,  -- 到达时间
    if(flightPhoto='',cast(null as varchar),flightPhoto)                                       as flight_photo,
    flightDepartureTime                                                                        as flight_departure_time,  -- 出发时间
    unKonwn                                                                                    as un_konwn,
    toDestinationDistance                                                                      as to_destination_distance,
    estimatedLandingDuration                                                                   as estimated_landing_duration,
    if(sMode='',cast(null as varchar),sMode)                                                   as s_mode,
    estimatedLandingDurationFormat                                                             as estimated_landing_duration_format,
    split_index(expectedLandingTime,':',0)                                                     as expected_landing_time_hour,
    split_index(expectedLandingTime,':',1)                                                     as expected_landing_time_minute,
    split_index(flightDepartureTime,':',0)                                                     as flight_departure_time_hour,
    split_index(flightDepartureTime,':',1)                                                     as flight_departure_time_minute,
    t2.is_military,
    proctime
from (
         select * from radarbox_aircraft_list_kafka
         where acquireTimestamp is not null
           and flightTraceId is not null
           and flightTraceId <> ''
           and longitude is not null
           and longitude <> ''
           and latitude is not null
           and latitude <> ''
     ) as t1
         left join dws_et_aircraft_info
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on if(t1.sMode is null or t1.sMode = '',t1.flightTraceId,t1.sMode) = t2.flight_id;




-- 切分注册号，并且计算国家、海域id
drop view if exists tmp_radarbox_aircraft_02;
create view tmp_radarbox_aircraft_02 as
select
    *,
    if(longitude is not null, getCountry(longitude,latitude),cast(null as string))    as country_code3, -- 经纬度位置转换国家
    if(longitude is not null, getSeaArea(longitude,latitude),cast(null as string)) as sea_id,               -- 海域id
    if(instr(registration,'-')>0,substring(registration,1,2),concat(substring(registration,1,1),'-')) as prefix_code2,
    if(instr(registration,'-')>0,substring(registration,1,3),concat(substring(registration,1,2),'-')) as prefix_code3,
    if(instr(registration,'-')>0,substring(registration,1,4),concat(substring(registration,1,3),'-')) as prefix_code4,
    if(instr(registration,'-')>0,substring(registration,1,5),concat(substring(registration,1,4),'-')) as prefix_code5,
    CHAR_LENGTH(replace(registration,'-','')) as registration_len -- 所有长度
from tmp_radarbox_aircraft_01;



-- 对数据进行处理，加减时间得到起飞时间和到达时间，关联航空公司、所处国家3字转2字代码、注册号前缀,关联飞机类型表、关联海域表取出海域名称
drop table if exists tmp_radarbox_aircraft_03;
create view tmp_radarbox_aircraft_03 as
select
    t1.*,

    if(estimated_landing_duration_format.flag = 'm',
       cast(timestampadd(minute,cast(estimated_landing_duration_format.`minute` as int),acquire_timestamp_format_date)as string),
       cast(timestampadd(hour,cast(estimated_landing_duration_format.`hour` as int),timestampadd(minute,cast(estimated_landing_duration_format.`minute` as int),acquire_timestamp_format_date)) as string)
        ) as expected_landing_time_format,   -- 预计着陆时间

    case
        when flight_departure_time_hour < expected_landing_time_hour then concat(cast(CURRENT_DATE as string),' ',flight_departure_time,':00')
        when flight_departure_time_hour = expected_landing_time_hour and flight_departure_time_minute < expected_landing_time_minute then concat(cast(CURRENT_DATE as string),' ',flight_departure_time,':00')
        when flight_departure_time_hour > expected_landing_time_hour then concat(cast(timestampadd(day,-1,CURRENT_DATE) as string),' ',flight_departure_time,':00')
        end as flight_departure_time_format,

    t2.e_name             as airlines_e_name,  -- 航空公司英文名称
    t2.c_name             as airlines_c_name,  -- 航空公司中文名称
    t3.country_code2      as position_country_2code,  -- 当前所处的区域
    t3.c_name             as position_country_name,
    t9.category_code      as flight_category,         -- 飞机类型
    t9.category_c_name    as flight_category_name,     -- 飞机类型名称
    t10.c_name            as sea_name,    -- 关联查询海域名称

    -- 这是一个flink bug 这样取不到值
    -- coalesce(t7.country_code,t6.country_code,t5.country_code,t4.country_code) as country_code,
    t7.country_code as country_code7,
    t6.country_code as country_code6,
    t5.country_code as country_code5,
    t4.country_code as country_code4,
    t7.country_name as country_name7,
    t6.country_name as country_name6,
    t5.country_name as country_name5,
    t4.country_name as country_name4

from tmp_radarbox_aircraft_02 as t1
         left join dim_airline_list_info FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.airlines_icao = t2.icao

         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF t1.proctime as t3   -- 区域3字转2字
                   on t1.country_code3 = t3.country_code3

         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.prefix_code2=t4.prefix_code

         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t5
                   on t1.prefix_code3=t5.prefix_code

         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t6
                   on t1.prefix_code4=t6.prefix_code

         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t7
                   on t1.prefix_code5=t7.prefix_code

         left join dim_aircraft_type_category FOR SYSTEM_TIME AS OF t1.proctime as t9
                   on t1.flight_type = t9.id

         left join dim_sea_area FOR SYSTEM_TIME AS OF t1.proctime as t10
                   on t1.sea_id = t10.id;



-- 判断当前位于哪个国家上空、注册号取值
drop table if exists tmp_radarbox_aircraft_04;
create view tmp_radarbox_aircraft_04 as
select
    *,

    if(position_country_2code is null
           and ((longitude between 107.491636 and 124.806089 and latitude between 20.522241 and 40.799277)
            or
                (longitude between 107.491636 and 121.433286 and latitude between 3.011639 and 20.522241)
           )
        ,'CN', position_country_2code) as position_country_code2,

    -- 这样才能取到值
    if (coalesce(country_code7,country_code6,country_code5,country_code4) = 'CN' and registration_len = 6,
        'TW',
        coalesce(country_code7,country_code6,country_code5,country_code4)) as country_code,    -- 去除第一位B，剩余5位就是台湾

    coalesce(country_name7,country_name6,country_name5,country_name4) as country_name  -- 国家代码

from tmp_radarbox_aircraft_03;


-- 敌我识别、关联所属国家名称、所处国家名称
drop view if exists tmp_radarbox_aircraft_05;
create view tmp_radarbox_aircraft_05 as
select
    *,
    case
        when country_code in('IN','US','JP','AU','TW') and is_military = 1 then '1'  -- 美日..军机 敌方
        when country_code in ('CN','HK','MO') and is_military = 1 then '2'                -- 中国的军机 我方
        when country_code in ('CN','HK','MO') and is_military = 0 then '3'           -- 中国的非军机 友方
        else '4' end as friend_foe                                        -- 由国家判断敌我
from tmp_radarbox_aircraft_04;


-- 关联取机场名称
drop table if exists tmp_radarbox_aircraft_07;
create view tmp_radarbox_aircraft_07 as
select
    if(s_mode is null,flight_trace_id,s_mode)       as id,
    1                                               as srcCode,
    acquire_timestamp_format                        as acquireTime,
    s_mode                                          as icaoCode,
    registration,
    flight_no                                       as flightNo,
    cast(null as varchar)                           as callsign,
    flight_type                                     as flightType,
    is_military                                     as isMilitary,  -- 是否军用,
    if(s_mode is null,'trace_id','hex')             as pkType,
    flight_trace_id                                 as srcPk,
    flight_category                                 as flightCategory,         -- 飞机类型
    flight_category_name                            as flightCategoryName,        -- 飞机类型名称
    longitude                                       as lng,
    latitude                                        as lat,
    speed,
    speed * 1.852                                   as speedKm,
    altitude                                        as altitudeBaro ,
    altitude * 0.3048                               as altitudeBaroM,
    cast(null as double)                            as altitudeGeom,
    cast(null as double)                            as altitudeGeomM,
    heading                                         as heading,
    squawk_code                                     as squawkCode,
    if(flight_status = 'departed',0,1)              as flightStatus,
    if(flight_special_flag = true,1,0)              as special, -- 是否有特殊情况
    origin_airport3_code                            as originAirport3Code,
    coalesce(b.e_name,c.e_name)                    as originAirportEName , -- 来源机场英文,
    coalesce(b.c_name,c.c_name)                    as originAirportCName , -- 来源机场中文,
    source_longitude                               as originLng,
    source_latitude                                as originLat,
    destination_airport3_code                      as destAirport3Code,
    coalesce(d.e_name,f.e_name)                    as destAirportEName,
    coalesce(d.c_name,f.c_name)                    as destAirportCName,
    destination_longitude                          as destLng,
    destination_latitude                           as destLat,
    flight_photo                                   as flightPhoto,
    flight_departure_time_format                   as flightDepartureTime,
    expected_landing_time_format                   as expectedLandingTime,
    cast(to_destination_distance as double)        as toDestinationDistance,
    estimated_landing_duration                     as estimatedLandingDuration,
    airlines_icao                                  as airlinesIcao,
    airlines_e_name                                as airlinesEName,
    airlines_c_name                                as airlinesCName,
    country_code                                   as countryCode,
    country_name                                   as countryName,
    data_source                                    as dataSource,
    source,
    position_country_code2                         as positionCountryCode2,
    position_country_name                          as positionCountryName,
    friend_foe                                     as friendFoe,
    sea_id                                         as seaId,
    sea_name                                       as seaName,
    cast(null as varchar)                          as h3Code,
    concat(
            '{',
            '"num":"',if(num is not null,num,''),'",'
                '"un_konwn":"',if(un_konwn is not null,un_konwn,''),'",'
                '"station":"',if(station is not null,station,''),'",',
            '"flight_status":"',if(flight_status is not null,flight_status,''),'",',
            '"expected_landing_time":"',if(expected_landing_time is not null,expected_landing_time,''),'",'
                '"flight_departure_time":"',if(flight_departure_time is not null,flight_departure_time,''),'"}'
        ) as extendInfo

from tmp_radarbox_aircraft_05 a
         left join dws_et_airport_info FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.origin_airport3_code = b.id
         left join dws_et_airport_info FOR SYSTEM_TIME AS OF a.proctime as c
                   on a.origin_airport3_code = c.iata
         left join dws_et_airport_info FOR SYSTEM_TIME AS OF a.proctime as d
                   on a.destination_airport3_code = d.id
         left join dws_et_airport_info FOR SYSTEM_TIME AS OF a.proctime as f
                   on a.destination_airport3_code = f.iata;




-----------------------

-- 数据插入

-----------------------

begin statement set;


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
from tmp_radarbox_aircraft_07;

end;


