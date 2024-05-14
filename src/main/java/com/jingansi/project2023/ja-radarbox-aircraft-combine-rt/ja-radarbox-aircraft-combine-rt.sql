--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2023/11/8 10:19:45
-- description:  radarbox网站的飞机数据入融合表
-- version:
--********************************************************************--
set 'pipeline.name' = 'ja-radarbox-aircraft-combine-rt';


SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'parallelism.default' = '8';
SET 'execution.checkpointing.interval' = '1200000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-radarbox-aircraft-list-rt-6';


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
      'topic' = 'radarbox_aircraft_list',
      -- 'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
      'properties.group.id' = 'radarbox-aircraft-list-rt-6',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 飞机各个网站数据融合表
drop table if exists dws_aircraft_combine_list_rt;
create table dws_aircraft_combine_list_rt (
                                              flight_id											varchar(20) 	comment '飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码',
                                              acquire_time										string 		    comment '采集时间',
                                              src_code											int 			comment '来源网站标识 1. radarbox 2. adsbexchange',
                                              icao_code											varchar(10) 	comment '24位 icao编码',
                                              registration										varchar(20) 	comment '注册号',
                                              flight_no											varchar(20) 	comment '航班号',
                                              callsign											varchar(20) 	comment '呼号',
                                              flight_type										string 	        comment '飞机型号',
                                              is_military										int 			comment '是否军用飞机 0 非军用 1 军用',
                                              pk_type											varchar(10) 	comment 'flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex',
                                              src_pk											varchar(20) 	comment '源网站主键',
                                              flight_category									varchar(10) 	comment '飞机类型',
                                              flight_category_name						        varchar(50) 	comment '飞机类型名称',
                                              lng												double 			comment '经度',
                                              lat												double 			comment '纬度',
                                              speed												double 			comment '飞行当时的速度（单位：节）',
                                              speed_km											double 			comment '速度单位 km/h',
                                              altitude_baro										double 			comment '气压高度 海拔 航班当前高度，单位为（ft）',
                                              altitude_baro_m									double 			comment '气压高度 海拔 单位米',
                                              altitude_geom										double 			comment '海拔高度 海拔 航班当前高度，单位为（ft）',
                                              altitude_geom_m									double 			comment '海拔高度 海拔 单位米',
                                              heading											double 			comment '方向  正北为0 ',
                                              squawk_code										varchar(10) 	comment '当前应答机代码',
                                              flight_status										varchar(20) 	comment '飞机状态： 已启程',
                                              special											int 			comment '是否有特殊情况',
                                              origin_airport3_code						        varchar(10) 	comment '起飞机场的iata代码',
                                              origin_airport_e_name						        string 	        comment '来源机场英文',
                                              origin_airport_c_name						        string 	        comment '来源机场中文',
                                              origin_lng										double 			comment '来源机场经度',
                                              origin_lat										double 			comment '来源机场纬度',
                                              dest_airport3_code							    varchar(10) 	comment '目标机场的 iata 代码',
                                              dest_airport_e_name							    string 	        comment '目的机场英文',
                                              dest_airport_c_name							    string 	        comment '目的机场中文',
                                              dest_lng											double 			comment '目的地坐标经度',
                                              dest_lat											double 			comment '目的地坐标纬度',
                                              flight_photo										varchar(200) 	comment '飞机的图片',
                                              flight_departure_time						        string 		    comment '航班起飞时间',
                                              expected_landing_time						        string 		    comment '预计降落时间',
                                              to_destination_distance					        double 			comment '目的地距离',
                                              estimated_landing_duration			            string 			comment '预计还要多久着陆',
                                              airlines_icao										varchar(10) 	comment '航空公司的icao代码',
                                              airlines_e_name									string 	        comment '航空公司英文',
                                              airlines_c_name									varchar(100) 	comment '航空公司中文',
                                              country_code										varchar(10) 	comment '飞机所属国家代码',
                                              country_name										varchar(50) 	comment '国家中文',
                                              data_source										varchar(20) 	comment '数据来源的系统',
                                              source											varchar(20) 	comment '来源',
                                              position_country_code2					        varchar(2) 		comment '位置所在国家简称',
                                              position_country_name						        varchar(50) 	comment '位置所在国家名称',
                                              friend_foe										varchar(20) 	comment '敌我',
                                              sea_id											varchar(3) 		comment '海域id',
                                              sea_name											varchar(100) 	comment '海域名字',
                                              h3_code											varchar(20) 	comment '位置h3编码',
                                              extend_info									    string          comment '扩展信息 json 串',
                                              update_time										string 		    comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'table.identifier' = 'sa.dws_aircraft_combine_list_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );




-- 飞机各个网站数据融合状态表
drop table if exists dws_aircraft_combine_status_rt;
create table dws_aircraft_combine_status_rt (
                                                flight_id											varchar(20) 	comment '飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码',
                                                src_code											int 			comment '来源网站标识 1. radarbox 2. adsbexchange',
                                                acquire_time										string 		    comment '采集时间',
                                                icao_code											varchar(10) 	comment '24位 icao编码',
                                                registration										varchar(20) 	comment '注册号',
                                                flight_no											varchar(20) 	comment '航班号',
                                                callsign											varchar(20) 	comment '呼号',
                                                flight_type										string 	        comment '飞机型号',
                                                is_military										int 			comment '是否军用飞机 0 非军用 1 军用',
                                                pk_type											varchar(10) 	comment 'flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex',
                                                src_pk											varchar(20) 	comment '源网站主键',
                                                flight_category									varchar(10) 	comment '飞机类型',
                                                flight_category_name						        varchar(50) 	comment '飞机类型名称',
                                                lng												double 			comment '经度',
                                                lat												double 			comment '纬度',
                                                speed												double 			comment '飞行当时的速度（单位：节）',
                                                speed_km											double 			comment '速度单位 km/h',
                                                altitude_baro										double 			comment '气压高度 海拔 航班当前高度，单位为（ft）',
                                                altitude_baro_m									double 			comment '气压高度 海拔 单位米',
                                                altitude_geom										double 			comment '海拔高度 海拔 航班当前高度，单位为（ft）',
                                                altitude_geom_m									double 			comment '海拔高度 海拔 单位米',
                                                heading											double 			comment '方向  正北为0 ',
                                                squawk_code										varchar(10) 	comment '当前应答机代码',
                                                flight_status										varchar(20) 	comment '飞机状态： 已启程',
                                                special											int 			comment '是否有特殊情况',
                                                origin_airport3_code						        varchar(10) 	comment '起飞机场的iata代码',
                                                origin_airport_e_name						        string 	        comment '来源机场英文',
                                                origin_airport_c_name						        string 	        comment '来源机场中文',
                                                origin_lng										double 			comment '来源机场经度',
                                                origin_lat										double 			comment '来源机场纬度',
                                                dest_airport3_code							    varchar(10) 	comment '目标机场的 iata 代码',
                                                dest_airport_e_name							    string 	        comment '目的机场英文',
                                                dest_airport_c_name							    string 	        comment '目的机场中文',
                                                dest_lng											double 			comment '目的地坐标经度',
                                                dest_lat											double 			comment '目的地坐标纬度',
                                                flight_photo										varchar(200) 	comment '飞机的图片',
                                                flight_departure_time						        string 		    comment '航班起飞时间',
                                                expected_landing_time						        string 		    comment '预计降落时间',
                                                to_destination_distance					        double 			comment '目的地距离',
                                                estimated_landing_duration			            string 			comment '预计还要多久着陆',
                                                airlines_icao										varchar(10) 	comment '航空公司的icao代码',
                                                airlines_e_name									string 	        comment '航空公司英文',
                                                airlines_c_name									varchar(100) 	comment '航空公司中文',
                                                country_code										varchar(10) 	comment '飞机所属国家代码',
                                                country_name										varchar(50) 	comment '国家中文',
                                                data_source										varchar(20) 	comment '数据来源的系统',
                                                source											varchar(20) 	comment '来源',
                                                position_country_code2					        varchar(2) 		comment '位置所在国家简称',
                                                position_country_name						        varchar(50) 	comment '位置所在国家名称',
                                                friend_foe										varchar(20) 	comment '敌我',
                                                sea_id											varchar(3) 		comment '海域id',
                                                sea_name											varchar(100) 	comment '海域名字',
                                                h3_code											varchar(20) 	comment '位置h3编码',
                                                extend_info									    string          comment '扩展信息 json 串',
                                                update_time										string 		    comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'table.identifier' = 'sa.dws_aircraft_combine_status_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
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
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_airline_list_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );



-- 飞机实体表（Source：doris）
drop table if exists dws_aircraft_info;
create table dws_aircraft_info (
                                   icao_code           string        comment '飞机的 24 位 ICAO 标识符，为 6 个十六进制数字 大写',
                                   registration        string        comment '地区国家三位编码',
                                   icao_type           string        comment '飞机的机型型码，用于标识不同类型的飞机',
                                   is_mil              int           comment '是否军用飞机',
                                   primary key (icao_code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_aircraft_info',
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
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
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
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_aircraft_country_prefix_code',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );


-- 国家数据匹配库（Source：doris）
drop table if exists dim_country_code_name_info;
create table dim_country_code_name_info (
                                            id                        string        comment '国家英文-id',
                                            source                    string        comment '来源',
                                            e_name                    string        comment '国家的英文',
                                            c_name                    string        comment '国家的中文',
                                            country_code2             string        comment '国家的编码2',
                                            primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_code_name_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
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
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_sea_area',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );



-- 机场名称
drop table if exists dws_airport_detail_info;
create table dws_airport_detail_info (
                                         icao 			string COMMENT 'icao',
                                         iata 			string COMMENT 'iata',
                                         airport 		string COMMENT '机场英文名称',
                                         airport_name 	string COMMENT '机场中文名称',
                                         primary key (icao) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_airport_detail_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
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
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_aircraft_type_category',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );



-- create function getCountry as 'GetCountryFromLngLat.getCountryFromLngLat' language python;
create function getCountry as 'com.jingan.udf.sea.GetCountryFromLngLat';
create function getSeaArea as 'com.jingan.udf.sea.GetSeaArea';



---------------

-- 数据处理

---------------


-- 对数据字段进行处理筛选，关联飞机实体表，取部分注册号、机型、是否军用
drop table if exists tmp_radarbox_aircraft_01;
create view tmp_radarbox_aircraft_01 as
select
    flightTraceId                                                                              as flight_trace_id,
    if(flightNo = '',cast(null as varchar),flightNo)                                           as flight_no,
    from_unixtime(cast(acquireTimestamp as bigint)/1000,'yyyy-MM-dd HH:mm:ss')                 as acquire_timestamp_format, -- 时间戳格式化
    to_timestamp(from_unixtime(cast(acquireTimestamp as bigint)/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format_date, -- 时间类型的年月日时分秒
    acquireTimestamp                                                                           as acquire_timestamp,
    latitude                                                                                   as latitude,
    longitude                                                                                  as longitude,
    altitude                                                                                   as altitude,
    if(t1.flightType = '' or t1.flightType is null,t2.icao_type,t1.flightType)                 as flight_type,
    if(speed = '',cast(null as varchar),speed)                                                 as speed,
    if(heading = '',cast(null as varchar),heading)                                             as heading,
    dataSource                                                                                 as data_source,
    if(t2.registration is not null,t2.registration,if(t1.registration <> '',t1.registration,cast(null as varchar))) as registration,
    -- coalesce(t2.registration,t1.registration)                                                  as registration,
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
    if(longitude is not null, getCountry(cast(longitude as double),cast(latitude as double)),cast(null as string))                             as country_code3, -- 经纬度位置转换国家
    proctime,
    t2.is_mil      -- 是否军用
from radarbox_aircraft_list_kafka as t1
         left join dws_aircraft_info
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on if(t1.sMode = '', cast(null as varchar),t1.sMode) = t2.icao_code
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



-- 对数据进行处理，加减时间得到起飞时间和到达时间
drop table if exists tmp_radarbox_aircraft_03;
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

    t2.e_name as airlines_e_name,  -- 航空公司英文名称
    t2.c_name as airlines_c_name,  -- 航空公司中文名称
    t3.code2 as position_country_2code,  -- 当前所处的区域
    -- 这是一个flink bug 这样取不到值
    -- coalesce(t7.country_code,t6.country_code,t5.country_code,t4.country_code) as country_code,
    t7.country_code as country_code7,
    t6.country_code as country_code6,
    t5.country_code as country_code5,
    t4.country_code as country_code4
from tmp_radarbox_aircraft_02 as t1
         left join dim_airline_list_info
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.airlines_icao = t2.icao
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



-- 判断当前位于哪个国家上空
drop table if exists tmp_radarbox_aircraft_04;
create view tmp_radarbox_aircraft_04 as
select
    *,
    if(position_country_2code is null
           and ((cast(longitude as double) between 107.491636 and 124.806089 and cast(latitude as double) between 20.522241 and 40.799277)
            or
                (cast(longitude as double) between 107.491636 and 121.433286 and cast(latitude as double) between 3.011639 and 20.522241)
           )
        ,'CN', position_country_2code) as position_country_code2,
    -- 这样才能取到值  'BLOCKED','VARIOUS','TACTICAL' 三个异常注册号不转换国家
    if(registration is null,cast(null as varchar),coalesce(country_code7,country_code6,country_code5,country_code4)) as country_code
from tmp_radarbox_aircraft_03;



-- 敌我识别
drop table if exists tmp_radarbox_aircraft_05;
create view tmp_radarbox_aircraft_05 as
select
    t1.*,
    case
        when t1.country_code in('IN','US','JP','AU') and is_mil = 1 then 'ENENY'  -- 美日..军机 敌方
        when t1.country_code = 'CN' and is_mil = 1 then 'OUR_SIDE' -- 中国的军机 我方
        when t1.country_code = 'CN' and is_mil = 0 then 'FRIENDLY_SIDE' -- 中国的非军机 友方
        else 'NEUTRALITY' end as friend_foe,  -- 由国家判断敌我
    t2.c_name as country_name,  -- 所属国家中文名称
    t3.c_name as position_country_name
from tmp_radarbox_aircraft_04 as t1
         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.country_code = t2.country_code2 and 'COMMON' = t2.source
         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.position_country_code2 = t3.country_code2 and 'COMMON' = t3.source;



-- 计算海域
drop table if exists tmp_radarbox_aircraft_06;
create view tmp_radarbox_aircraft_06 as
select
    a.*,
    b.c_name as sea_name    -- 关联查询海域名称
from (
         select
             *,
             getSeaArea(cast(longitude as double),cast(latitude as double)) as sea_id   -- 计算海域id
         from tmp_radarbox_aircraft_05
     ) a left join dim_sea_area
    FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.sea_id = b.id;



-- 关联取机场名称，形成最后插入数据库的表
drop table if exists tmp_radarbox_aircraft_07;
create view tmp_radarbox_aircraft_07 as
select
    if(s_mode is null,flight_trace_id,s_mode)       as flight_id,
    acquire_timestamp_format                        as acquire_time,
    1                                               as src_code,
    s_mode                                          as icao_code,
    registration,
    flight_no,
    cast(null as varchar)                           as callsign,
    flight_type,
    is_mil                                          as is_military,  -- 是否军用,
    if(s_mode is null,'trace_id','hex')             as pk_type,
    flight_trace_id                                 as src_pk,
    g.category_code                                 as flight_category,         -- 飞机类型
    g.category_c_name                               as flight_category_name,        -- 飞机类型名称
    cast(longitude as double)                       as lng,
    cast(latitude as double)                        as lat,
    cast(speed as double)                           as speed,
    cast(speed as double) * 1.852                   as speed_km,
    cast(altitude as double)                        as altitude_baro ,
    cast(altitude as double) * 0.3048               as altitude_baro_m,
    cast(null as double)                            as altitude_geom,
    cast(null as double)                            as altitude_geom_m,
    cast(heading as double)                         as heading,
    squawk_code,
    flight_status,
    if(flight_special_flag = true,1,0)              as special, -- 是否有特殊情况
    origin_airport3_code,
    coalesce(b.airport,c.airport)                  as origin_airport_e_name , -- 来源机场英文,
    coalesce(b.airport_name,c.airport_name)        as origin_airport_c_name , -- 来源机场中文,
    source_longitude                               as origin_lng,
    source_latitude                                as origin_lat,
    destination_airport3_code                      as dest_airport3_code,
    coalesce(d.airport,f.airport)                  as dest_airport_e_name,
    coalesce(d.airport_name,f.airport_name)        as dest_airport_c_name,
    destination_longitude                          as dest_lng,
    destination_latitude                           as dest_lat,
    flight_photo,
    flight_departure_time_format                   as flight_departure_time,
    expected_landing_time_format                   as expected_landing_time,
    cast(to_destination_distance as double)        as to_destination_distance,
    estimated_landing_duration,
    airlines_icao,
    airlines_e_name,
    airlines_c_name,
    country_code,
    country_name,
    data_source,
    source,
    position_country_code2,
    position_country_name,
    friend_foe,
    sea_id,
    sea_name,
    cast(null as varchar)                         as h3_code,
    concat(
            '{',
            '"num":"',if(num is not null,num,''),'",'
                '"un_konwn":"',if(un_konwn is not null,un_konwn,''),'",'
                '"station":"',if(station is not null,station,''),'",',
            '"expected_landing_time":"',if(expected_landing_time is not null,expected_landing_time,''),'",'
                '"flight_departure_time":"',if(flight_departure_time is not null,flight_departure_time,''),'"}'
        )as extend_info,
    from_unixtime(unix_timestamp())  as update_time -- 数据入库时间
from tmp_radarbox_aircraft_06 a
         left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.origin_airport3_code = b.icao
         left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as c
                   on a.origin_airport3_code = c.iata
         left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as d
                   on a.destination_airport3_code = d.icao
         left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as f
                   on a.destination_airport3_code = f.iata
         left join dim_aircraft_type_category FOR SYSTEM_TIME AS OF a.proctime as g
                   on a.flight_type = g.id;



-----------------------

-- 数据插入

-----------------------

begin statement set;

insert into dws_aircraft_combine_list_rt
select
    flight_id,
    acquire_time,
    src_code,
    icao_code,
    registration,
    flight_no,
    callsign,
    flight_type,
    is_military,  -- 是否军用
    pk_type,
    src_pk,
    flight_category,
    flight_category_name,
    lng,
    lat,
    speed,
    speed_km,
    altitude_baro ,
    altitude_baro_m,
    altitude_geom,
    altitude_geom_m,
    heading,
    squawk_code,
    flight_status,
    special, -- 是否有特殊情况
    origin_airport3_code,
    origin_airport_e_name,
    origin_airport_c_name,
    origin_lng,
    origin_lat,
    dest_airport3_code,
    dest_airport_e_name,
    dest_airport_c_name,
    dest_lng,
    dest_lat,
    flight_photo,
    flight_departure_time,
    expected_landing_time,
    to_destination_distance,
    estimated_landing_duration,
    airlines_icao,
    airlines_e_name,
    airlines_c_name,
    country_code,
    country_name,
    data_source,
    source,
    position_country_code2,
    position_country_name,
    friend_foe,
    sea_id,
    sea_name,
    h3_code,
    extend_info,
    update_time
from tmp_radarbox_aircraft_07;


insert into dws_aircraft_combine_status_rt
select
    flight_id,
    src_code,
    acquire_time,
    icao_code,
    registration,
    flight_no,
    callsign,
    flight_type,
    is_military,  -- 是否军用
    pk_type,
    src_pk,
    flight_category,
    flight_category_name,
    lng,
    lat,
    speed,
    speed_km,
    altitude_baro ,
    altitude_baro_m,
    altitude_geom,
    altitude_geom_m,
    heading,
    squawk_code,
    flight_status,
    special, -- 是否有特殊情况
    origin_airport3_code,
    origin_airport_e_name,
    origin_airport_c_name,
    origin_lng,
    origin_lat,
    dest_airport3_code,
    dest_airport_e_name,
    dest_airport_c_name,
    dest_lng,
    dest_lat,
    flight_photo,
    flight_departure_time,
    expected_landing_time,
    to_destination_distance,
    estimated_landing_duration,
    airlines_icao,
    airlines_e_name,
    airlines_c_name,
    country_code,
    country_name,
    data_source,
    source,
    position_country_code2,
    position_country_name,
    friend_foe,
    sea_id,
    sea_name,
    h3_code,
    extend_info,
    update_time
from tmp_radarbox_aircraft_07;

end;

