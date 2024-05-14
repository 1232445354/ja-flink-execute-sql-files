--********************************************************************--
-- author:      write your name here
-- create time: 2023/11/14 17:08:09
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ja-flightrader24-aircraft-rt';


set 'parallelism.default' = '4';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '600000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-flightrader24-aircraft-rt';


create table flightradar24_aircraft_list(
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
                                            squawk               string,
                                            `time`               bigint,
                                            airline_icao         string,
                                            callsign             string,
                                            registration         string,
                                            origin_airport_iata  string,
                                            destination_airport_iata string,
                                            latitude             double,
                                            proctime          as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'flightradar24_aircraft_list',
      -- 'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
      'properties.group.id' = 'ja-flightrader24-aircraft-rt',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );





create table dwd_fr24_aircraft_list_rt (
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
      'fenodes' = '172.27.95.211:30031',
      'table.identifier' = 'sa.dwd_fr24_aircraft_list_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='100000',
      'sink.batch.interval'='10s'
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

-- 机场名称
drop table if exists dws_airport_detail_info;
create table dws_airport_detail_info (
                                         icao 			string COMMENT 'icao',
                                         iata 			string COMMENT 'iata',
                                         airport 		string COMMENT '机场英文名称',
                                         airport_name 	string COMMENT '机场中文名称',
                                         latitude        double COMMENT '纬度',
                                         longitude       double COMMENT '经度',
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


create function getCountry as 'com.jingan.udf.sea.GetCountryFromLngLat';
create function getSeaArea as 'com.jingan.udf.sea.GetSeaArea';


drop table if exists tmp_fr24_aircraft_01;
create view tmp_fr24_aircraft_01 as
select
    id                             , -- fr24网站飞机编号
    from_unixtime(`time`) as acquire_time                   , -- 采集时间
    `time`                           , -- 时间，unixtime
    if(number='N/A',cast(null as string),number) as number                         , -- 航班号
    if(icao_24bit='N/A',cast(null as string),icao_24bit) as icao_24bit                     , -- 飞机的24位ICAO地址，用于在航空通信中唯一标识飞机
    coalesce(if(a.registration='N/A',cast(null as string),a.registration),b.registration) as  registration                   , -- 飞机的注册号，唯一标识特定飞机
    coalesce(if(aircraft_code='N/A',cast(null as string),aircraft_code),b.icao_type) as aircraft_code                  , -- 飞机型号代码
    if(callsign='N/A',cast(null as string),callsign) as callsign                       , -- 飞机的呼号，通常是航班号
    latitude                       , -- 纬度
    longitude                      , -- 经度
    vertical_speed                 , -- 飞机的垂直速度，单位是英尺/分钟
    ground_speed                   , -- 飞机的地面速度，单位是节
    heading                        , -- 飞机的航向，表示飞机指向的方向
    altitude                       , -- 飞机的飞行高度
    on_ground                      , -- 表示飞机是否在地面上。0表示飞机正在飞行，1表示飞机在地面上。
    if(squawk='N/A',cast(null as string),squawk) as squawk                         , -- Mode-3/A 应答机代码，通常为 4 位八进制数，
    if(origin_airport_iata='N/A',cast(null as string),origin_airport_iata) as origin_airport_iata            , -- 起始机场的IATA代码
    if(destination_airport_iata='N/A',cast(null as string),destination_airport_iata) as destination_airport_iata       , -- 目的地机场的IATA代码
    if(airline_iata='N/A',cast(null as string),airline_iata) as airline_iata                   , -- 航空公司的IATA代码
    if(airline_icao='N/A',cast(null as string),airline_icao) as airline_icao                   , -- 航空公司的ICAO代码
    b.is_mil,      -- 是否军用
    if(longitude is not null, getCountry(cast(longitude as double),cast(latitude as double)),cast(null as string))                             as country_code3, -- 经纬度位置转换国家
    getSeaArea(cast(longitude as double),cast(latitude as double)) as sea_id   ,-- 计算海域id
    proctime
from flightradar24_aircraft_list a
         left join dws_aircraft_info
    FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.icao_24bit=b.icao_code;




drop table if exists tmp_fr24_aircraft_02;
create view tmp_fr24_aircraft_02 as
select
    a.*,
    coalesce(b.airport,c.airport)                  as origin_airport_e_name , -- 来源机场英文,
    coalesce(b.airport_name,c.airport_name)        as origin_airport_c_name , -- 来源机场中文,
    coalesce(b.longitude,c.longitude)                              as origin_lng,
    coalesce(b.latitude,c.latitude)                                as origin_lat,
    coalesce(d.airport,f.airport)                  as dest_airport_e_name,
    coalesce(d.airport_name,f.airport_name)        as dest_airport_c_name,
    coalesce(d.longitude,f.longitude)                          as dest_lng,
    coalesce(d.latitude,f.latitude)                            as dest_lat,
    g.category_code                                 as flight_category,         -- 飞机类型
    g.category_c_name                               as flight_category_name,        -- 飞机类型名称
    t2.e_name as airlines_e_name,  -- 航空公司英文名称
    t2.c_name as airlines_c_name,  -- 航空公司中文名称
    t3.code2 as position_country_2code,  -- 当前所处的区域
    t4.c_name as sea_name,    -- 关联查询海域名称
    if(instr(registration,'-')>0,substring(registration,1,2),concat(substring(registration,1,1),'-')) as prefix_code2,
    if(instr(registration,'-')>0,substring(registration,1,3),concat(substring(registration,1,2),'-')) as prefix_code3,
    if(instr(registration,'-')>0,substring(registration,1,4),concat(substring(registration,1,3),'-')) as prefix_code4,
    if(instr(registration,'-')>0,substring(registration,1,5),concat(substring(registration,1,4),'-')) as prefix_code5
from tmp_fr24_aircraft_01 a
         left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.origin_airport_iata = b.icao
         left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as c
                   on a.origin_airport_iata = c.iata
         left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as d
                   on a.destination_airport_iata = d.icao
         left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as f
                   on a.destination_airport_iata = f.iata
         left join dim_aircraft_type_category FOR SYSTEM_TIME AS OF a.proctime as g
                   on a.aircraft_code = g.id
         left join dim_airline_list_info
    FOR SYSTEM_TIME AS OF a.proctime as t2
                   on a.airline_icao = t2.icao
         left join dim_country_info
    FOR SYSTEM_TIME AS OF a.proctime as t3
                   on a.country_code3=t3.code3
         left join dim_sea_area
    FOR SYSTEM_TIME AS OF a.proctime as t4
                   on a.sea_id = t4.id;


drop table if exists tmp_fr24_aircraft_03;
create view tmp_fr24_aircraft_03 as
select
    t1.*,
    if(position_country_2code is null
           and ((cast(longitude as double) between 107.491636 and 124.806089 and cast(latitude as double) between 20.522241 and 40.799277)
            or
                (cast(longitude as double) between 107.491636 and 121.433286 and cast(latitude as double) between 3.011639 and 20.522241)
           )
        ,'CN', position_country_2code) as position_country_code2,
    t7.country_code as country_code7,
    t6.country_code as country_code6,
    t5.country_code as country_code5,
    t4.country_code as country_code4
from tmp_fr24_aircraft_02 t1
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



drop table if exists tmp_fr24_aircraft_04;
create view tmp_fr24_aircraft_04 as
select
    t1.*,
    case
        when t1.country_code in('IN','US','JP','AU') and is_mil = 1 then 'ENENY'  -- 美日..军机 敌方
        when t1.country_code = 'CN' and is_mil = 1 then 'OUR_SIDE' -- 中国的军机 我方
        when t1.country_code = 'CN' and is_mil = 0 then 'FRIENDLY_SIDE' -- 中国的非军机 友方
        else 'NEUTRALITY' end as friend_foe,  -- 由国家判断敌我
    t2.c_name as country_name,  -- 所属国家中文名称
    t3.c_name as position_country_name
from (select *,coalesce(country_code7,country_code6,country_code5,country_code4) as country_code  from tmp_fr24_aircraft_03) t1
         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.country_code = t2.country_code2 and 'COMMON' = t2.source
         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.position_country_code2 = t3.country_code2 and 'COMMON' = t3.source;


drop table if exists tmp_fr24_aircraft_05;
create view tmp_fr24_aircraft_05 as
select
    if(icao_24bit is null,id,icao_24bit)       as flight_id,
    acquire_time                        as acquire_time,
    3                                               as src_code,
    if(char_length(icao_24bit)=6,icao_24bit,cast(null as string))                                          as icao_code,
    registration,
    number as flight_no,
    callsign                           as callsign,
    aircraft_code as flight_type,
    is_mil                                          as is_military,  -- 是否军用,
    if(icao_24bit is null,'id','hex')             as pk_type,
    id                                 as src_pk,
    flight_category                                 as flight_category,         -- 飞机类型
    flight_category_name                               as flight_category_name,        -- 飞机类型名称
    longitude                       as lng,
    latitude                       as lat,
    ground_speed                           as speed,
    ground_speed * 1.852                   as speed_km,
    altitude                        as altitude_baro ,
    altitude * 0.3048               as altitude_baro_m,
    cast(null as double)                            as altitude_geom,
    cast(null as double)                            as altitude_geom_m,
    heading                         as heading,
    squawk as squawk_code,
    cast(null as string) flight_status,
    cast(null as int)              as special, -- 是否有特殊情况
    origin_airport_iata as origin_airport3_code,
    origin_airport_e_name                  as origin_airport_e_name , -- 来源机场英文,
    origin_airport_c_name        as origin_airport_c_name , -- 来源机场中文,
    origin_lng                               as origin_lng,
    origin_lat                                as origin_lat,
    destination_airport_iata                      as dest_airport3_code,
    dest_airport_e_name                  as dest_airport_e_name,
    dest_airport_c_name        as dest_airport_c_name,
    dest_lng                          as dest_lng,
    dest_lat                           as dest_lat,
    cast(null as string) as flight_photo,
    cast(null as string)               as flight_departure_time,
    cast(null as string)                   as expected_landing_time,
    cast(null as double)        as to_destination_distance,
    cast(null as string) as estimated_landing_duration,
    airline_icao as airlines_icao,
    airlines_e_name,
    airlines_c_name,
    country_code,
    country_name,
    cast(null as string) as data_source,
    cast(null as string) as source,
    position_country_code2,
    position_country_name,
    friend_foe,
    sea_id,
    sea_name,
    cast(null as varchar)                         as h3_code,
    concat('{',
           if(vertical_speed is null,'',concat('"vertical_speed":',cast(vertical_speed as string),',')),
           if(on_ground is null,'',concat('"on_ground":',cast(on_ground as string),',')),
           if(airline_iata is null,'',concat('"airline_iata":"',cast(on_ground as string),'",')),
           if(`time` is null,'',concat('"time":',cast(`time` as string))),
           '}') as extend_info,
    from_unixtime(unix_timestamp())  as update_time -- 数据入库时间
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
from tmp_fr24_aircraft_05;

insert into dws_aircraft_combine_status_rt
select
    flight_id											, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
    src_code											, -- 来源网站标识 1. radarbox 2. adsbexchange
    acquire_time										, -- 采集时间
    icao_code											, -- 24位 icao编码
    registration										, -- 注册号
    flight_no											, -- 航班号
    callsign											, -- 呼号
    flight_type										, -- 飞机型号
    is_military										, -- 是否军用飞机 0 非军用 1 军用
    pk_type											, -- flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex
    src_pk											, -- 源网站主键
    flight_category									, -- 飞机类型
    flight_category_name						        , -- 飞机类型名称
    lng												, -- 经度
    lat												, -- 纬度
    speed												, -- 飞行当时的速度（单位：节）
    speed_km											, -- 速度单位 km/h
    altitude_baro										, -- 气压高度 海拔 航班当前高度，单位为（ft）
    altitude_baro_m									, -- 气压高度 海拔 单位米
    altitude_geom										, -- 海拔高度 海拔 航班当前高度，单位为（ft）
    altitude_geom_m									, -- 海拔高度 海拔 单位米
    heading											, -- 方向  正北为0
    squawk_code										, -- 当前应答机代码
    flight_status										, -- 飞机状态： 已启程
    special											, -- 是否有特殊情况
    origin_airport3_code						        , -- 起飞机场的iata代码
    origin_airport_e_name						        , -- 来源机场英文
    origin_airport_c_name						        , -- 来源机场中文
    origin_lng										, -- 来源机场经度
    origin_lat										, -- 来源机场纬度
    dest_airport3_code							    , -- 目标机场的 iata 代码
    dest_airport_e_name							    , -- 目的机场英文
    dest_airport_c_name							    , -- 目的机场中文
    dest_lng											, -- 目的地坐标经度
    dest_lat											, -- 目的地坐标纬度
    flight_photo										, -- 飞机的图片
    flight_departure_time						        , -- 航班起飞时间
    expected_landing_time						        , -- 预计降落时间
    to_destination_distance					        , -- 目的地距离
    estimated_landing_duration			            , -- 预计还要多久着陆
    airlines_icao										, -- 航空公司的icao代码
    airlines_e_name									, -- 航空公司英文
    airlines_c_name									, -- 航空公司中文
    country_code										, -- 飞机所属国家代码
    country_name										, -- 国家中文
    data_source										, -- 数据来源的系统
    source											, -- 来源
    position_country_code2					        , -- 位置所在国家简称
    position_country_name						        , -- 位置所在国家名称
    friend_foe										, -- 敌我
    sea_id											, -- 海域id
    sea_name											, -- 海域名字
    h3_code											, -- 位置h3编码
    extend_info										, -- 扩展信息 json 串
    from_unixtime(unix_timestamp()) as update_time	-- 更新时间
from tmp_fr24_aircraft_05;


end;
