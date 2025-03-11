--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/10/16 17:30:49
-- description: 飞机出发、到达、地面航班数据处理实时数据
--********************************************************************--

set 'pipeline.name' = 'ja-airport-dep-arr';

set 'sql-client.execution.result-mode' = 'TABLEAU';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';


-- checkpoint时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'execution.checkpointing.timeout' = '360000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-airport-dep-arr';


 -- -----------------------

 -- source数据结构

 -- -----------------------

-- 出发航班信息
create table fr24_airport_departure_kafka(
                                             flight_airport            string, -- 飞机机场 iata或者icao
                                             flight_type               string, -- 起飞降落
                                             acquire_time              string, -- 采集时间string
                                             acquire_timestamp         string, -- 采集时间(unix_time)

                                             flight_number             string, -- 航班编号
                                             flight_callsign           string, -- 航班编号
                                             flight_codeshare          string, -- 共享代码
                                             status_live               string, -- 是否活动，飞行中
                                             status_desc               string, -- 状态描述

                                             id                        string, -- fr24网站飞机编号
                                             hex                       string, -- 飞机icao
                                             aircraft_reg              string, -- 飞机注册号
                                             aircraft_seriaNo          string, --
                                             aircraft_model            string, -- 飞机型号
                                             aircraft_desc             string, -- 飞机类型描述
                                             aircraft_date             string, -- 飞机启用时间
                                             airline_name              string, -- 航空公司英文名称
                                             airline_iata              string, -- 航空公司iata
                                             airline_icao              string, -- 航空公司icao

                                             airport_from_name         string, -- 出发机场英文名称
                                             airport_from_iata         string, -- 出发机场iata
                                             airport_from_icao         string, -- 出发机场icao
                                             airport_from_country      string, -- 出发机场国家
                                             airport_from_city         string, -- 出发机场城市
                                             airport_from_latitude     string, -- 出发机场纬度
                                             airport_from_longitude    string, -- 出发机场经度
                                             airport_from_time_zone    string, -- 出发机场时区
                                             airport_from_time_offset  string, -- 出发机场时间偏移（秒）
                                             airport_from_terminal     string, -- 出发机场航站楼
                                             airport_from_baggate      string, -- 出发机场行李处
                                             airport_from_gate         string, -- 出发机场登记口

                                             airport_to_name           string, -- 到达机场英文名称
                                             airport_to_iata           string, -- 到达机场iata
                                             airport_to_icao           string, -- 到达机场icao
                                             airport_to_country        string, -- 到达机场国家
                                             airport_to_city           string, -- 到达机场城市
                                             airport_to_latitude       string, -- 到达机场纬度
                                             airport_to_longitude      string, -- 到达机场经度
                                             airport_to_time_zone      string, -- 到达机场时区
                                             airport_to_time_offset    string, -- 到达机场时间偏移（秒）
                                             airport_to_terminal       string, -- 到达机场航站楼
                                             airport_to_baggate        string, -- 到达机场行李处
                                             airport_to_gate           string, -- 到达机场登记口

                                             std_bj_time               string, -- 计划出发时间(北京时间)
                                             sta_bj_time               string, -- 计划到达时间(北京时间)
                                             atd_bj_time               string, -- 实际起飞时间(北京时间)
                                             ata_bj_time               string, -- 实际降落时间(北京时间)
                                             eta_bj_time               string, -- 预计降落时间(北京时间)
                                             fligh_time                string, -- 飞行时间(秒)

                                             std_unix_time             string, -- 计划出发时间(unix time)
                                             std_local_time            string, -- 计划出发时间(当地时间)
                                             sta_unix_time             string, -- 计划到达时间(unix time)
                                             sta_local_time            string, -- 计划到达时间(当地时间)
                                             atd_unix_time             string, -- 实际起飞时间(unix time)
                                             atd_local_time            string, -- 实际起飞时间(当地时间)
                                             ata_unix_time             string, -- 实际降落时间(unix time)
                                             ata_local_time            string, -- 实际降落时间(当地时间)
                                             eta_unix_time             string, -- 预计降落时间(unix time)
                                             eta_local_time            string -- 预计降落时间(当地时间)
) with (
      'connector' = 'kafka',
      'topic' = 'fr24_airport_departure_2',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'fr24_airport_departure_info',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1729828503000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 到达航班信息
create table fr24_airport_arrival_kafka(
                                           flight_airport            string, -- 飞机机场 iata或者icao
                                           flight_type               string, -- 起飞降落
                                           acquire_time              string, -- 采集时间string
                                           acquire_timestamp         string, -- 采集时间(unix_time)

                                           flight_number             string, -- 航班编号
                                           flight_callsign           string, -- 航班编号
                                           flight_codeshare          string, -- 共享代码
                                           status_live               string, -- 是否活动，飞行中
                                           status_desc               string, -- 状态描述

                                           id                        string, -- fr24网站飞机编号
                                           hex                       string, -- 飞机icao
                                           aircraft_reg              string, -- 飞机注册号
                                           aircraft_seriaNo          string, --
                                           aircraft_model            string, -- 飞机型号
                                           aircraft_desc             string, -- 飞机类型描述
                                           aircraft_date             string, -- 飞机启用时间
                                           airline_name              string, -- 航空公司英文名称
                                           airline_iata              string, -- 航空公司iata
                                           airline_icao              string, -- 航空公司icao

                                           airport_from_name         string, -- 出发机场英文名称
                                           airport_from_iata         string, -- 出发机场iata
                                           airport_from_icao         string, -- 出发机场icao
                                           airport_from_country      string, -- 出发机场国家
                                           airport_from_city         string, -- 出发机场城市
                                           airport_from_latitude     string, -- 出发机场纬度
                                           airport_from_longitude    string, -- 出发机场经度
                                           airport_from_time_zone    string, -- 出发机场时区
                                           airport_from_time_offset  string, -- 出发机场时间偏移（秒）
                                           airport_from_terminal     string, -- 出发机场航站楼
                                           airport_from_baggate      string, -- 出发机场行李处
                                           airport_from_gate         string, -- 出发机场登记口

                                           airport_to_name           string, -- 到达机场英文名称
                                           airport_to_iata           string, -- 到达机场iata
                                           airport_to_icao           string, -- 到达机场icao
                                           airport_to_country        string, -- 到达机场国家
                                           airport_to_city           string, -- 到达机场城市
                                           airport_to_latitude       string, -- 到达机场纬度
                                           airport_to_longitude      string, -- 到达机场经度
                                           airport_to_time_zone      string, -- 到达机场时区
                                           airport_to_time_offset    string, -- 到达机场时间偏移（秒）
                                           airport_to_terminal       string, -- 到达机场航站楼
                                           airport_to_baggate        string, -- 到达机场行李处
                                           airport_to_gate           string, -- 到达机场登记口

                                           std_bj_time               string, -- 计划出发时间(北京时间)
                                           sta_bj_time               string, -- 计划到达时间(北京时间)
                                           atd_bj_time               string, -- 实际起飞时间(北京时间)
                                           ata_bj_time               string, -- 实际降落时间(北京时间)
                                           eta_bj_time               string, -- 预计降落时间(北京时间)
                                           fligh_time                string, -- 飞行时间(秒)

                                           std_unix_time             string, -- 计划出发时间(unix time)
                                           std_local_time            string, -- 计划出发时间(当地时间)
                                           sta_unix_time             string, -- 计划到达时间(unix time)
                                           sta_local_time            string, -- 计划到达时间(当地时间)
                                           atd_unix_time             string, -- 实际起飞时间(unix time)
                                           atd_local_time            string, -- 实际起飞时间(当地时间)
                                           ata_unix_time             string, -- 实际降落时间(unix time)
                                           ata_local_time            string, -- 实际降落时间(当地时间)
                                           eta_unix_time             string, -- 预计降落时间(unix time)
                                           eta_local_time            string -- 预计降落时间(当地时间)
) with (
      'connector' = 'kafka',
      'topic' = 'fr24_airport_arrival_2',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'fr24_airport_arrival_info',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1729828503000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 地面航班数据来源（kafka）
create table fr24_airport_on_ground_kafka(
                                             flight_airport            string, -- 飞机机场 iata或者icao
                                             flight_type               string, -- 起飞降落
                                             acquire_time              string, -- 采集时间string
                                             acquire_timestamp         string, -- 采集时间(unix_time)

                                             flight_number             string, -- 航班编号
                                             flight_callsign           string, -- 航班编号
                                             flight_codeshare          string, -- 共享代码

                                             id                        string, -- fr24网站飞机编号
                                             hex                       string, -- 飞机icao
                                             aircraft_reg              string, -- 飞机注册号
                                             aircraft_seriaNo          string, --
                                             aircraft_model            string, -- 飞机型号
                                             aircraft_desc             string, -- 飞机类型描述
                                             airline_name              string, -- 航空公司英文名称
                                             airline_iata              string, -- 航空公司iata
                                             airline_icao              string, -- 航空公司icao

                                             on_ground_update_unix     string, -- 在地面时间时间戳
                                             on_ground_update          string, -- 在地面时间
                                             hours_diff                string,
                                             time_diff                 string,
                                             airport_name              string, -- 机场名称
                                             airport_iata              string, -- 机场iata
                                             airport_icao              string  -- 机场icao
) with (
      'connector' = 'kafka',
      'topic' = 'fr24_airport_on_ground_2',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'fr24_airport_on_ground_info',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 机场实体表
drop table if exists dws_et_airport_info;
create table dws_et_airport_info (
                                     id        string, -- id,
                                     icao      string, -- icao
                                     iata      string, -- iata
                                     c_name    string, -- 中文名称
                                     e_name    string, -- 英文名称
                                     longitude double,
                                     latitude  double,
                                     primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_airport_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 飞机实体表（Source：doris）
drop table if exists dws_et_aircraft_info;
create table dws_et_aircraft_info (
                                      flight_id           string        comment '飞机id',
                                      icao_code           string        comment '飞机的 24 位 ICAO 标识符，为 6 个十六进制数字 大写',
                                      registration        string        comment '地区国家三位编码',
                                      flight_type         string        comment '飞机的机型型码，用于标识不同类型的飞机',
                                      country_code        string,
                                      country_name        string,
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



-- ----------------------- sink数据结构 -----------------------

-- 航班数据写入表（Sink：doris）
drop table if exists dwd_bhv_airport_flight_rt;
create table dwd_bhv_airport_flight_rt (
                                           id                        string, -- 飞机机场 iata或者icao
                                           src_code                  string, -- 起飞降落类型，flight_departure-起飞，flight_arrival-到达
                                           acquire_time              string, -- 采集时间string
                                           flight_id                 string, -- 飞机id

                                           acquire_timestamp         bigint, -- 采集时间(unix_time)
                                           flight_number             string, -- 航班编号
                                           flight_callsign           string, -- 航班编号
                                           flight_codeshare          string, -- 共享代码
                                           status_live               string, -- 是否活动，飞行中
                                           status_desc               string, -- 状态描述
                                           status_desc_name			string,	-- '状态描述中文',

                                           f24_id                    string, -- fr24网站飞机编号
                                           hex                       string, -- icao
                                           aircraft_reg              string, -- 飞机注册号
                                           aircraft_type             string, -- 飞机型号 - 机型
                                           aircraft_desc             string, -- 飞机类型描述
                                           aircraft_date             string, -- 飞机启用时间
                                           airline_name              string, -- 航空公司英文名称
                                           airline_iata              string, -- 航空公司iata
                                           airline_icao              string, -- 航空公司icao
                                           country_code              string, -- 国家代码
                                           country_name              string, -- 国家名称

                                           airport_from_id           string, -- 出发机场id
                                           airport_from_e_name       string, -- 出发机场英文名称
                                           airport_from_c_name       string, -- 出发机场中文名称
                                           airport_from_iata         string, -- 出发机场iata
                                           airport_from_icao         string, -- 出发机场icao
                                           airport_from_country      string, -- 出发机场国家
                                           airport_from_city         string, -- 出发机场城市
                                           airport_from_latitude     double, -- 出发机场纬度
                                           airport_from_longitude    double, -- 出发机场经度
                                           airport_from_time_zone    string, -- 出发机场时区
                                           airport_from_time_offset  double, -- 出发机场时间偏移（秒）
                                           airport_from_terminal     string, -- 出发机场航站楼
                                           airport_from_baggate      string, -- 出发机场行李处
                                           airport_from_gate         string, -- 出发机场登记口

                                           airport_to_id             string, -- 出发机场id
                                           airport_to_e_name         string, -- 到达机场英文名称
                                           airport_to_c_name         string, -- 到达机场中文名称
                                           airport_to_iata           string, -- 到达机场iata
                                           airport_to_icao           string, -- 到达机场icao
                                           airport_to_country        string, -- 到达机场国家
                                           airport_to_city           string, -- 到达机场城市
                                           airport_to_latitude       double, -- 到达机场纬度
                                           airport_to_longitude      double, -- 到达机场经度
                                           airport_to_time_zone      string, -- 到达机场时区
                                           airport_to_time_offset    double, -- 到达机场时间偏移（秒）
                                           airport_to_terminal       string, -- 到达机场航站楼
                                           airport_to_baggate        string, -- 到达机场行李处
                                           airport_to_gate           string, -- 到达机场登记口

                                           std_bj_time               string, -- 计划出发时间(北京时间)
                                           sta_bj_time               string, -- 计划到达时间(北京时间)
                                           atd_bj_time               string, -- 实际起飞时间(北京时间)
                                           ata_bj_time               string, -- 实际降落时间(北京时间)
                                           eta_bj_time               string, -- 预计降落时间(北京时间)
                                           fligh_time                string, -- 飞行时间(秒)
                                           extend_info               string, -- 扩展字段

                                           std_unix_time             bigint, -- 计划出发时间(unix time)
                                           std_local_time            string, -- 计划出发时间(当地时间)

                                           sta_unix_time             bigint, -- 计划到达时间(unix time)
                                           sta_local_time            string, -- 计划到达时间(当地时间)

                                           atd_unix_time             bigint, -- 实际起飞时间(unix time)
                                           atd_local_time            string, -- 实际起飞时间(当地时间)

                                           ata_unix_time             bigint, -- 实际降落时间(unix time)
                                           ata_local_time            string, -- 实际降落时间(当地时间)

                                           eta_unix_time             bigint, -- 预计降落时间(unix time)
                                           eta_local_time            string, -- 预计降落时间(当地时间)
                                           update_time               string
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.244:8030',
      'table.identifier' = 'sa.dwd_bhv_airport_flight_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='30000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 机场航班最大时间表
drop table if exists dws_bhv_airport_flight_time;
create table dws_bhv_airport_flight_time (
                                             id                        string, -- 机场id
                                             src_code                  string, -- 起飞降落类型，flight_departure-起飞，flight_arrival-到达
                                             acquire_time              string, -- 采集时间string
                                             update_time               string
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.244:8030',
      'table.identifier' = 'sa.dws_bhv_airport_flight_time',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='30000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );




-- 地面航班（Sink：doris）
drop table if exists dwd_bhv_airport_onground_rt;
create table dwd_bhv_airport_onground_rt (
                                             id                        string, -- 飞机机场 iata或者icao
                                             acquire_time              string, -- 采集时间string
                                             flight_id                 string, -- 飞机id

                                             src_code                  string, -- 起飞降落类型
                                             acquire_timestamp         bigint, -- 采集时间(unix_time)
                                             flight_number             string, -- 航班编号
                                             flight_callsign           string, -- 航班编号
                                             flight_codeshare          string, -- 共享代码

                                             f24_id                    string, -- fr24网站飞机编号
                                             hex                       string, -- icao
                                             aircraft_reg              string, -- 飞机注册号
                                             aircraft_type             string, -- 飞机型号 - 机型
                                             aircraft_desc             string, -- 飞机类型描述
                                             airline_name              string, -- 航空公司英文名称
                                             airline_iata              string, -- 航空公司iata
                                             airline_icao              string, -- 航空公司icao
                                             country_code              string, -- 国家代码
                                             country_name              string, -- 国家名称

                                             on_ground_update_unix     bigint, -- 在地面时间戳
                                             on_ground_update     	    string, -- 在地面时间
                                             hours_diff                double,
                                             time_diff 				double,
                                             airport_e_name			string, -- 机场英文名称
                                             airport_c_name 			string, -- 机场中文名称
                                             airport_iata				string, -- 机场iata
                                             airport_icao				string, -- 机场icao
                                             extend_info               string,
                                             update_time               string
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dwd_bhv_airport_onground_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='30000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- -----------------------

-- 数据处理

-- -----------------------

-- 出发到达数据聚合
create view tmp_01 as
select
    *
from fr24_airport_departure_kafka
union all
select * from fr24_airport_arrival_kafka;



-- 出发到达数据字段整理
create view tmp_02 as
select
    flight_airport,
    flight_type         as src_code,
    acquire_time,
    cast(acquire_timestamp as bigint) as acquire_timestamp,

    if(flight_number = '',cast(null as varchar),flight_number)       as flight_number,
    if(flight_callsign = '',cast(null as varchar),flight_callsign)   as flight_callsign,
    if(flight_codeshare = '',cast(null as varchar),flight_codeshare) as flight_codeshare,
    if(status_live = '',cast(null as varchar),status_live)           as status_live,
    if(status_desc = '',cast(null as varchar),status_desc)           as status_desc,

    if(id = '',cast(null as varchar),id)                             as f24_id,
    if(hex = '',cast(null as varchar),hex)                           as hex,
    if(aircraft_reg = '',cast(null as varchar),aircraft_reg)         as aircraft_reg,
    if(aircraft_model = '',cast(null as varchar),aircraft_model)     as aircraft_type,
    aircraft_desc,
    aircraft_date,
    if(airline_name = '',cast(null as varchar),airline_name) as airline_name,
    if(airline_iata = '',cast(null as varchar),airline_iata) as airline_iata,
    if(airline_icao = '',cast(null as varchar),airline_icao) as airline_icao,

    airport_from_name,
    airport_from_iata,
    airport_from_icao,
    airport_from_country,
    airport_from_city,
    cast(airport_from_latitude as double) as airport_from_latitude,
    cast(airport_from_longitude as double) as airport_from_longitude,
    airport_from_time_zone,
    cast(airport_from_time_offset as double) as airport_from_time_offset,
    if(airport_from_terminal = '',cast(null as varchar),airport_from_terminal) as airport_from_terminal,
    if(airport_from_baggate = '',cast(null as varchar),airport_from_baggate) as airport_from_baggate,
    if(airport_from_gate = '',cast(null as varchar),airport_from_gate) as airport_from_gate,

    airport_to_name,
    airport_to_iata,
    airport_to_icao,
    airport_to_country,
    airport_to_city,
    cast(airport_to_latitude as double)  as airport_to_latitude,
    cast(airport_to_longitude as double) as airport_to_longitude,
    airport_to_time_zone,
    cast(airport_to_time_offset as double) as airport_to_time_offset,
    if(airport_to_terminal = '',cast(null as varchar),airport_to_terminal) as airport_to_terminal,
    if(airport_to_baggate = '',cast(null as varchar),airport_to_baggate) as airport_to_baggate,
    if(airport_to_gate = '',cast(null as varchar),airport_to_gate) as airport_to_gate,

    if(std_bj_time = '',cast(null as varchar),std_bj_time) as std_bj_time,
    if(sta_bj_time = '',cast(null as varchar),sta_bj_time) as sta_bj_time,
    if(atd_bj_time = '',cast(null as varchar),atd_bj_time) as atd_bj_time,
    if(ata_bj_time = '',cast(null as varchar),ata_bj_time) as ata_bj_time,
    if(eta_bj_time = '',cast(null as varchar),eta_bj_time) as eta_bj_time,
    if(fligh_time = '',cast(null as varchar),fligh_time) as fligh_time,

    cast(std_unix_time as bigint) as std_unix_time,
    std_local_time,

    cast(sta_unix_time as bigint) as sta_unix_time,
    sta_local_time,

    cast(atd_unix_time as bigint) as atd_unix_time,
    atd_local_time,

    cast(ata_unix_time as bigint) as ata_unix_time,
    ata_local_time,

    cast(eta_unix_time as bigint) as eta_unix_time,
    eta_local_time,
    PROCTIME()  as proctime
from tmp_01
where acquire_time <> ''
  and hex is not null
  and hex <> ''
  and flight_type <> '';


-- 出发到达关联取机场id
create view tmp_03 as
select
    if(src_code = 'flight_departure',t2.id,t3.id) as id,
    src_code,
    acquire_time,
    hex          as flight_id,
    -- t4.flight_id as flight_id,

    acquire_timestamp,
    flight_number,
    flight_callsign,
    flight_codeshare,
    status_live,
    status_desc,
    cast(null as varchar) as status_desc_name,

    f24_id,
    hex,
    aircraft_reg,
    aircraft_type,
    aircraft_desc,
    aircraft_date,
    airline_name,
    airline_iata,
    airline_icao,
    t4.country_code,
    t4.country_name,

    t2.id as airport_from_id,
    coalesce(t1.airport_from_name,t2.e_name) as airport_from_e_name,
    t2.c_name as airport_from_c_name,
    airport_from_iata,
    airport_from_icao,
    airport_from_country,
    airport_from_city,
    airport_from_latitude,
    airport_from_longitude,
    airport_from_time_zone,
    airport_from_time_offset,
    airport_from_terminal,
    airport_from_baggate,
    airport_from_gate,

    t3.id as airport_to_id,
    coalesce(t1.airport_to_name,t3.e_name) as airport_to_e_name,
    t3.c_name as airport_to_c_name,
    airport_to_iata,
    airport_to_icao,
    airport_to_country,
    airport_to_city,
    airport_to_latitude,
    airport_to_longitude,
    airport_to_time_zone,
    airport_to_time_offset,
    airport_to_terminal,
    airport_to_baggate,
    airport_to_gate,

    std_bj_time,
    sta_bj_time,
    atd_bj_time,
    ata_bj_time,
    eta_bj_time,
    fligh_time,
    flight_airport as extend_info,

    std_unix_time,
    std_local_time,

    sta_unix_time,
    sta_local_time,

    atd_unix_time,
    atd_local_time,

    ata_unix_time,
    ata_local_time,

    eta_unix_time,
    eta_local_time

from tmp_02 as t1
         left join dws_et_airport_info FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on if(t1.airport_from_icao is not null,t1.airport_from_icao,t1.airport_from_iata) = t2.icao    -- 起飞

         left join dws_et_airport_info FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on if(t1.airport_to_icao is not null,t1.airport_to_icao,t1.airport_to_iata) = t3.icao     -- 到达

         left join dws_et_aircraft_info FOR SYSTEM_TIME AS OF t1.proctime as t4      -- 飞机实体
                   on t1.hex = t4.flight_id
     -- on t1.aircraft_reg = t4.registration
where coalesce(t2.id,t3.id) is not null;



-- 出发到达过滤数据
-- create view tmp_04 as
-- select
--   *
-- from tmp_03
-- where id is not null
--   and flight_id is not null;



-- 地面航班字段整理
create view tmp_ground_01 as
select
    flight_airport,
    acquire_time,

    flight_type         as src_code,
    cast(acquire_timestamp as bigint) as acquire_timestamp,
    if(flight_number = '',cast(null as varchar),flight_number)       as flight_number,
    if(flight_callsign = '',cast(null as varchar),flight_callsign)   as flight_callsign,
    if(flight_codeshare = '',cast(null as varchar),flight_codeshare) as flight_codeshare,

    if(id = '',cast(null as varchar),id)                             as f24_id,
    if(hex = '',cast(null as varchar),hex)                           as hex,
    if(aircraft_reg = '',cast(null as varchar),aircraft_reg)         as aircraft_reg,
    if(aircraft_model = '',cast(null as varchar),aircraft_model)     as aircraft_type,
    aircraft_desc,
    if(airline_name = '',cast(null as varchar),airline_name) as airline_name,
    if(airline_iata = '',cast(null as varchar),airline_iata) as airline_iata,
    if(airline_icao = '',cast(null as varchar),airline_icao) as airline_icao,

    cast(on_ground_update_unix as bigint)     as on_ground_update_unix,
    if(on_ground_update = '',cast(null as varchar),on_ground_update) as on_ground_update,
    cast(hours_diff as double) as hours_diff,
    cast(time_diff as double) as time_diff,
    if(airport_name = '',cast(null as varchar),airport_name) as airport_name,
    if(airport_iata = '',cast(null as varchar),airport_iata) as airport_iata,
    if(airport_icao = '',cast(null as varchar),airport_icao) as airport_icao,
    PROCTIME()  as proctime
from fr24_airport_on_ground_kafka
where flight_type is not null    -- flight_type不是飞机类型，而是航班的类型
  and hex is not null;



-- 地面航班关联数据
create view tmp_ground_02 as
select
    t2.id as id,
    acquire_time,
    hex as flight_id,

    src_code,
    acquire_timestamp,
    flight_number,
    flight_callsign,
    flight_codeshare,

    f24_id,
    hex,
    aircraft_reg,
    coalesce(t1.aircraft_type,t4.flight_type) as aircraft_type,
    aircraft_desc,
    airline_name,
    airline_iata,
    airline_icao,
    t4.country_code,
    t4.country_name,

    on_ground_update_unix,
    on_ground_update,
    hours_diff,
    time_diff,

    coalesce(t2.e_name,t1.airport_name) as airport_e_name,
    t2.c_name as airport_c_name,
    airport_iata,
    airport_icao,
    flight_airport as extend_info

from tmp_ground_01 as t1
         left join dws_et_airport_info FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on if(t1.airport_icao is not null,t1.airport_icao,t1.flight_airport) = t2.icao    -- 起飞

         left join dws_et_aircraft_info FOR SYSTEM_TIME AS OF t1.proctime as t4      -- 飞机实体
                   on t1.hex = t4.flight_id
where t2.id is not null;



-- -----------------------

-- 数据写入

-- -----------------------


begin statement set;


-- 出发到达航班记录时间
insert into dws_bhv_airport_flight_time
select
    id,
    src_code,
    acquire_time,
    from_unixtime(unix_timestamp()) as update_time
from tmp_03;


-- 地面航班记录时间
insert into dws_bhv_airport_flight_time
select
    id,
    src_code,
    acquire_time,
    from_unixtime(unix_timestamp()) as update_time
from tmp_ground_02;



-- 地面航班明细
insert into dwd_bhv_airport_onground_rt
select
    id,
    acquire_time,
    flight_id,

    src_code,
    acquire_timestamp,
    flight_number,
    flight_callsign,
    flight_codeshare,

    f24_id,
    hex,
    aircraft_reg,
    aircraft_type,
    aircraft_desc,
    airline_name,
    airline_iata,
    airline_icao,
    country_code,
    country_name,

    on_ground_update_unix,
    on_ground_update,
    hours_diff,
    time_diff,
    airport_e_name,
    airport_c_name,
    airport_iata,
    airport_icao,
    extend_info,
    from_unixtime(unix_timestamp()) as update_time
from tmp_ground_02;




-- 出发到达明细
insert into dwd_bhv_airport_flight_rt
select
    id,
    src_code,
    acquire_time,
    flight_id,

    acquire_timestamp,
    flight_number,
    flight_callsign,
    flight_codeshare,
    status_live,
    status_desc,
    status_desc_name,

    f24_id,
    hex,
    aircraft_reg,
    aircraft_type,
    aircraft_desc,
    aircraft_date,
    airline_name,
    airline_iata,
    airline_icao,
    country_code,
    country_name,

    airport_from_id,
    airport_from_e_name,
    airport_from_c_name,
    airport_from_iata,
    airport_from_icao,
    airport_from_country,
    airport_from_city,
    airport_from_latitude,
    airport_from_longitude,
    airport_from_time_zone,
    airport_from_time_offset,
    airport_from_terminal,
    airport_from_baggate,
    airport_from_gate,

    airport_to_id,
    airport_to_e_name,
    airport_to_c_name,
    airport_to_iata,
    airport_to_icao,
    airport_to_country,
    airport_to_city,
    airport_to_latitude,
    airport_to_longitude,
    airport_to_time_zone,
    airport_to_time_offset,
    airport_to_terminal,
    airport_to_baggate,
    airport_to_gate,

    std_bj_time,
    sta_bj_time,
    atd_bj_time,
    ata_bj_time,
    eta_bj_time,
    fligh_time,
    extend_info,

    std_unix_time,
    std_local_time,

    sta_unix_time,
    sta_local_time,

    atd_unix_time,
    atd_local_time,

    ata_unix_time,
    ata_local_time,

    eta_unix_time,
    eta_local_time,
    from_unixtime(unix_timestamp()) as update_time
from tmp_03;




end;


