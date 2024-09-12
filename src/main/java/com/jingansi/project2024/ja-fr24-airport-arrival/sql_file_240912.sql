--********************************************************************--
-- author:      fangzheng
-- create time: 2024/08/26 20:43:28
-- description: 读取机场出发航班列表，处理后写入doris
--********************************************************************--
set 'sql-client.execution.result-mode' = 'TABLEAU';
set 'pipeline.name' = 'flink_airport_arrival';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';


-- checkpoint时间和位置
SET 'parallelism.default' = '4';
set 'execution.checkpointing.interval' = '1200000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/flink_airport_arrival';


drop table if exists  fr24_airport_arrival_kafka;
create table fr24_airport_arrival_kafka(
                                           flight_airport     string,
                                           flight_type    string,
                                           acquire_time string,
                                           acquire_timestamp string,
                                           flight_number  string,
                                           flight_callsign  string,
                                           flight_codeshare  string,
                                           status_live  string,
                                           status_desc  string,
                                           aircraft_reg  string,
                                           aircraft_seriaNo  string,
                                           aircraft_model  string,
                                           aircraft_desc  string,
                                           airline_name  string,
                                           airline_iata  string,
                                           airline_icao  string,
                                           airport_from_name  string,
                                           airport_from_iata  string,
                                           eta_local_time  string,
                                           airport_from_icao  string,
                                           airport_from_country  string,
                                           airport_from_city  string,
                                           airport_from_latitude  string,
                                           airport_from_longitude  string,
                                           airport_from_time_zone  string,
                                           airport_from_time_offset  string,
                                           airport_from_terminal  string,
                                           airport_from_baggate  string,
                                           airport_from_gate  string,
                                           airport_to_name  string,
                                           airport_to_iata  string,
                                           airport_to_icao  string,
                                           aircraft_date  string,
                                           airport_to_country  string,
                                           airport_to_city  string,
                                           airport_to_latitude  string,
                                           airport_to_longitude  string,
                                           airport_to_time_zone  string,
                                           airport_to_time_offset  string,
                                           airport_to_terminal  string,
                                           airport_to_baggate  string,
                                           airport_to_gate  string,
                                           std_unix_time  string,
                                           std_local_time  string,
                                           std_bj_time  string,
                                           sta_unix_time  string,
                                           sta_local_time  string,
                                           sta_bj_time  string,
                                           atd_unix_time  string,
                                           atd_local_time  string,
                                           atd_bj_time  string,
                                           ata_unix_time  string,
                                           ata_local_time  string,
                                           ata_bj_time  string,
                                           eta_unix_time  string,
                                           eta_bj_time  string,
                                           fligh_time  string
) with (
      'connector' = 'kafka',
      'topic' = 'fr24_airport_arrival_2',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.201:30090',
      'properties.group.id' = 'fr24_airport_arrival_2_id',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


drop table if exists  ods_airport_flight_arrival;
create table ods_airport_flight_arrival(
                                           flight_airport string  COMMENT '航班查询机场',
                                           flight_std_time string  COMMENT '计划出发时间',
                                           acquire_time string COMMENT '采集时间',
                                           acquire_timestamp string COMMENT '采集时间',
                                           flight_number string  COMMENT '航班编号',
                                           flight_callsign string  COMMENT '航班编号',
                                           flight_codeshare string  COMMENT '共享代码',
                                           status_live string  COMMENT '是否活动，飞行中',
                                           status_desc string  COMMENT '状态描述',
                                           aircraft_reg string  COMMENT '注册号',
                                           aircraft_model string  COMMENT '飞机类型',
                                           aircraft_desc string  COMMENT '飞机类型描述',
                                           aircraft_date string  COMMENT '飞机启用时间',
                                           airline_name string  COMMENT '航空公司',
                                           airline_iata string  COMMENT '航空公司',
                                           airline_icao string  COMMENT '航空公司',
                                           airport_from_name string  COMMENT '出发机场名称',
                                           airport_from_iata string  COMMENT '出发机场iata',
                                           airport_from_icao string  COMMENT '出发机场icao',
                                           airport_from_country string  COMMENT '出发机场国家',
                                           airport_from_city string  COMMENT '出发机场城市',
                                           airport_from_latitude string  COMMENT '出发机场纬度',
                                           airport_from_longitude string  COMMENT '出发机场经度',
                                           airport_from_time_zone string  COMMENT '出发机场时区',
                                           airport_from_time_offset string  COMMENT '出发机场时间偏移（秒）',
                                           airport_from_terminal string  COMMENT '出发机场航站楼',
                                           airport_from_baggate string  COMMENT '出发机场行李处',
                                           airport_from_gate string  COMMENT '出发机场登记口',
                                           airport_to_name string  COMMENT '到达机场名称',
                                           airport_to_iata string  COMMENT '到达机场iata',
                                           airport_to_icao string  COMMENT '到达机场icao',
                                           airport_to_country string  COMMENT '到达机场国家',
                                           airport_to_city string  COMMENT '到达机场城市',
                                           airport_to_latitude string  COMMENT '到达机场纬度',
                                           airport_to_longitude string  COMMENT '到达机场经度',
                                           airport_to_time_zone string  COMMENT '到达机场时区',
                                           airport_to_time_offset string  COMMENT '到达机场时间偏移（秒）',
                                           airport_to_terminal string  COMMENT '到达机场航站楼',
                                           airport_to_baggate string  COMMENT '到达机场行李处',
                                           airport_to_gate string  COMMENT '到达机场登记口',
                                           std_unix_time string  COMMENT '计划出发时间(unix time)',
                                           std_local_time string COMMENT '计划出发时间(当地时间)',
                                           std_bj_time string COMMENT '计划出发时间(北京时间)',
                                           sta_unix_time string  COMMENT '计划到达时间(unix time)',
                                           sta_local_time string COMMENT '计划到达时间(当地时间)',
                                           sta_bj_time string COMMENT '计划到达时间(北京时间)',
                                           atd_unix_time string  COMMENT '实际起飞时间(unix time)',
                                           atd_local_time string COMMENT '实际起飞落时间(当地时间)',
                                           atd_bj_time string COMMENT '实际起飞时间(北京时间)',
                                           ata_unix_time string  COMMENT '实际降落时间(unix time)',
                                           ata_local_time string COMMENT '实际降落时间(当地时间)',
                                           ata_bj_time string COMMENT '实际降落时间(北京时间)',
                                           eta_unix_time string  COMMENT '预计降落时间(unix time)',
                                           eta_local_time string COMMENT '预计降落时间(当地时间)',
                                           eta_bj_time string COMMENT '预计降落时间(北京时间)',
                                           fligh_time string  COMMENT '飞行时间(秒)'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.ods_airport_flight_arrival',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',     -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'        -- 行分隔符
      );


--- ========  数据处理部分   ========
begin statement set;

-- 1. 将kafka数据写入doris表
insert into ods_airport_flight_arrival
select
    flight_airport,
    std_unix_time,
    acquire_time,
    acquire_timestamp,
    flight_number,
    flight_callsign,
    flight_codeshare,
    status_live ,
    status_desc,
    aircraft_reg,
    aircraft_model,
    aircraft_desc,
    aircraft_date,
    airline_name,
    airline_iata,
    airline_icao,
    airport_from_name,
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
    airport_to_name,
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
    std_unix_time,
    -- cast(if(std_unix_time = 'None', '0', std_unix_time) as bigint),
    std_local_time,
    std_bj_time,
    sta_unix_time,
    -- cast(if(sta_unix_time = 'None', '0', sta_unix_time) as bigint),
    sta_local_time,
    sta_bj_time,
    atd_unix_time,
    -- cast(if(atd_unix_time = 'None', '0', atd_unix_time) as bigint),
    atd_local_time,
    atd_bj_time,
    ata_unix_time,
    -- cast(if(ata_unix_time = 'None', '0', ata_unix_time) as bigint),
    ata_local_time,
    ata_bj_time,
    eta_unix_time,
    -- cast(if(eta_unix_time = 'None', '0', eta_unix_time) as bigint),
    eta_local_time,
    eta_bj_time,
    fligh_time
    -- cast(if(fligh_time = 'None', '0', fligh_time) as bigint)
from fr24_airport_arrival_kafka;


end;

