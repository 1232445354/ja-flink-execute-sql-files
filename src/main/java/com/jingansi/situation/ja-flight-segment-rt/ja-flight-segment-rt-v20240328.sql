--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2023/10/31 17:32:26
-- description: 飞机航班计算
--********************************************************************--

set 'pipeline.name' = 'ja-flight-segment-rt';


set 'parallelism.default' = '4';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '600000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '600000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-flight-segment-rt';



-- 来源kafka的源数据
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
                                flightStatus               string, -- 飞机状态
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
      'properties.group.id' = 'ja_flight_segment_idc1',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


drop table if exists dws_flight_segment_rt;
create table dws_flight_segment_rt (
                                       flight_id               string    comment '飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码',
                                       registration            string    comment '注册号',
                                       flight_no               string    comment '航班号',
                                       flight_trace_id         string    comment '飞行记录号 没有的为空',
                                       start_time              string    comment '开始时间',
                                       end_time                string    comment '结束时间',
                                       flight_duration         int       comment '飞行时长',
                                       icao_code               string    comment 'icao 24位地址码',
                                       flight_type             string    comment '机型',
                                       is_military			  bigint    comment '是否军用飞机 0 非军用 1 军用',
                                       country_code            string    comment '国家代码',
                                       country_name            string    comment '国家名称',
                                       origin_airport3_code    string    comment '起飞机场的iata代码',
                                       origin_airport_e_name   string    comment '来源机场英文',
                                       origin_airport_c_name   string    comment '来源机场中文',
                                       dest_airport3_code      string    comment '目标机场的 iata 代码',
                                       dest_airport_e_name     string    comment '目的机场英文',
                                       dest_airport_c_name     string    comment '目的机场中文',
                                       flight_departure_time   string    comment '航班起飞时间',
                                       expected_landing_time   string    comment '预计降落时间',
                                       src_cnt				  bigint    comment '数据源的个数',
                                       update_time             string    comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030,172.21.30.244:8030,172.21.30.246:8030',
      'table.identifier' = 'sa.dws_flight_segment_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='50000',
      'sink.batch.interval'='20s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



drop view if exists tmp_flight_segment_01;
create view tmp_flight_segment_01 as
select
    id                                                           as flight_id,
    srcCode                                                      as src_code,
    trim(registration)                                           as registration,
    trim(flightNo)                                               as flight_no,
    if(srcCode = 1,srcPk,cast(null as string))                   as src_pk,
    acquireTime                                                  as acquire_time,
    if(char_length(icaoCode)=6,icaoCode,cast(null as string))    as icao_code, -- icao 24位地址码
    flightType                                                   as flight_type,
    isMilitary                                                   as is_military,
    countryCode                                                  as country_code,
    countryName                                                  as country_name,
    originAirport3Code                                           as origin_airport3_code, -- 起飞机场的iata代码
    originAirportEName                                           as origin_airport_e_name, -- 来源机场英文
    originAirportCName                                           as origin_airport_c_name, -- 来源机场中文
    destAirport3Code                                             as dest_airport3_code, -- 目标机场的 iata 代码
    destAirportEName                                             as dest_airport_e_name, -- 目的机场英文
    destAirportCName                                             as dest_airport_c_name, -- 目的机场中文
    flightDepartureTime                                          as flight_departure_time, -- 航班起飞时间
    expectedLandingTime                                          as expected_landing_time -- 预计降落时间
from aircraft_source;



drop view if exists tmp_flight_segment_02;
create view tmp_flight_segment_02 as
select
    flight_id,
    registration,
    flight_no,
    min(src_pk)                 as flight_trace_id,
    min(acquire_time)           as start_time,
    max(acquire_time)           as end_time,
    max(icao_code)              as icao_code,
    max(flight_type)            as flight_type,
    max(is_military)            as is_military,
    max(country_code)           as country_code,
    max(country_name)           as country_name,
    max(origin_airport3_code)   as origin_airport3_code,
    max(origin_airport_e_name)  as origin_airport_e_name,
    max(origin_airport_c_name)  as origin_airport_c_name,
    max(dest_airport3_code)     as dest_airport3_code,
    max(dest_airport_e_name)    as dest_airport_e_name,
    max(dest_airport_c_name)    as dest_airport_c_name,
    max(flight_departure_time)  as flight_departure_time,
    max(expected_landing_time)  as expected_landing_time,
    count(distinct src_code)    as src_cnt
from tmp_flight_segment_01
group by
    flight_id,
    registration,
    flight_no;


drop view if exists tmp_flight_segment_03;
create view tmp_flight_segment_03 as
select
    flight_id,
    registration,
    flight_no,
    flight_trace_id,
    min(start_time)             as start_time,
    max(end_time)               as end_time,
    max(icao_code)              as icao_code,
    max(flight_type)            as flight_type,
    max(is_military)            as is_military,
    max(country_code)           as country_code,
    max(country_name)           as country_name,
    max(origin_airport3_code)   as origin_airport3_code,
    max(origin_airport_e_name)  as origin_airport_e_name,
    max(origin_airport_c_name)  as origin_airport_c_name,
    max(dest_airport3_code)     as dest_airport3_code,
    max(dest_airport_e_name)    as dest_airport_e_name,
    max(dest_airport_c_name)    as dest_airport_c_name,
    max(flight_departure_time)  as flight_departure_time,
    max(expected_landing_time)  as expected_landing_time,
    max(src_cnt)                as src_cnt
from tmp_flight_segment_02
group by
    flight_id,
    registration,
    flight_no ,
    flight_trace_id;



begin statement set;


insert into dws_flight_segment_rt
select
    *
from (
         select
             flight_id,
             registration,
             flight_no,
             flight_trace_id,
             start_time,
             end_time,
             timestampdiff(SECOND,to_timestamp(start_time),to_timestamp(end_time))  as flight_duration, -- 飞行时长
             icao_code                                          as icao_code, -- icao 24位地址码
             flight_type                                        as flight_type, -- 机型
             is_military                                        as is_military,     		   -- 是否军用飞机 0 非军用 1 军用
             country_code                                       as country_code, -- 国家代码
             country_name                                       as country_name, -- 国家名称
             origin_airport3_code                               as origin_airport3_code, -- 起飞机场的iata代码
             origin_airport_e_name                              as origin_airport_e_name, -- 来源机场英文
             origin_airport_c_name                              as origin_airport_c_name, -- 来源机场中文
             dest_airport3_code                                 as dest_airport3_code, -- 目标机场的 iata 代码
             dest_airport_e_name                                as dest_airport_e_name, -- 目的机场英文
             dest_airport_c_name                                as dest_airport_c_name, -- 目的机场中文
             flight_departure_time                              as flight_departure_time, -- 航班起飞时间
             expected_landing_time                              as expected_landing_time, -- 预计降落时间
             src_cnt , -- 数据源的个数
             from_unixtime(unix_timestamp())                    as update_time	-- 更新时间
         from tmp_flight_segment_03
     ) a
where flight_duration>0;


end;

