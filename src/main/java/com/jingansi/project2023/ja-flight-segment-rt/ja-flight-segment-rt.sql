--********************************************************************--
-- author:      write your name here
-- create time: 2023/10/31 17:32:26
-- description: 飞机的航班数据入库
--********************************************************************--
set 'pipeline.name' = 'ja-flight-segment-rt';


set 'parallelism.default' = '4';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '600000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-flight-segment-rt';





-- 创建kafka全量数据表（Source：kafka）
drop table if exists adsb_exchange_aircraft_list_kafka;
create table adsb_exchange_aircraft_list_kafka(
  hex                  string,     -- 飞机的 24 位 ICAO 标识符，为 6 个十六进制数字
  seen_pos             double,     -- 上次更新位置的时间（在“now”之前的秒数）
  flight               string,     -- 呼号、航班名称或飞机注册，8 个字符
  alt_geom             double,     -- 高度 参考 WGS84 椭球体的几何 (GNSS / INS) 高度（以英尺为单位）
  nic                  bigint,     -- 导航完整性类别
  emergency            double,     -- ADS-B紧急/优先状态 无、一般、救生员、minfuel、nordo、非法、击落、保留）
  lon                  double,     -- 经度
  lat                  double,     -- 纬度
  nogps                bigint,     -- 0 - 表示飞机是否具有GPS（0表示有GPS）
  type                 string,     -- 消息的类型/该位置/飞机的当前数据的最佳来源
  seen                 double,     -- 多久以前（以“现在”之前的几秒为单位）最后一次从这架飞机收到消息
  nav_qnh              double,     -- 高度计设置的高度（QFE 或 QNH/QNE）、hPa
  adsb_version         bigint,     -- ADS-B协议版本
  squawk               string,     -- A模式应答机代码，编码为4个八进制数字
  gva                  double,     -- 几何垂直精度
  nic_a                bigint,     -- 导航完整性类别
  sil                  bigint,     -- 源完整性级别
  sil_type             bigint,     -- SIL 的解释：未知、每小时、每个样本
  receiverCount        bigint,     -- 当前接收来自该飞机的数据的接收器数量
  nic_baro             bigint,     -- 气压高度的导航完整性类别
  track                double,     -- 地面上的方向角度轨迹，以度为单位（0-359）
  airground            bigint,     -- 表示飞机当前在空中还是地面上
  tisb_version         bigint,     -- tis-b 版本
  dbFlags              bigint,     -- 某些数据库标志的位字段
  alt_baro             double,     -- 飞机气压高度（以英尺为单位）作为数字或“地面”作为字符串
  geom_rate            double,     -- 几何（GNSS / INS）高度的变化率，英尺/分钟
  rssi                 double,     -- 最近平均RSSI（信号功率），以dbFS为单位；这将永远是负面的
  nav_altitude_mcp     double,     -- 从模式控制面板/飞行控制单元（MCP/FCU）或同等设备选择的高度
  gs                   double,     -- 地面速度（节）
  spi                  int,        -- 飞行状态特殊位置标识位
  version              bigint,     -- 版本：ADS-B 版本号 0、1、2（3-7 保留）
  rc                   double,     -- 限定半径，米；源自 NIC 和补充位的位置完整性度量
  sda                  bigint,     -- 系统设计保证
  r                    string,     -- 这册号
  alert1               bigint,     -- 表示是否存在警报情况（0表示无警报）
  t                    string,     -- 从数据库中提取的飞机类型
  extraFlags           bigint,     -- 附加标志
  messages             bigint,     -- 处理的 S 模式消息总数（任意）
  nowtime              double,     -- 文件生成时间
  adsr_version         bigint,     -- ADS-R协议版本
  nac_p                double,     -- 位置导航精度
  category             string,     -- 发射器类别，用于识别特定飞机或车辆类别
  nac_v                bigint,     -- 速度导航精度
  proctime             as PROCTIME()
) with (
    'connector' = 'kafka',
    'topic' = 'adsb-exchange-aircraft-list',
    'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
    'properties.group.id' = 'ja-flight-segment-rt',
    -- 'scan.startup.mode' = 'latest-offset',
    'scan.startup.mode' = 'timestamp',
    'scan.startup.timestamp-millis' = '1700546400000',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);


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
  'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
  'properties.group.id' = 'ja-flight-segment-rt',
  -- 'scan.startup.mode' = 'group-offsets',
  -- 'scan.startup.mode' = 'latest-offset',
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp-millis' = '1700546400000',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

-- flightradar24网站的飞机数据
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
    'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
    'properties.group.id' = 'ja-flightrader24-aircraft-rt',
    'scan.startup.mode' = 'timestamp',
    'scan.startup.timestamp-millis' = '1700546400000',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);


drop table if exists dws_flight_segment_rt;
create table dws_flight_segment_rt (
  flight_id               varchar(20)     comment '飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码',
  registration            varchar(20)     comment '注册号',
  flight_no               varchar(20)     comment '航班号',
  flight_trace_id         varchar(20)     comment '飞行记录号 没有的为空',
  start_time              string        comment '开始时间',
  end_time                string        comment '结束时间',
  flight_duration         int             comment '飞行时长',
  icao_code               varchar(10)     comment 'icao 24位地址码',
  flight_type             varchar(20)     comment '机型',
  is_military			   int     		  comment '是否军用飞机 0 非军用 1 军用',
  country_code            varchar(10)     comment '国家代码',
  country_name            varchar(50)     comment '国家名称',
  origin_airport3_code    VARCHAR(10)     comment '起飞机场的iata代码',
  origin_airport_e_name   VARCHAR(50)     comment '来源机场英文',
  origin_airport_c_name   VARCHAR(100)    comment '来源机场中文',
  dest_airport3_code      VARCHAR(10)     comment '目标机场的 iata 代码',
  dest_airport_e_name     VARCHAR(50)     comment '目的机场英文',
  dest_airport_c_name     VARCHAR(100)    comment '目的机场中文',
  flight_departure_time   string        comment '航班起飞时间',
  expected_landing_time   string        comment '预计降落时间',
  src_cnt				  bigint    	 comment '数据源的个数',
  update_time             string        comment '更新时间'
) with (
    'connector' = 'doris',
    'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
    'table.identifier' = 'sa.dws_flight_segment_rt',
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


drop table if exists dws_aircraft_info;
create table dws_aircraft_info (
    icao_code           string        comment '飞机的 24 位 ICAO 标识符，为 6 个十六进制数字 大写',
    registration        string        comment '地区国家三位编码',
    icao_type           string        comment '飞机的机型型码，用于标识不同类型的飞机',
    is_mil              int           comment '是否军用飞机',
    operator		    string	comment '飞机的运营者，即运营和管理飞机的实体或公司。',
    operator_c_name		string	      comment '运营者的中文名',
    operator_icao		string		  comment '运营者的 ICAO 代码，用于标识飞机的运营者。',
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

drop view if exists tmp_exchange_aircraft_list_01;
create view tmp_exchange_aircraft_list_01 as
select
  upper(hex) as flight_id, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
  coalesce(if(r='',cast(null as string),r),c.registration) as registration, -- 注册号
  flight as flight_no, -- 航班号
  cast(null as string) as flight_trace_id, -- 飞行记录号 没有的为空
  -- as start_time, -- 开始时间
  -- as end_time, -- 结束时间
  from_unixtime(cast((nowtime - coalesce(seen_pos,seen,0)) as bigint),'yyyy-MM-dd HH:mm:ss') as acquire_time,
  --  as flight_duration, -- 飞行时长
  if(left(hex,1)='~',cast(null as string),upper(hex)) as icao_code, -- icao 24位地址码
  coalesce(if(t='',cast(null as string),t),c.icao_type) as flight_type, -- 机型
  dbFlags % 2 as is_military,     		   -- 是否军用飞机 0 非军用 1 军用
  -- as country_code, -- 国家代码
  -- as country_name, -- 国家名称
  cast(null as string) as origin_airport3_code, -- 起飞机场的iata代码
  cast(null as string) as origin_airport_e_name, -- 来源机场英文
  cast(null as string) as origin_airport_c_name, -- 来源机场中文
  cast(null as string) as dest_airport3_code, -- 目标机场的 iata 代码
  cast(null as string) as dest_airport_e_name, -- 目的机场英文
  cast(null as string) as dest_airport_c_name, -- 目的机场中文
  cast(null as string) as flight_departure_time, -- 航班起飞时间
  cast(null as string) as expected_landing_time, -- 预计降落时间
  proctime
  --  as update_time, -- 更新时间
from adsb_exchange_aircraft_list_kafka a
left join dws_aircraft_info
FOR SYSTEM_TIME AS OF a.proctime as c
on upper(a.hex)=c.icao_code;

drop view if exists tmp_exchange_aircraft_list_02;
create view tmp_exchange_aircraft_list_02 as
select
  *,
  if(instr(registration,'-')>0,substring(registration,1,2),concat(substring(registration,1,1),'-')) as prefix_code2,
  if(instr(registration,'-')>0,substring(registration,1,3),concat(substring(registration,1,2),'-')) as prefix_code3,
  if(instr(registration,'-')>0,substring(registration,1,4),concat(substring(registration,1,3),'-')) as prefix_code4,
  if(instr(registration,'-')>0,substring(registration,1,5),concat(substring(registration,1,4),'-')) as prefix_code5
from tmp_exchange_aircraft_list_01;


drop view if exists tmp_exchange_aircraft_list_03;
create view tmp_exchange_aircraft_list_03 as
select
  a.*,
  coalesce(t7.country_code,t6.country_code,t5.country_code,t4.country_code) as country_code
from tmp_exchange_aircraft_list_02 a
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t4
on a.prefix_code2=t4.prefix_code
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t5
on a.prefix_code3=t5.prefix_code
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t6
on a.prefix_code4=t6.prefix_code
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t7
on a.prefix_code5=t7.prefix_code;


drop view if exists tmp_exchange_aircraft_list_04;
create view tmp_exchange_aircraft_list_04 as
select
  a.*,
  b.c_name as country_name						       -- 飞机所属的国家
from tmp_exchange_aircraft_list_03 a
left join dim_country_code_name_info FOR SYSTEM_TIME AS OF a.proctime as b
on a.country_code = b.country_code2 and 'COMMON' = b.source;


-- select * from tmp_exchange_aircraft_list_04;

drop view if exists tmp_radarbox_aircraft_list_01;
create view tmp_radarbox_aircraft_list_01 as
select
  if(sMode is null or sMode='',flightTraceId,sMode) as flight_id, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
  if(a.registration is null or a.registration in('','BLOCKED','VARIOUS','TACTICAL'),c.registration,a.registration) as registration, -- 注册号
  if(flightNo='',cast(null as string),flightNo) as flight_no, -- 航班号
  flightTraceId as flight_trace_id, -- 飞行记录号 没有的为空
  -- as start_time, -- 开始时间
  -- as end_time, -- 结束时间
  from_unixtime(cast(acquireTimestamp as bigint)/1000,'yyyy-MM-dd HH:mm:ss') as acquire_time,
  --  as flight_duration, -- 飞行时长
  if(sMode is null or sMode='',cast(null as string),sMode) as icao_code, -- icao 24位地址码
  coalesce(if(a.flightType = '' or a.flightType is null,cast(null as string),flightType),c.icao_type) as flight_type, -- 机型
  c.is_mil as is_military,     		   -- 是否军用飞机 0 非军用 1 军用
  -- as country_code, -- 国家代码
  -- as country_name, -- 国家名称
  if(originAirport3Code = '',cast(null as varchar),originAirport3Code) as origin_airport3_code, -- 起飞机场的iata代码
  -- cast(null as string) as origin_airport_e_name, -- 来源机场英文
  -- cast(null as string) as origin_airport_c_name, -- 来源机场中文
  if(destinationAirport3Code='',cast(null as varchar),destinationAirport3Code) as dest_airport3_code, -- 目标机场的 iata 代码
  -- cast(null as string) as dest_airport_e_name, -- 目的机场英文
  -- cast(null as string) as dest_airport_c_name, -- 目的机场中文
  -- cast(null as string) as flight_departure_time, -- 航班起飞时间
  -- cast(null as string) as expected_landing_time, -- 预计降落时间
  estimatedLandingDurationFormat                                                             as estimated_landing_duration_format,
  split_index(expectedLandingTime,':',0)                                                     as expected_landing_time_hour,
  split_index(expectedLandingTime,':',1)                                                     as expected_landing_time_minute,
  split_index(flightDepartureTime,':',0)                                                     as flight_departure_time_hour,
  split_index(flightDepartureTime,':',1)                                                     as flight_departure_time_minute,
  flightDepartureTime                                                                        as flight_departure_time,  -- 出发
  proctime
  --  as update_time, -- 更新时间
from radarbox_aircraft_list_kafka a
left join dws_aircraft_info
FOR SYSTEM_TIME AS OF a.proctime as c
on a.sMode=c.icao_code;


drop view if exists tmp_radarbox_aircraft_list_02;
create view tmp_radarbox_aircraft_list_02 as
select
  a.*,
  if(estimated_landing_duration_format.flag = 'm',
    cast(timestampadd(minute,cast(estimated_landing_duration_format.`minute` as int),to_timestamp(acquire_time))as string),
    cast(timestampadd(hour,cast(estimated_landing_duration_format.`hour` as int),timestampadd(minute,cast(estimated_landing_duration_format.`minute` as int),to_timestamp(acquire_time))) as string)
  ) as expected_landing_time,
  case
	when flight_departure_time_hour < expected_landing_time_hour then concat(cast(CURRENT_DATE as string),' ',flight_departure_time,':00')
	when flight_departure_time_hour = expected_landing_time_hour and flight_departure_time_minute < expected_landing_time_minute then concat(cast(CURRENT_DATE as string),' ',flight_departure_time,':00')
	when flight_departure_time_hour > expected_landing_time_hour then concat(cast(timestampadd(day,-1,CURRENT_DATE) as string),' ',flight_departure_time,':00')
	end as flight_departure_time_format,
    coalesce(b.airport,c.airport)  as origin_airport_e_name , -- 来源机场英文
  coalesce(b.airport_name,c.airport_name) as origin_airport_c_name , -- 来源机场中文
  coalesce(d.airport,f.airport) as dest_airport_e_name , -- 目的机场英文
  coalesce(d.airport_name,f.airport_name) as dest_airport_c_name , -- 目的机场中文
  if(instr(registration,'-')>0,substring(registration,1,2),concat(substring(registration,1,1),'-')) as prefix_code2,
  if(instr(registration,'-')>0,substring(registration,1,3),concat(substring(registration,1,2),'-')) as prefix_code3,
  if(instr(registration,'-')>0,substring(registration,1,4),concat(substring(registration,1,3),'-')) as prefix_code4,
  if(instr(registration,'-')>0,substring(registration,1,5),concat(substring(registration,1,4),'-')) as prefix_code5
from tmp_radarbox_aircraft_list_01 a
left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as b
on a.origin_airport3_code = b.icao
left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as c
on a.origin_airport3_code = c.iata
left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as d
on a.dest_airport3_code = d.icao
left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as f
on a.dest_airport3_code = f.iata;


drop view if exists tmp_radarbox_aircraft_list_03;
create view tmp_radarbox_aircraft_list_03 as
select
  a.*,
  coalesce(t7.country_code,t6.country_code,t5.country_code,t4.country_code) as country_code
from tmp_radarbox_aircraft_list_02 a
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t4
on a.prefix_code2=t4.prefix_code
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t5
on a.prefix_code3=t5.prefix_code
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t6
on a.prefix_code4=t6.prefix_code
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t7
on a.prefix_code5=t7.prefix_code;


drop view if exists tmp_radarbox_aircraft_list_04;
create view tmp_radarbox_aircraft_list_04 as
select
  a.*,
  b.c_name as country_name						       -- 飞机所属的国家
from tmp_radarbox_aircraft_list_03 a
left join dim_country_code_name_info FOR SYSTEM_TIME AS OF a.proctime as b
on a.country_code = b.country_code2 and 'COMMON' = b.source;


-- fr24
drop view if exists tmp_fr24_aircraft_list_01;
create view tmp_fr24_aircraft_list_01 as
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
    b.is_mil as is_military     , -- 是否军用
    proctime
from flightradar24_aircraft_list a
left join dws_aircraft_info
FOR SYSTEM_TIME AS OF a.proctime as b
on a.icao_24bit=b.icao_code;


drop table if exists tmp_fr24_aircraft_list_02;
create view tmp_fr24_aircraft_list_02 as
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
  if(instr(registration,'-')>0,substring(registration,1,2),concat(substring(registration,1,1),'-')) as prefix_code2,
  if(instr(registration,'-')>0,substring(registration,1,3),concat(substring(registration,1,2),'-')) as prefix_code3,
  if(instr(registration,'-')>0,substring(registration,1,4),concat(substring(registration,1,3),'-')) as prefix_code4,
  if(instr(registration,'-')>0,substring(registration,1,5),concat(substring(registration,1,4),'-')) as prefix_code5
from tmp_fr24_aircraft_list_01 a
left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as b
on a.origin_airport_iata = b.icao
left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as c
on a.origin_airport_iata = c.iata
left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as d
on a.destination_airport_iata = d.icao
left join dws_airport_detail_info FOR SYSTEM_TIME AS OF a.proctime as f
on a.destination_airport_iata = f.iata;


drop table if exists tmp_fr24_aircraft_list_03;
create view tmp_fr24_aircraft_list_03 as
select
  a.*,
  coalesce(t7.country_code,t6.country_code,t5.country_code,t4.country_code) as country_code
from tmp_fr24_aircraft_list_02 a
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t4
on a.prefix_code2=t4.prefix_code
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t5
on a.prefix_code3=t5.prefix_code
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t6
on a.prefix_code4=t6.prefix_code
left join dim_aircraft_country_prefix_code
FOR SYSTEM_TIME AS OF a.proctime as t7
on a.prefix_code5=t7.prefix_code;



drop view if exists tmp_fr24_aircraft_list_04;
create view tmp_fr24_aircraft_list_04 as
select
  a.*,
  b.c_name as country_name						       -- 飞机所属的国家
from tmp_fr24_aircraft_list_03 a
left join dim_country_code_name_info FOR SYSTEM_TIME AS OF a.proctime as b
on a.country_code = b.country_code2 and 'COMMON' = b.source;




drop view if exists tmp_dws_flight_segment_rt_01;
create view tmp_dws_flight_segment_rt_01 as
select
  *
from (
  select
    flight_id, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
    trim(registration) as registration, -- 注册号
    trim(flight_no) as flight_no, -- 航班号
    flight_trace_id, -- 飞行记录号 没有的为空
    -- as start_time, -- 开始时间
    -- as end_time, -- 结束时间
    acquire_time,
    --  as flight_duration, -- 飞行时长
    icao_code, -- icao 24位地址码
    flight_type, -- 机型
    is_military,     		   -- 是否军用飞机 0 非军用 1 军用
    country_code, -- 国家代码
    country_name, -- 国家名称
    origin_airport3_code, -- 起飞机场的iata代码
    origin_airport_e_name, -- 来源机场英文
    origin_airport_c_name, -- 来源机场中文
    dest_airport3_code, -- 目标机场的 iata 代码
    dest_airport_e_name, -- 目的机场英文
    dest_airport_c_name, -- 目的机场中文
    flight_departure_time, -- 航班起飞时间
    expected_landing_time, -- 预计降落时间
    2 as src_type
  from tmp_exchange_aircraft_list_04
  union all
  select
    flight_id, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
    trim(registration) as registration, -- 注册号
    trim(flight_no) as flight_no, -- 航班号
    flight_trace_id, -- 飞行记录号 没有的为空
    -- as start_time, -- 开始时间
    -- as end_time, -- 结束时间
    acquire_time,
    --  as flight_duration, -- 飞行时长
    icao_code, -- icao 24位地址码
    flight_type, -- 机型
    is_military,     		   -- 是否军用飞机 0 非军用 1 军用
    country_code, -- 国家代码
    country_name, -- 国家名称
    origin_airport3_code, -- 起飞机场的iata代码
    origin_airport_e_name, -- 来源机场英文
    origin_airport_c_name, -- 来源机场中文
    dest_airport3_code, -- 目标机场的 iata 代码
    dest_airport_e_name, -- 目的机场英文
    dest_airport_c_name, -- 目的机场中文
    flight_departure_time_format as flight_departure_time, -- 航班起飞时间
    expected_landing_time, -- 预计降落时间
    1 as src_type
  from tmp_radarbox_aircraft_list_04
  union all
  select
    if(icao_24bit is null,id,icao_24bit) as flight_id, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
    trim(registration) as registration, -- 注册号
    trim(number) as flight_no, -- 航班号
    cast(null as string) as flight_trace_id, -- 飞行记录号 没有的为空
    -- as start_time, -- 开始时间
    -- as end_time, -- 结束时间
    acquire_time,
    --  as flight_duration, -- 飞行时长
    if(char_length(icao_24bit)=6,icao_24bit,cast(null as string))  as icao_code, -- icao 24位地址码
    aircraft_code as flight_type, -- 机型
    is_military,     		   -- 是否军用飞机 0 非军用 1 军用
    country_code, -- 国家代码
    country_name, -- 国家名称
    origin_airport_iata as origin_airport3_code, -- 起飞机场的iata代码
    origin_airport_e_name, -- 来源机场英文
    origin_airport_c_name, -- 来源机场中文
    destination_airport_iata as dest_airport3_code, -- 目标机场的 iata 代码
    dest_airport_e_name, -- 目的机场英文
    dest_airport_c_name, -- 目的机场中文
    cast(null as string) as flight_departure_time, -- 航班起飞时间
    cast(null as string) as expected_landing_time, -- 预计降落时间
    3 as src_type
  from tmp_fr24_aircraft_list_04
) a;

drop view if exists tmp_dws_flight_segment_rt_02;
create view tmp_dws_flight_segment_rt_02 as
select
  flight_id, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
  registration, -- 注册号
  flight_no, -- 航班号
  min(flight_trace_id) as flight_trace_id, -- 飞行记录号 没有的为空
  min(acquire_time) as start_time, -- 开始时间
  max(acquire_time) as end_time, -- 结束时间
  max(icao_code) as icao_code, -- icao 24位地址码
  max(flight_type) as flight_type, -- 机型
  max(is_military) as is_military,     		   -- 是否军用飞机 0 非军用 1 军用
  max(country_code)  as country_code, -- 国家代码
  max(country_name) as country_name, -- 国家名称
  max(origin_airport3_code) as origin_airport3_code, -- 起飞机场的iata代码
  max(origin_airport_e_name) as origin_airport_e_name, -- 来源机场英文
  max(origin_airport_c_name) as origin_airport_c_name, -- 来源机场中文
  max(dest_airport3_code) as dest_airport3_code, -- 目标机场的 iata 代码
  max(dest_airport_e_name) as dest_airport_e_name, -- 目的机场英文
  max(dest_airport_c_name) as dest_airport_c_name, -- 目的机场中文
  max(flight_departure_time) as flight_departure_time, -- 航班起飞时间
  max(expected_landing_time) as expected_landing_time, -- 预计降落时间
  count(distinct src_type) as src_cnt
from tmp_dws_flight_segment_rt_01
group by
  flight_id, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
  registration, -- 注册号
  flight_no   -- 航班号
;


drop view if exists tmp_dws_flight_segment_rt_03;
create view tmp_dws_flight_segment_rt_03 as
select
  flight_id, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
  registration, -- 注册号
  flight_no, -- 航班号
  flight_trace_id, -- 飞行记录号 没有的为空
  min(start_time) as start_time, -- 开始时间
  max(end_time) as end_time, -- 结束时间
  -- acquire_time,
  --  as flight_duration, -- 飞行时长
  max(icao_code) as icao_code, -- icao 24位地址码
  max(flight_type) as flight_type, -- 机型
  max(is_military) as is_military,     		   -- 是否军用飞机 0 非军用 1 军用
  max(country_code)  as country_code, -- 国家代码
  max(country_name) as country_name, -- 国家名称
  max(origin_airport3_code) as origin_airport3_code, -- 起飞机场的iata代码
  max(origin_airport_e_name) as origin_airport_e_name, -- 来源机场英文
  max(origin_airport_c_name) as origin_airport_c_name, -- 来源机场中文
  max(dest_airport3_code) as dest_airport3_code, -- 目标机场的 iata 代码
  max(dest_airport_e_name) as dest_airport_e_name, -- 目的机场英文
  max(dest_airport_c_name) as dest_airport_c_name, -- 目的机场中文
  max(flight_departure_time) as flight_departure_time, -- 航班起飞时间
  max(expected_landing_time) as expected_landing_time, -- 预计降落时间
  max(src_cnt) as src_cnt
from tmp_dws_flight_segment_rt_02
group by
  flight_id, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
  registration, -- 注册号
  flight_no , -- 航班号
  flight_trace_id;


insert into dws_flight_segment_rt
select
  *
from (
  select
    flight_id, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
    registration, -- 注册号
    flight_no, -- 航班号
    flight_trace_id, -- 飞行记录号 没有的为空
    start_time, -- 开始时间
    end_time, -- 结束时间
    timestampdiff(SECOND,to_timestamp(start_time),to_timestamp(end_time))  as flight_duration, -- 飞行时长
    icao_code as icao_code, -- icao 24位地址码
    flight_type as flight_type, -- 机型
    is_military as is_military,     		   -- 是否军用飞机 0 非军用 1 军用
    country_code  as country_code, -- 国家代码
    country_name as country_name, -- 国家名称
    origin_airport3_code as origin_airport3_code, -- 起飞机场的iata代码
    origin_airport_e_name as origin_airport_e_name, -- 来源机场英文
    origin_airport_c_name as origin_airport_c_name, -- 来源机场中文
    dest_airport3_code as dest_airport3_code, -- 目标机场的 iata 代码
    dest_airport_e_name as dest_airport_e_name, -- 目的机场英文
    dest_airport_c_name as dest_airport_c_name, -- 目的机场中文
    flight_departure_time as flight_departure_time, -- 航班起飞时间
    expected_landing_time as expected_landing_time, -- 预计降落时间
    src_cnt , -- 数据源的个数
    from_unixtime(unix_timestamp()) as update_time	-- 更新时间
  from tmp_dws_flight_segment_rt_03
) a
where flight_duration>0




