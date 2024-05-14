
set 'pipeline.name' = 'fly-adsbexchange-real-data';

set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'table.exec.state.ttl' = '600000';
SET 'parallelism.default' = '4';


-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'state.checkpoints.dir' = 's3://ja-bice2/flink-checkpoints/fly-adsbexchange-real-data';


-- f24数据
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
                                                  nac_v                bigint     -- 速度导航精度
) with (
      'connector' = 'kafka',
      'topic' = 'adsb-exchange-aircraft-list',
      'properties.bootstrap.servers' = '47.111.155.82:30097',
      'properties.group.id' = 'adbs-exchange-ceshi1',
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
      'sink.buffer-flush.max-rows'='10000',
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
      'sink.buffer-flush.max-rows'='10000',
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
      'sink.buffer-flush.max-rows'='10000',
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
      'url' = 'jdbc:mysql://8.130.39.51:9030/global_entity?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'yshj@yshj',
      'table-name' = 'dim_prefix',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );



-- ---------------------
-- 数据处理
-- ---------------------

-- 数据晒选处理
drop view if exists tmp_table01;
create view tmp_table01 as
select
    upper(hex) as flight_id,
    from_unixtime(cast((nowtime - coalesce(seen_pos,seen,0)) as bigint),'yyyy-MM-dd HH:mm:ss') as acquire_time,
    seen_pos             ,
    REGEXP_REPLACE(flight,'[^a-zA-Z0-9\-]','') as flight_no,
    upper(hex) as icao_code,
    if(r='',cast(null as string),r) as registration,
    if(t='',cast(null as string),t) as flight_type,
    lon as lng												, -- 经度
    lat as lat												, -- 纬度
    cast(gs as double) * 1.852 as speed_km	, -- 速度单位 km/h
    cast(alt_baro as double)*0.3048 as altitude_baro_m									, -- 气压高度 海拔 单位米
    track as heading											, -- 方向  正北为0
    squawk as squawk_code									, -- 当前应答机代码
    cast(null as varchar) as airlines_icao				, -- 航空公司的icao代码
    type as ly									, -- 数据来源的系统
    PROCTIME()  as proctime
from adsb_exchange_aircraft_list_kafka
where nowtime is not null
  and hex is not null
  and hex <> '';



-- 切分注册号
drop view if exists tmp_table02;
create view tmp_table02 as
select
    *,
    if(instr(registration,'-')>0,substring(registration,1,2),concat(substring(registration,1,1),'-')) as prefix_code2,
    if(instr(registration,'-')>0,substring(registration,1,3),concat(substring(registration,1,2),'-')) as prefix_code3,
    if(instr(registration,'-')>0,substring(registration,1,4),concat(substring(registration,1,3),'-')) as prefix_code4,
    if(instr(registration,'-')>0,substring(registration,1,5),concat(substring(registration,1,4),'-')) as prefix_code5
from tmp_table01;


-- 对数据进行处理，计算国家代码
drop view if exists tmp_table03;
create view tmp_table03 as
select
    t1.*,
    t7.country_code as country_code7,
    t6.country_code as country_code6,
    t5.country_code as country_code5,
    t4.country_code as country_code4
from tmp_table02 as t1

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


-- 飞机国家计算,规整字段
drop view if exists tmp_table04;
create view tmp_table04 as
select
    flight_id                             as id,
    acquire_time                          as time1,
    2                                     as flag,
    icao_code                             as s_mode,
    registration                          as registration_number,
    flight_no                             as flight_number,
    flight_type                           as model,
    cast(null as varchar)                 as `type`,
    lng                                   as longitude02,
    lat                                   as latitude02,
    speed_km                              as speed_km,
    altitude_baro_m                       as height,
    heading                               as heading,
    cast(null as varchar)                 as fly_status,
    if(registration is null,cast(null as varchar),coalesce(country_code7,country_code6,country_code5,country_code4))  as country_flag,
    cast(null as varchar)                 as country_chinese_name,
    ly                                    as origin,
    airlines_icao                         as airline_code,
    squawk_code                           as responder_code,
    cast(null as string)                  as take_off_code,
    cast(null as double)                  as origin_longitude02,
    cast(null as double)                  as origin_latitude02,
    cast(null as string)                  as land_code,
    cast(null as double)                  as destination_longitude02,
    cast(null as double)                  as destination_latitude02,
    cast(null as string)                  as `image`,
    cast(null as string)                  as take_off_time,
    cast(null as string)                  as land_time,
    cast(null as string)                  as during,
    from_unixtime(unix_timestamp())       as gmt_create    -- 数据入库时间
from tmp_table03;


-- ---------------------
-- 数据处理
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
from tmp_table04;



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
from tmp_table04;


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
from tmp_table04;

end;