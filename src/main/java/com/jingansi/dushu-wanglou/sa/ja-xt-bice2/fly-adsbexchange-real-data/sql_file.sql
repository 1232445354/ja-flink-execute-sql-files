
set 'pipeline.name' = 'fly-adsbexchange-real-data';

set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'table.exec.state.ttl' = '600000';
SET 'parallelism.default' = '4';


-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'state.checkpoints.dir' = 's3://ja-bice1/flink-checkpoints/fly-adsbexchange-real-data';


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
      'properties.bootstrap.servers' = '115.231.236.106:30090',
      'properties.group.id' = 'adbs-exchange-ceshi1',
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
                                   bs      int  , --  标识
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
    acquire_time                          as cjsj,
    2                                     as bs,
    icao_code                             as sms,
    registration                          as zch,
    flight_no                             as hbh,
    flight_type                           as xh,
    cast(null as varchar)                 as lx,
    lng                                   as jd,
    lat                                   as wd,
    speed_km                              as sd,
    altitude_baro_m                       as gd,
    heading                               as fx,
    cast(null as varchar)                 as zt,
    if(registration is null,cast(null as varchar),coalesce(country_code7,country_code6,country_code5,country_code4))  as gjdm,
    cast(null as varchar)                 as gjmc,
    ly                                    as ly,
    airlines_icao                         as hkgsdm,
    squawk_code                           as ydjdm,
    cast(null as string)                  as lyjcdm,
    cast(null as double)                  as lyjcjd,
    cast(null as double)                  as lyjcwd,
    cast(null as string)                  as mdjcdm,
    cast(null as double)                  as mdjcjd,
    cast(null as double)                  as mdjcwd,
    cast(null as string)                  as fjtp,
    cast(null as string)                  as qfsj,
    cast(null as string)                  as jlsj,
    cast(null as string)                  as yjdjzl,
    from_unixtime(unix_timestamp())       as rksj    -- 数据入库时间
from tmp_table03;


-- ---------------------
-- 数据处理
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
from tmp_table04;



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
from tmp_table04;


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
from tmp_table04;

end;