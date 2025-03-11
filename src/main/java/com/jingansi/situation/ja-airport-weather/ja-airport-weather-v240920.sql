--********************************************************************--
-- author:      write your name here
-- create time: 2024/9/18 17:20:02
-- description: 机场天气数据
--********************************************************************--
set 'pipeline.name' = 'ja-airport-weather';


SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'parallelism.default' = '3';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-airport-weather';


-- 自定义函数模型

-- 计算日出日落
create function sunrise_sunset as 'com.jingan.udf.sun.SunriseAndSunset';

-- 降雨天气转换
create function weather_trans as 'com.jingan.udf.sun.WeatherTranslator';


-- -----------------------

-- 数据结构

-- -----------------------

-- 数据来源（Source：Kafka）
drop table if exists kafka_source_info;
create table kafka_source_info(
                                  station_id                      string, -- 机场icao
                                  observation_time                string, -- 观测时间（UTC）
                                  acquire_time                    string, -- 时间
                                  temp_c                          string, -- 温度，摄氏度
                                  dewpoint_c                      string, -- 露点温度，摄氏度
                                  wind_dir_degrees                string, -- 风向（度）
                                  wind_speed_kt                   string, -- 风速（节）
                                  visibility_statute_mi           string, -- 能见度（英里）
                                  altim_in_hg                     string, -- 修正海平面气压（英寸汞柱）
                                  sea_level_pressure_mb           string, -- 海平面压力（毫巴）
                                  pressure_hpa                    bigint, -- 压强 - 百帕
                                  vert_vis_ft                     string, -- 垂直能见度（英尺）
                                  humidity_level                  double, -- 湿度级别

                                  raw_text                        string, -- 原始观测文本
                                  wind_gust_kt                    string, -- 阵风速度（节）
                                  corrected                       string, -- 校正标记
                                  auto                            string, -- 自动观测标记
                                  auto_station                    string, -- 自动气象站标记
                                  maintenance_indicator_on        string, -- 维护指示器
                                  no_signal                       string, -- 无信号指示
                                  lightning_sensor_off            string, -- 闪电传感器关闭指示
                                  freezing_rain_sensor_off        string, -- 冻雨传感器关闭指示
                                  present_weather_sensor_off      string, -- 当前天气传感器关闭指示
                                  wx_string                       string, -- 天气现象字符串
                                  sky_cover                       string, -- 天空覆盖情况
                                  cloud_base_ft_agl               string, -- 云底高度（英尺）
                                  flight_category                 string, -- 飞行类别
                                  three_hr_pressure_tendency_mb   string, -- 三小时气压变化（毫巴）
                                  maxT_c                          string, -- 最高气温（摄氏度）
                                  minT_c                          string, -- 最低气温（摄氏度）
                                  maxT24hr_c                      string, -- 过去24小时最高气温（摄氏度）
                                  minT24hr_c                      string, -- 过去24小时最低气温（摄氏度）
                                  precip_in                       string, -- 降水量（英寸）
                                  pcp3hr_in                       string, -- 过去3小时降水量（英寸）
                                  pcp6hr_in                       string, -- 过去6小时降水量（英寸）
                                  pcp24hr_in                      string, -- 过去24小时降水量（英寸）
                                  snow_in                         string, -- 雪深（英寸）
                                  metar_type                      string, -- METAR类型
                                  elevation_m                     string, -- 站点海拔（米）
                                  `key`                           string,
                                  longitude                       string, -- 经度
                                  latitude                        string, -- 维度
                                  proctime  as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'aviation_weather',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'aviation_weather_group_id',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1731442630000',
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



-- 机场天气表（Sink：doris）
drop table if exists dws_bhv_airport_weather_info;
create table dws_bhv_airport_weather_info (
                                              id      						  string    , -- 机场icao
                                              acquire_time					  string	, -- 事件时间-天气产生时间
                                              src_code					      string	, -- 数据来源网站
                                              observation_time                string     , -- 观测时间（UTC）
                                              temp_c                          string     , -- 温度，摄氏度
                                              dewpoint_c                      string     , -- 露点温度，摄氏度
                                              wind_dir_degrees                string     , -- 风向（度）
                                              wind_speed_kt                   string     , -- 风速（节）
                                              visibility_statute_mi           string     , -- 能见度（英里）
                                              altim_in_hg                     string     , -- 修正海平面气压（英寸汞柱）
                                              sea_level_pressure_mb           string     , -- 海平面压力（毫巴）
                                              pressure_hpa                    bigint 	 , -- 压强 - 百帕
                                              vert_vis_ft                     string     , -- 垂直能见度（英尺）
                                              humidity_level                  double     , -- 降雨等级
                                              sunrise                         string   , -- 日出时间
                                              sunset                          string   , -- 日落时间
                                              raw_text                        string    , -- 原始观测文本
                                              wind_gust_kt                    string    , -- 阵风速度（节）
                                              corrected                       string    , -- 校正标记
                                              auto1                           string    , -- 自动观测标记
                                              auto_station                    string    , -- 自动气象站标记
                                              maintenance_indicator_on        string    , -- 维护指示器
                                              no_signal                       string    , -- 无信号指示
                                              lightning_sensor_off            string    , -- 闪电传感器关闭指示
                                              freezing_rain_sensor_off        string    , -- 冻雨传感器关闭指示
                                              present_weather_sensor_off      string    , -- 当前天气传感器关闭指示
                                              wx_string                       string    , -- 天气现象字符串
                                              wx_string_name                  string    , -- 天气现象字符串
                                              sky_cover                       string    , -- 天空覆盖情况
                                              cloud_base_ft_agl               string    , -- 云底高度（英尺）
                                              flight_category                 string    , -- 飞行类别
                                              three_hr_pressure_tendency_mb   string    , -- 三小时气压变化（毫巴）
                                              maxT_c                          string    , -- 最高气温（摄氏度）
                                              minT_c                          string    , -- 最低气温（摄氏度）
                                              maxT24hr_c                      string    , -- 过去24小时最高气温（摄氏度）
                                              minT24hr_c                      string    , -- 过去24小时最低气温（摄氏度）
                                              precip_in                       string    , -- 降水量（英寸）
                                              pcp3hr_in                       string    , -- 过去3小时降水量（英寸）
                                              pcp6hr_in                       string    , -- 过去6小时降水量（英寸）
                                              pcp24hr_in                      string    , -- 过去24小时降水量（英寸）
                                              snow_in                         string    , -- 雪深（英寸）
                                              metar_type                      string    , -- METAR类型
                                              elevation_m                     string    , -- 站点海拔（米）
                                              `key`                           string    , -- key
                                              longitude                       string    , -- 经度
                                              latitude                        string    , -- 维度
                                              extend_info                     string    , -- 扩展字段
                                              update_time                     string     -- 入库时间

) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.244:8030',
      'table.identifier' = 'sa.dws_bhv_airport_weather_info',
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

-- 关联维表处理,计算日出日落
create view tmp_01 as
select
    station_id,
    acquire_time,
    observation_time,
    if(temp_c = '',cast(null as varchar),temp_c)                               as temp_c,
    if(dewpoint_c = '',cast(null as varchar),dewpoint_c)                       as dewpoint_c,
    if(wind_dir_degrees = '',cast(null as varchar),wind_dir_degrees)           as wind_dir_degrees,
    if(wind_speed_kt = '',cast(null as varchar),wind_speed_kt)                 as wind_speed_kt,
    if(visibility_statute_mi = '',cast(null as varchar),visibility_statute_mi) as visibility_statute_mi,
    if(altim_in_hg = '',cast(null as varchar),altim_in_hg)                     as altim_in_hg,
    if(sea_level_pressure_mb = '',cast(null as varchar),sea_level_pressure_mb) as sea_level_pressure_mb,
    pressure_hpa,
    if(vert_vis_ft = '',cast(null as varchar),vert_vis_ft)                     as vert_vis_ft,
    humidity_level,
    if(raw_text = '',cast(null as varchar),raw_text)                           as raw_text,
    if(wind_gust_kt = '',cast(null as varchar),wind_gust_kt)                   as wind_gust_kt,
    if(corrected = '',cast(null as varchar),corrected)                         as corrected,
    if(auto = '',cast(null as varchar),auto)                                   as auto1,
    if(auto_station = '',cast(null as varchar),auto_station)                   as auto_station,
    if(maintenance_indicator_on = '',cast(null as varchar),maintenance_indicator_on)     as maintenance_indicator_on,

    if(no_signal = '',cast(null as varchar),no_signal)                                   as no_signal,
    if(lightning_sensor_off = '',cast(null as varchar),lightning_sensor_off)             as lightning_sensor_off,
    if(freezing_rain_sensor_off = '',cast(null as varchar),freezing_rain_sensor_off)     as freezing_rain_sensor_off,
    if(present_weather_sensor_off = '',cast(null as varchar),present_weather_sensor_off) as present_weather_sensor_off,
    if(wx_string = '',cast(null as varchar),wx_string)                                   as wx_string,
    if(sky_cover = '',cast(null as varchar),sky_cover)                                   as sky_cover,
    if(cloud_base_ft_agl = '',cast(null as varchar),cloud_base_ft_agl)                   as cloud_base_ft_agl,
    if(flight_category = '',cast(null as varchar),flight_category)                       as flight_category,
    if(three_hr_pressure_tendency_mb = '',cast(null as varchar),three_hr_pressure_tendency_mb) as three_hr_pressure_tendency_mb,
    if(maxT_c = '',cast(null as varchar),maxT_c)                            as maxT_c,
    if(minT_c = '',cast(null as varchar),minT_c)                            as minT_c,
    if(maxT24hr_c = '',cast(null as varchar),maxT24hr_c)                    as maxT24hr_c,
    if(minT24hr_c = '',cast(null as varchar),minT24hr_c)                    as minT24hr_c,
    if(precip_in = '',cast(null as varchar),precip_in)                      as precip_in,
    if(pcp3hr_in = '',cast(null as varchar),pcp3hr_in)                      as pcp3hr_in,
    if(pcp6hr_in = '',cast(null as varchar),pcp6hr_in)                      as pcp6hr_in,
    if(pcp24hr_in = '',cast(null as varchar),pcp24hr_in)                    as pcp24hr_in,
    if(snow_in = '',cast(null as varchar),snow_in)                          as snow_in,
    if(metar_type = '',cast(null as varchar),metar_type)                    as metar_type,
    if(elevation_m = '',cast(null as varchar),elevation_m)                  as elevation_m,
    if(`key` = '',cast(null as varchar),`key`)                              as `key`,
    cast(longitude as double) as longitude,
    cast(latitude as double) as latitude,
    proctime
from kafka_source_info
where station_id is not null
  and station_id <> ''
  and acquire_time is not null;


create view tmp_02 as
select
    coalesce(t2.id,t3.id,t1.station_id) as id,
    t1.*,
    sunrise_sunset(coalesce(t2.longitude,t3.longitude,t1.longitude),coalesce(t2.latitude,t3.latitude,t1.latitude)) as sunrise_sunset,
    weather_trans(wx_string) as wx_string_name
from tmp_01 as t1
         left join dws_et_airport_info FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.station_id = t2.id

         left join dws_et_airport_info FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.station_id = t3.iata;


-- -----------------------

-- 数据入库

-- -----------------------


insert into dws_bhv_airport_weather_info
select
    id      		                  ,
    acquire_time					  ,
    '1' as src_code				  ,
    observation_time                ,
    temp_c                          ,
    dewpoint_c                      ,
    wind_dir_degrees                ,
    wind_speed_kt                   ,
    visibility_statute_mi           ,
    altim_in_hg                     ,
    sea_level_pressure_mb           ,
    pressure_hpa                    ,
    vert_vis_ft                     ,
    humidity_level                  ,
    split_index(sunrise_sunset,'=',0) as sunrise,
    split_index(sunrise_sunset,'=',1) as sunset,
    raw_text                        ,
    wind_gust_kt                    ,
    corrected                       ,
    auto1                            ,
    auto_station                    ,
    maintenance_indicator_on        ,
    no_signal                       ,
    lightning_sensor_off            ,
    freezing_rain_sensor_off        ,
    present_weather_sensor_off      ,
    wx_string                       ,
    wx_string_name                  ,
    sky_cover                       ,
    cloud_base_ft_agl               ,
    flight_category                 ,
    three_hr_pressure_tendency_mb   ,
    maxT_c                          ,
    minT_c                          ,
    maxT24hr_c                      ,
    minT24hr_c                      ,
    precip_in                       ,
    pcp3hr_in                       ,
    pcp6hr_in                       ,
    pcp24hr_in                      ,
    snow_in                         ,
    metar_type                      ,
    elevation_m                     ,
    `key`                           ,
    cast(longitude as varchar) as longitude    ,
    cast(latitude as varchar) as latitude      ,
    cast(null as varchar)         as extend_info ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_02;





