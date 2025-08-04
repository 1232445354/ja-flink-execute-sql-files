--********************************************************************--
-- author:     yibo@jingan-inc.com
-- create time: 2023/12/22 15:57:32
-- description: 态势无人机的轨迹
--********************************************************************--


set 'pipeline.name' = 'ja-malan-sa-rt';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';



-- SET 'parallelism.default' = '6';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-malan-sa-rt' ;


-- 自定义函数，转换经纬度
create function distance_udf as 'com.jingan.udf.geohash.DistanceUdf';

-----------------------

-- 数据结构

-----------------------

-- 设备轨迹数据（Source：kafka）
drop table if exists iot_device_message_kafka;
create table iot_device_message_kafka (
                                          productKey    string,     -- 产品编码
                                          deviceId      string,     -- 设备id
                                          type          string,     -- 设备检测数据-类型
                                          message  row(
                                              tid                     string    , -- 机库子设备-无人机/设备检测目标数据-当前请求的事务唯一ID
                                              bid                     string    , -- 机库子设备-无人机/设备检测目标数据-长连接整个业务的ID
                                              version                 string    , -- 设备检测数据-版本
                                              `timestamp`             bigint    , -- 机库子设备-无人机/设备检测目标数据-时间戳
                                              `method`                string    , -- 机库子设备-无人机/设备检测目标数据-服务&事件标识
                                              `data` row(
                                              aircraftModel                     string,   -- 飞机型号
                                              hasEO                             string,   -- 是否携带EO传感器
                                              aircraftNumber                    int,      -- 飞机编号
                                              routeNumber                       int,      -- 当前航线号
                                              targetRouteNumber                 int,      -- 目标航线号
                                              targetWayPointNumber              int,      -- 目标航点号
                                              longitude                         double,   -- 经度
                                              latitude                          double,   -- 纬度
                                              yaw                               double,   -- 方位角
                                              pitch                             double,   -- 俯仰角
                                              altitude                          double,   -- 海拔高度
                                              eastboundVelocity                 double,   -- 东向速度
                                              northboundVelocity                double,   -- 北向速度
                                              celestialVelocity                 double,   -- 天向速度
                                              currentLinkUsage                  string,   -- 当前使用链路
                                              recorderDownloadStatus            string,   -- 记录器传输状态
                                              recorderDownloadType              string,   -- 下传信息类型
                                              recordStatus                      string,   -- 记录状态
                                              recordDownloadBandwidth           string,   -- 实时传输带宽选择
                                              recordDownloadCompressionRatio    string,   -- 控制传输压缩比
                                              ccdStatus                         string,   -- CCD工作状态
                                              ccdTakePhotoMode                  string,   -- CCD拍照模式
                                              eoMode                            string,   -- EO-传感器选择回报
                                              eoYaw                             double,   -- EO-方位角
                                              eoPitch                           double,   -- EO-俯仰角
                                              eoFov                             int,      -- EO-激光测距值
                                              eoLaserDistance                   int,      -- EO-传感器视场
                                              eoTrackingMethod                  string,   -- EO-跟踪方式回报
                                              eoLaserStatus                     string,   -- EO-激光器状态
                                              eoInfraredPowerStatus             string,   -- EO-红外上电状态
                                              eoLaserPowerStatus                string,   -- EO-激光上电状态
                                              targetLongitude                   double,   -- 目标经度
                                              targetLatitude                    double,   -- 目标纬度
                                              targetAltitude                    int,      -- 目标海拔高度
                                              targetRange                       int,      -- 目标距离
                                              targetYaw                         double,   -- 目标方位
                                              targetPitch                       double,   -- 目标俯仰
                                              sarStatus                         string,   -- 雷达工作状态(SAR)
                                              sarMode                           string,   -- SAR工作模式
                                              sarResolution                     string,   -- SAR分辨率
                                              sarSystemMode                     string,   -- SAR系统模式
                                              sarEffectDistance                 int       -- SAR作用距离
                                              )
                                              ),
                                          rowtime as to_timestamp_ltz(message.`timestamp`,3),
                                          watermark for rowtime as rowtime - interval '3' second
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'iot-device-message-rt1',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 设备检测目标全量数据入库（Sink：doris）
create table dwd_device_all_rt(
                                  device_id                         string    comment '设备id',
                                  acquire_timestamp_format          string    comment '采集时间戳格式化',
                                  acquire_timestamp                 bigint    comment '采集时间戳',
                                  product_key                       string    comment '产品编码',
                                  type                              string    comment '类型',
                                  tid                               string    comment '当前请求的事务唯一ID',
                                  bid                               string    comment '长连接整个业务的ID',
                                  `method`                          string    comment '服务&事件标识',
                                  version                           string    comment '版本',
                                  aircraft_model                    string    comment '飞机型号',
                                  has_eO                            string    comment '是否携带EO传感器',
                                  aircraft_number                   int       comment '飞机编号',
                                  route_number                      int       comment '当前航线号',
                                  target_route_number               int       comment '目标航线号',
                                  target_way_point_number           int       comment '目标航点号',
                                  longitude                         double    comment '经度',
                                  latitude                          double    comment '纬度',
                                  yaw                               double    comment '方位角',
                                  pitch                             double    comment '俯仰角',
                                  altitude                          double    comment '海拔高度',
                                  eastbound_velocity                double    comment '东向速度',
                                  northbound_velocity               double    comment '北向速度',
                                  celestial_velocity                double    comment '天向速度',
                                  current_link_usage                string    comment '当前使用链路',
                                  recorder_download_status          string    comment '记录器传输状态',
                                  recorder_download_type            string    comment '下传信息类型',
                                  record_status                     string    comment '记录状态',
                                  record_download_bandwidth         string    comment '实时传输带宽选择',
                                  record_download_compression_ratio string    comment '控制传输压缩比',
                                  ccd_status                        string    comment 'CCD工作状态',
                                  ccd_take_photo_mode               string    comment 'CCD拍照模式',
                                  eo_mode                           string    comment 'EO-传感器选择回报',
                                  eo_yaw                            double    comment 'EO-方位角',
                                  eo_pitch                          double    comment 'EO-俯仰角',
                                  eo_fov                            int       comment 'EO-激光测距值',
                                  eo_laser_distance                 int       comment 'EO-传感器视场',
                                  eo_tracking_method                string    comment 'EO-跟踪方式回报',
                                  eo_laser_status                   string    comment 'EO-激光器状态',
                                  eo_infrared_power_status          string    comment 'EO-红外上电状态',
                                  eo_laser_power_status             string    comment 'EO-激光上电状态',
                                  target_longitude                  double    comment '目标经度',
                                  target_latitude                   double    comment '目标纬度',
                                  target_altitude                   int       comment '目标海拔高度',
                                  target_range                      int       comment '目标距离',
                                  target_yaw                        double    comment '目标方位',
                                  target_pitch                      double    comment '目标俯仰',
                                  sar_status                        string    comment '雷达工作状态(SAR)',
                                  sar_mode                          string    comment 'SAR工作模式',
                                  sar_resolution                    string    comment 'SAR分辨率',
                                  sar_system_mode                   string    comment 'SAR系统模式',
                                  sar_effect_distance               int       comment 'SAR作用距离',
                                  update_time        				  string    comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_sa_malan.dwd_device_all_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='10s'
-- 'sink.properties.escape_delimiters' = 'true',
-- 'sink.properties.column_separator' = '\x01',	 -- 列分隔符
-- 'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
-- 'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-----------------------

-- 数据处理

-----------------------

-- kafka来源的所有数据解析
create view tmp_source_kafka_01 as
select
    productKey           as product_key,
    deviceId             as device_id,
    type                 as type,
    message.tid          as tid,
    message.bid          as bid,
    message.version      as version,
    message.`timestamp`  as acquire_timestamp,
    message.`method`     as `method`,
    message.`data`       as message_data,
    lag(message.`data`.longitude) over(partition by deviceId order by rowtime) as pre_longitude,
        lag(message.`data`.latitude) over(partition by deviceId order by rowtime) as pre_latitude,
        lag(message.`timestamp`) over(partition by deviceId order by rowtime) as pre_timestamp
from iot_device_message_kafka
where message.`data`.longitude is not null
  and message.`data`.latitude is not null
  and message.`timestamp` is not null;



-- 设备(雷达)检测数据筛选处理
drop table if exists tmp_source_kafka_02;
create view tmp_source_kafka_02 as
select
    product_key,
    device_id,
    type,
    tid,
    bid,
    version,
    acquire_timestamp,
    `method`,
    message_data.aircraftModel                   as aircraft_model,
    message_data.hasEO                           as has_eO,
    message_data.aircraftNumber                  as aircraft_number,
    message_data.routeNumber                     as route_number,
    message_data.targetRouteNumber               as target_route_number,
    message_data.targetWayPointNumber            as target_way_point_number,
    message_data.longitude                       as longitude,
    message_data.latitude                        as latitude,
    message_data.yaw                             as yaw,
    message_data.pitch                           as pitch,
    message_data.altitude                        as altitude,
    message_data.eastboundVelocity               as eastbound_velocity,
    message_data.northboundVelocity              as northbound_velocity,
    message_data.celestialVelocity               as celestial_velocity,
    message_data.currentLinkUsage                as current_link_usage,
    message_data.recorderDownloadStatus          as recorder_download_status,
    message_data.recorderDownloadType            as recorder_download_type,
    message_data.recordStatus                    as record_status,
    message_data.recordDownloadBandwidth         as record_download_bandwidth,
    message_data.recordDownloadCompressionRatio  as record_download_compression_ratio,
    message_data.ccdStatus                       as ccd_status,
    message_data.ccdTakePhotoMode                as ccd_take_photo_mode,
    message_data.eoMode                          as eo_mode,
    message_data.eoYaw                           as eo_yaw,
    message_data.eoPitch                         as eo_pitch,
    message_data.eoFov                           as eo_fov,
    message_data.eoLaserDistance                 as eo_laser_distance,
    message_data.eoTrackingMethod                as eo_tracking_method,
    message_data.eoLaserStatus                   as eo_laser_status,
    message_data.eoInfraredPowerStatus           as eo_infrared_power_status,
    message_data.eoLaserPowerStatus              as eo_laser_power_status,
    message_data.targetLongitude                 as target_longitude,
    message_data.targetLatitude                  as target_latitude,
    message_data.targetAltitude                  as target_altitude,
    message_data.targetRange                     as target_range,
    message_data.targetYaw                       as target_yaw,
    message_data.targetPitch                     as target_pitch,
    message_data.sarStatus                       as sar_status,
    message_data.sarMode                         as sar_mode,
    message_data.sarResolution                   as sar_resolution,
    message_data.sarSystemMode                   as sar_system_mode,
    message_data.sarEffectDistance               as sar_effect_distance,
    -- distance_udf(message_data.latitude,message_data.longitude,pre_latitude,pre_longitude)/((acquire_timestamp - pre_timestamp)/1000/3600) as speed -- 速度 km/h

    if(abs((acquire_timestamp - pre_timestamp)/1000/3600) = 0,
       distance_udf(message_data.latitude,message_data.longitude,pre_latitude,pre_longitude),
       distance_udf(message_data.latitude,message_data.longitude,pre_latitude,pre_longitude)/abs((acquire_timestamp - pre_timestamp)/1000/3600)
        ) as speed   -- 速度 km/h,不能除0异常

from tmp_source_kafka_01;

-- and product_key = 'jk0L2mCujQu';



-----------------------

-- 数据插入

-----------------------


begin statement set;

-- 轨迹数据插入数据库
insert into dwd_device_all_rt
select
    device_id                         ,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    acquire_timestamp                 ,
    product_key                       ,
    type                              ,
    tid                               ,
    bid                               ,
    `method`                          ,
    version                           ,
    aircraft_model                    ,
    has_eO                            ,
    aircraft_number                   ,
    route_number                      ,
    target_route_number               ,
    target_way_point_number           ,
    longitude                         ,
    latitude                          ,
    yaw                               ,
    pitch                             ,
    altitude                          ,
    eastbound_velocity                ,
    northbound_velocity               ,
    celestial_velocity                ,
    current_link_usage                ,
    recorder_download_status          ,
    recorder_download_type            ,
    record_status                     ,
    record_download_bandwidth         ,
    record_download_compression_ratio ,
    ccd_status                        ,
    ccd_take_photo_mode               ,
    eo_mode                           ,
    eo_yaw                            ,
    eo_pitch                          ,
    eo_fov                            ,
    eo_laser_distance                 ,
    eo_tracking_method                ,
    eo_laser_status                   ,
    eo_infrared_power_status          ,
    eo_laser_power_status             ,
    target_longitude                  ,
    target_latitude                   ,
    target_altitude                   ,
    target_range                      ,
    target_yaw                        ,
    target_pitch                      ,
    sar_status                        ,
    sar_mode                          ,
    sar_resolution                    ,
    sar_system_mode                   ,
    sar_effect_distance               ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_02
where speed < 500;

end;



