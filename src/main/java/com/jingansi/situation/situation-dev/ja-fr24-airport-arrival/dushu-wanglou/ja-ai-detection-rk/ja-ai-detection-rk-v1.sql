--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/6/24 14:05:32
-- description: 检测数据的单独入库程序，主要针对于无人机的检测数据-警航检测的
--********************************************************************--

set 'pipeline.name' = 'ja-ai-detection-rt';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '3';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-ai-detection-rt' ;



-- 可见光红外检测的数据（Source：kafka）
drop table if exists photoelectric_inspection_result_kafka;
create table photoelectric_inspection_result_kafka(
                                                      batch_id             bigint,
                                                      frame_num            bigint,
                                                      frame_tensor_list    string,
                                                      image_path           string, -- 大图
                                                      record_path          string, -- 告警视频地址
                                                      infer_done           bigint,
                                                      ntp_timestamp        bigint, -- 发出消息的ntp时间
                                                      pts                  bigint,
                                                      source_frame_height  bigint, -- 原视频高度
                                                      source_frame_width   bigint, -- 原视频宽度
                                                      source_id            string, -- 设备id
                                                      video_infer_done     boolean,
                                                      video_path           string,
                                                      radar_id             string, -- 雷达id
                                                      object_list array<
                                                          row<
                                                          bbox_height          bigint,
                                                      bbox_width           bigint,
                                                      bbox_left            bigint,
                                                      bbox_top             bigint,
                                                      class_id             bigint,
                                                      confidence           double,
                                                      image_path           string,  -- 目标小图
                                                      infer_id             bigint,
                                                      object_id            bigint,  -- 可见光、红外检测目标的id
                                                      object_label         string,  -- 舰船
                                                      object_sub_label     string,  -- 舰船
                                                      radar_target_id      bigint,  -- 对应的雷达检出目标id
                                                      video_time           bigint,
                                                      speed                double,  -- 速度m/s
                                                      distance             double,  -- 距离单位m
                                                      yaw                  double,  -- 目标方位
                                                      source_type          string,  -- 数据来源:VISUAL:	可见光,INFRARED:	红外,FUSHION:	融合,VIBRATOR: 震动器，这里数据是空
                                                      longitude            double,  -- 目标经度 合上就有经纬度
                                                      latitude             double,  -- 目标纬度
                                                      altitude             double,  -- 高度
                                                      obj_label_list       string,
                                                      obj_tensor_list      string,
                                                      obj_track_list       string
                                                          >
                                                          >,
                                                      proctime as PROCTIME()
) WITH (
      'connector' = 'kafka',
      'topic' = 'ja-ai-detection-output',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-ai-detection-output-group-id',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );




-- 设备（可见光、红外）检测全量数据入库（Sink：doris）
drop table  if exists dwd_photoelectric_target_all_rt;
create table dwd_photoelectric_target_all_rt(
                                                device_id                  string     , -- 设备id,
                                                target_id                  bigint     , -- 目标id
                                                parent_id                  string     , -- 父设备id
                                                acquire_timestamp_format   string     , -- 上游程序上报时间戳-时间戳格式化,
                                                acquire_timestamp          bigint     , -- 采集时间戳毫秒级别，上游程序上报时间戳,
                                                source_type                string     , -- 设备名称
                                                device_name                string     , -- 设备名称
                                                source                     string     , -- 数据检测的来源拼接 示例：雷达（11）、可见光（22）
                                                bbox_height                double     , -- 长度
                                                bbox_left	               double     , -- 左
                                                bbox_top	               double     , -- 上
                                                bbox_width	               double     , -- 宽度
                                                source_frame_height        bigint     , -- 原视频高度
                                                source_frame_width         bigint     , -- 原视频宽度
                                                big_image_path             string     , -- 大图
                                                small_image_path           string     , -- 小图
                                                class_id                   double     ,
                                                confidence                 string     , -- 置信度
                                                infer_id                   double     ,
                                                object_label               string     , -- 目标的类型，人，车
                                                object_sub_label           string     , -- 目标的类型子类型
                                                create_by                  string     , -- 创建人
                                                update_time                string      -- 数据入库时间
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dwd_photoelectric_target_all_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='10s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );




-- 设备（可见光、红外）检测目标状态数据入库（Sink：doris）
drop table  if exists dws_photoelectric_target_status_rt;
create table dws_photoelectric_target_status_rt(
                                                   device_id                  string     , -- 设备id,
                                                   target_id                  bigint     , -- 目标id
                                                   parent_id                  string     , -- 父设备id
                                                   acquire_timestamp_format   string     , -- 上游程序上报时间戳-时间戳格式化,
                                                   acquire_timestamp          bigint     , -- 采集时间戳毫秒级别，上游程序上报时间戳,
                                                   source_type                string     , -- 设备名称
                                                   device_name                string     , -- 设备名称
                                                   source                     string     , -- 数据检测的来源拼接 示例：雷达（11）、可见光（22）
                                                   bbox_height                double     , -- 长度
                                                   bbox_left	               double     , -- 左
                                                   bbox_top	               double     , -- 上
                                                   bbox_width	               double     , -- 宽度
                                                   source_frame_height        bigint     , -- 原视频高度
                                                   source_frame_width         bigint     , -- 原视频宽度
                                                   big_image_path             string     , -- 大图
                                                   small_image_path           string     , -- 小图
                                                   class_id                   double     ,
                                                   confidence                 string     , -- 置信度
                                                   infer_id                   double     ,
                                                   object_label               string     , -- 目标的类型，人，车
                                                   object_sub_label           string     , -- 目标的类型子类型
                                                   create_by                  string     , -- 创建人
                                                   update_time                string      -- 数据入库时间

)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dws_photoelectric_target_status_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='2s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );




-- 建立映射mysql的表（为了查询用户名称）
drop table if exists iot_device;
create table iot_device (
                            id	             int,    -- 自增id
                            parent_id        string, -- 父设备的id,也就是望楼id
                            device_id	     string, -- 设备id
                            device_name      string, -- 设备名称
                            gmt_create_by	 string, -- 创建用户名
                            primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'iot_device',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '3'
     );



-----------------------

-- 数据处理

-----------------------


-- 可见光红外数据关联设备表取出设备名称
drop view if exists tmp_source_kafka_001;
create view tmp_source_kafka_001 as
select
    t1.*,
    t2.device_name -- 设备名称 无人机
from photoelectric_inspection_result_kafka as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.source_id = t2.device_id
where t1.source_id is not null
  and t1.ntp_timestamp is not null;



drop view if exists tmp_source_kafka_002;
create view tmp_source_kafka_002 as
select
    t1.batch_id,
    t1.image_path as big_image_path,     -- 外层的大图
    t1.source_id as device_id,           -- 设备id
    t1.ntp_timestamp as acquire_timestamp,
    -- TO_TIMESTAMP_LTZ(t1.ntp_timestamp,3) as acquire_timestamp_format,
    from_unixtime(ntp_timestamp/1000) as acquire_timestamp_format,
    t1.source_frame_height,
    t1.source_frame_width,

    t2.image_path small_image_path,   -- 里层的小图
    t2.object_id as target_id,
    t2.source_type,
    t2.bbox_height,
    t2.bbox_left,
    t2.bbox_top,
    t2.bbox_width,
    t2.class_id,
    cast(t2.confidence as varchar) as confidence,
    t2.infer_id,
    t2.object_label,
    t2.object_sub_label,

    t1.source_id as parent_id,
    t1.device_name, -- 设备名称 无人机
    concat('[{',
           concat('"deviceName":"',t1.device_name,'",'),
           concat('"deviceId":"',t1.source_id,'",'),
           concat('"targetId":"',cast(t2.object_id as string),'",'),
           concat('"type":"',t1.device_name,'"}]')
        ) as source,
    if(t2.source_type is null or t2.source_type = '',t1.device_name,t2.source_type) as source_type

from tmp_source_kafka_001 as t1
         cross join unnest (object_list) as t2 (
                                                bbox_height          ,
                                                bbox_width           ,
                                                bbox_left            ,
                                                bbox_top             ,
                                                class_id             ,
                                                confidence           ,
                                                image_path           , -- 目标小图
                                                infer_id             ,
                                                object_id            , -- 可见光、红外检测的目标id
                                                object_label         , -- 舰船
                                                object_sub_label     , -- 舰船
                                                radar_target_id      , -- 雷达检测到的目标的目标id
                                                video_time           ,
                                                speed                , -- 速度m/s
                                                distance             , -- 距离单位m
                                                yaw                  , -- 目标方位
                                                source_type          , -- 数据来源:VISUAL:	可见光,INFRARED:	红外,FUSHION:	融合,VIBRATOR: 震动器
                                                longitude            , -- 经度
                                                latitude             , -- 纬度
                                                altitude             , -- 高度
                                                obj_label_list       ,
                                                obj_tensor_list      ,
                                                obj_track_list
    );




-- -- 可见光、红外检测数据处理
-- drop view if exists tmp_source_kafka_001;
-- create view tmp_source_kafka_001 as
-- select
--   t1.batch_id,
--   t1.image_path as big_image_path,     -- 外层的大图
--   t1.source_id as device_id,           -- 设备id
--   t1.ntp_timestamp as acquire_timestamp,
--   -- TO_TIMESTAMP_LTZ(t1.ntp_timestamp,3) as acquire_timestamp_format,
--   from_unixtime(ntp_timestamp/1000) as acquire_timestamp_format,
--   t1.source_frame_height,
--   t1.source_frame_width,
--   t2.image_path small_image_path,   -- 里层的小图
--   t2.object_id as target_id,
--   t2.source_type,
--   t2.bbox_height,
--   t2.bbox_left,
--   t2.bbox_top,
--   t2.bbox_width,
--   t2.class_id,
--   cast(t2.confidence as varchar) as confidence,
--   t2.infer_id,
--   t2.object_label,
--   t2.object_sub_label,
--   PROCTIME()  as proctime
--   -- t1.record_path,                      -- 告警视频地址
--   -- t1.radar_id,
--   -- t2.radar_target_id,
--   -- t2.longitude,
--   -- t2.latitude,
--   -- t2.altitude,
--   -- t2.speed,
--   -- t2.distance,
--   -- t2.yaw,

-- from photoelectric_inspection_result_kafka as t1
-- cross join unnest (object_list) as t2 (
-- 	bbox_height          ,
-- 	bbox_width           ,
-- 	bbox_left            ,
--     bbox_top             ,
-- 	class_id             ,
-- 	confidence           ,
-- 	image_path           , -- 目标小图
--     infer_id             ,
-- 	object_id            , -- 可见光、红外检测的目标id
--   	object_label         , -- 舰船
-- 	object_sub_label     , -- 舰船
--     radar_target_id      , -- 雷达检测到的目标的目标id
-- 	video_time           ,
--     speed                , -- 速度m/s
--     distance             , -- 距离单位m
--     yaw                  , -- 目标方位
--     source_type          , -- 数据来源:VISUAL:	可见光,INFRARED:	红外,FUSHION:	融合,VIBRATOR: 震动器
--     longitude            , -- 经度
--     latitude             , -- 纬度
--     altitude             , -- 高度
--     obj_label_list       ,
--     obj_tensor_list      ,
--     obj_track_list
-- )
--   where t1.source_id is not null  -- 光电设备ID
--     and t2.object_id is not null  -- 光电检测目标ID
--     and t1.ntp_timestamp is not null;




-- -- 可见光红外数据关联设备表取出设备名称
-- drop view if exists tmp_source_kafka_002;
-- create view tmp_source_kafka_002 as
-- select
--   t1.*,
--   t1.device_id as parent_id,
--   t2.device_name, -- 设备名称 无人机
--   concat('[{',
--       concat('"deviceName":"',t2.device_name,'",'),
--       concat('"deviceId":"',t1.device_id,'",'),
--       concat('"targetId":"',cast(t1.target_id as string),'",'),
--       concat('"type":"',t2.device_name,'"}]')
--     ) as source,
--   if(t1.source_type not in('',null),t1.source_type,t2.device_name) as source_type

-- from tmp_source_kafka_001 as t1
--   left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
--   on t1.device_id = t2.device_id;


-----------------------

-- 数据插入

-----------------------


begin statement set;


-- 设备(红外可见光)检测全量数据入库doris
insert into dwd_photoelectric_target_all_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    source_type,
    device_name,
    source,
    bbox_height,
    bbox_left,
    bbox_top,
    bbox_width,
    source_frame_height,
    source_frame_width,
    big_image_path,
    small_image_path,
    class_id,
    confidence,
    infer_id,
    object_label,
    object_sub_label,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_002;



-- 设备(红外可见光)检测状态数据入库doris
insert into dws_photoelectric_target_status_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    source_type,
    device_name,
    source,
    bbox_height,
    bbox_left,
    bbox_top,
    bbox_width,
    source_frame_height,
    source_frame_width,
    big_image_path,
    small_image_path,
    class_id,
    confidence,
    infer_id,
    object_label,
    object_sub_label,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_002;


end;





