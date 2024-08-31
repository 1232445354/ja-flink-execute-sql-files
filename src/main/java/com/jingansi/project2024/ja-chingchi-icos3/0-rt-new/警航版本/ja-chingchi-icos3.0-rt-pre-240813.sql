--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/04/16 14:06:19
-- description: 旌旗3.0设备接入日志、设备检测数据状态、轨迹等
-- version: 3.0.2.240419
-- fix:新增望楼3.0
--********************************************************************--


set 'pipeline.name' = 'ja-chingchi-icos3.0-rt-pre';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '6';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-chingchi-icos3.0-rt-pre' ;


-- 设备检测数据上报（Source：kafka）
drop table if exists iot_device_message_kafka;
create table iot_device_message_kafka (
                                          productKey    string     comment '产品编码',
                                          deviceId      string     comment '设备id',
                                          type          string     comment '类型',
                                          version       string     comment '版本',
                                          `timestamp`   bigint     comment '时间戳毫秒',
                                          tid           string     comment '当前请求的事务唯一ID',
                                          bid           string     comment '长连接整个业务的ID',
                                          `method`      string     comment '服务&事件标识',

    -- 截图数据
                                          `data`  row(
                                              pictureUrl       string   , -- 拍照数据上报-图片url
                                              width            int      , -- 拍照数据上报-宽度
                                              height           int        -- 拍照数据上报-高度
                                              ),

                                          message  row(
                                              tid                     string    , -- 当前请求的事务唯一ID
                                              bid                     string    , -- 长连接整个业务的ID
                                              version                 string    , -- 版本
                                              `timestamp`             bigint    , -- 时间戳
                                              `method`                string    , -- 服务&事件标识
                                              productKey              string    , -- 产品编码
                                              deviceId                string    , -- 设备编码
                                              `data` row(

                                              -- 拍照数据
                                              photoUrl            string    , -- 媒体上报的拍照图片url
                                              longitude           double    , -- 机库子设备-无人机,经度
                                              latitude            double    , -- 机库子设备-无人机，纬度
                                              height              double    , -- 机库子设备-无人机，高度
                                              isCapture           bigint    , -- 是否截图

                                              deviceName         string,  -- 执法仪名称

                                              -- 雷达检测数据
                                              targets array<
                                              row(
                                              targetId         bigint   ,-- -目标id
                                              xDistance        double   ,-- x距离
                                              yDistance        double   ,-- y距离
                                              speed            double   ,-- 速度
                                              targetPitch      double   ,-- 俯仰角
                                              targetAltitude   double   ,-- 目标海拔高度
                                              status           string   ,-- 0 目标跟踪 1 目标丢失 2 跟踪终止
                                              targetLongitude  string   ,-- 目标经度
                                              targetLatitude   string   , -- 目标纬度
                                              distance         double   , -- 上报距离
                                              targetYaw        double   , -- 水平角度
                                              utc_time         bigint   , -- 雷达检测数据上报时间戳
                                              tracked_times    double   , -- 已跟踪次数
                                              loss_times       double     -- 连续丢失次数
                                              )
                                              >

                                              )
                                              )
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      -- 'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.bootstrap.servers' = '135.100.11.103:9092',
      'properties.group.id' = 'iot-device-message-rt-5',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1711537635000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



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
                                                      source_type          string,  -- 数据来源:VISUAL:	可见光,INFRARED:	红外,FUSHION:	融合,VIBRATOR: 震动器
                                                      longitude            double,  -- 目标经度 合上就有经纬度
                                                      latitude             double,  -- 目标纬度
                                                      altitude             double,  -- 高度
                                                      obj_label_list       string,
                                                      obj_tensor_list      string,
                                                      obj_track_list       string
                                                          >
                                                          >
) WITH (
      'connector' = 'kafka',
      'topic' = 'ja-ai-detection-output',
      'properties.bootstrap.servers' = '135.100.11.103:9092',
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
drop table  if exists dwd_photoelectric_target_all_rt_pre;
create table dwd_photoelectric_target_all_rt_pre(
                                                    device_id                  string     , -- 设备id,
                                                    target_id                  bigint     , -- 目标id
                                                    parent_id                  string     , -- 父设备id
                                                    acquire_timestamp_format   string     , -- 上游程序上报时间戳-时间戳格式化,
                                                    acquire_timestamp          bigint     , -- 采集时间戳毫秒级别，上游程序上报时间戳,
                                                    source_type                string     , -- 设备来源
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
     'table.identifier' = 'dushu.dwd_photoelectric_target_all_rt_pre',
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
drop table  if exists dws_photoelectric_target_status_rt_pre;
create table dws_photoelectric_target_status_rt_pre(
                                                       device_id                  string     , -- 设备id,
                                                       target_id                  bigint     , -- 目标id
                                                       parent_id                  string     , -- 父设备id
                                                       acquire_timestamp_format   string     , -- 上游程序上报时间戳-时间戳格式化,
                                                       acquire_timestamp          bigint     , -- 采集时间戳毫秒级别，上游程序上报时间戳,
                                                       source_type                string     , -- 设备来源
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
     'table.identifier' = 'dushu.dws_photoelectric_target_status_rt_pre',
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





-- 媒体拍照数据入库（Sink：mysql）
drop table if exists device_media_datasource;
create table device_media_datasource (
                                         device_id                      string        comment '设备编码',
                                         source_id                      string        comment '来源id/应用id',
                                         source_name                    string        comment '来源名称/应用名称',
                                         type                           string        comment 'PICTURE/HISTORY_VIDEO',
                                         start_time                     string        comment '开始时间',
                                         end_time                       string        comment '结束时间',
                                         url                            string        comment '原图/视频 url',
                                         width                          int           comment '图片宽度',
                                         height                         int           comment '图片高度',
                                         bid                            string        comment '业务id',
                                         tid                            string        comment '',
                                         b_type                         string        comment '业务类型',
                                         extends                        string        comment 'json字符串 {}',
                                         gmt_create                     string        comment '',
                                         gmt_create_by                  string        comment '',
                                         gmt_modified_by                string        comment '',
                                         PRIMARY KEY (device_id,start_time,url) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://135.100.11.103:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8', -- ECS环境
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device_media_datasource',
      'sink.buffer-flush.interval' = '1s',
      'sink.buffer-flush.max-rows' = '20'
      );



-- 设备执法仪、无人机轨迹数据（Sink：doris）
drop table if exists dwd_device_track_rt;
create table dwd_device_track_rt (
                                     device_id                 string              comment '上报的设备id',
                                     acquire_timestamp_format  string              comment '格式化时间',
                                     acquire_timestamp         bigint              comment '上报时间戳',
                                     lng_02                    DECIMAL(30,18)      comment '经度—高德坐标系、火星坐标系',
                                     lat_02                    DECIMAL(30,18)      comment '纬度—高德坐标系、火星坐标系',
                                     username                  string              comment '设备用户',
                                     group_id                  string              comment '组织id',
                                     product_key               string              comment '产品key',
                                     tid                       string              comment 'tid',
                                     bid                       string              comment 'bid',
                                     device_name               string              comment '设备名称',
                                     create_by                 string              comment '创建人',
                                     update_time               string              comment '更新插入时间（数据入库时间'
    -- device_type               string      comment '上报的设备类型',
    -- lng_84                    string      comment '经度—84坐标系',
    -- lat_84                    string      comment '纬度—84坐标系',
    -- rectify_lng_lat           string      comment '高德坐标算法纠偏后经纬度',
    -- start_time                string      comment '开始时间（开窗）',
    -- end_time                  string      comment '结束时间（开窗）',
    -- type                      string      comment 'POSITION 位置',
    -- location_type             string      comment '位置类型',
    -- record_id                 string      comment '行为id',
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dwd_device_track_rt_pre',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='5s'
     );



-- 建立映射mysql的表（为了查询用户名称）
drop table if exists iot_device;
create table iot_device (
                            id	             int,
                            parent_id        string, -- 父设备的id,也就是望楼id
                            device_id	     string, -- 设备id
                            device_name      string, -- 设备名称
                            gmt_create_by	 string,
                            primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     'url' = 'jdbc:mysql://135.100.11.103:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'iot_device',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '3'
     );



-- 建立映射mysql的表（为了查询组织id）
drop table if exists users;
create table users (
                       user_id	int,
                       username	string,
                       password	string,
                       name	    string,
                       group_id	string,
                       primary key (user_id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     'url' = 'jdbc:mysql://135.100.11.103:3306/ja-4a?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'users',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '3'
     );



-----------------------

-- 数据处理

-----------------------

-- kafka来源的所有数据解析
drop table if exists tmp_source_kafka_01;
create view tmp_source_kafka_01 as
select
    productKey  as product_key,
    deviceId    as device_id,
    version     as version,
    `timestamp` as acquire_timestamp,
    type,
    tid,
    bid,
    `method`,
    -- 共有的
    message.tid as message_tid,
    message.bid as message_bid,
    message.version as message_version,
    message.`timestamp` as message_acquire_timestamp,
    message.productKey as message_product_key,
    message.deviceId as message_device_id,
    message.`method` as message_method,

    -- 截图
    `data`.pictureUrl as picture_url,
    `data`.width  as width,
    `data`.height as height,

    -- 雷达
    message.`data`.targets     as targets,

    -- 轨迹和操作日志的
    message.`data`.longitude   as longitude,
    message.`data`.latitude    as latitude,
    message.`data`.height      as height,

    -- 拍照
    message.`data`.deviceName                 as device_name,
    message.`data`.photoUrl                   as photo_url,
    message.`data`.isCapture                  as is_capture

from iot_device_message_kafka;



-- 执法仪、无人机轨迹数据处理
drop table if exists tmp_source_kafka_05;
create view tmp_source_kafka_05 as
select
    t1.device_id,
    t1.product_key,
    t1.type,
    t1.message_tid as tid,
    t1.message_bid as bid,
    t1.message_acquire_timestamp as acquire_timestamp,
    t1.device_name,
    t1.longitude,
    t1.latitude,
    t2.gmt_create_by as username,
    t3.group_id
from (
         select
             device_id,
             product_key,
             type,
             message_tid,
             message_bid,
             message_acquire_timestamp,
             device_name,
             longitude,
             latitude,
             PROCTIME()  as proctime
         from tmp_source_kafka_01
         where product_key in('QptZJHOd1KD','zyrFih3kept','00000000002','xmYA9WCWCtk')  -- QptZJHOd1KD :执法仪，zyrFih3kept、00000000002：无人机
           and device_id is not null
           and message_acquire_timestamp is not null
     ) as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.device_id = t2.device_id
         left join users FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t2.gmt_create_by = t3.username;




-- 可见光、红外检测数据处理
drop view if exists tmp_source_kafka_001;
create view tmp_source_kafka_001 as
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

    t2.bbox_height,
    t2.bbox_left,
    t2.bbox_top,
    t2.bbox_width,
    t2.class_id,
    cast(t2.confidence as varchar) as confidence,
    t2.infer_id,
    t2.object_label,
    t2.object_sub_label,
    t2.source_type,
    PROCTIME()  as proctime
    -- t1.record_path,                      -- 告警视频地址
    -- t1.radar_id,
    -- t2.radar_target_id,
    -- t2.longitude,
    -- t2.latitude,
    -- t2.altitude,
    -- t2.speed,
    -- t2.distance,
    -- t2.yaw,

from photoelectric_inspection_result_kafka as t1
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
    )
where t1.source_id is not null  -- 光电设备ID
  and t2.object_id is not null  -- 光电检测目标ID
  and t1.ntp_timestamp is not null;


-- 可见光红外数据关联设备表取出设备名称
drop view if exists tmp_source_kafka_002;
create view tmp_source_kafka_002 as
select
    t1.*,
    t1.device_id as parent_id,
    t2.device_name, -- 设备名称 无人机
    cast(null as varchar) as source,
    if(t1.source_type is not null,t1.source_type,t2.device_name) as source_type

from tmp_source_kafka_001 as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.device_id = t2.device_id;




-----------------------

-- 数据插入

-----------------------


begin statement set;


-- 执法仪、无人机轨迹数据入库
insert into dwd_device_track_rt
select
    device_id               ,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format  ,
    acquire_timestamp         ,
    longitude   as lng_02     ,
    latitude    as lat_02     ,
    username                  ,
    group_id                  ,
    product_key               ,
    tid                       ,
    bid                       ,
    device_name               ,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_05;



-- 截图
insert into device_media_datasource
select
    device_id            as device_id,
    'SCREENSHOT'         as source_id,
    cast(null as string) as source_name,
    'PICTURE' as type,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as start_time,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as end_time,
    concat('/',picture_url)    as url,
    width,
    height,
    bid,
    tid,
    cast(null as varchar) as b_type,
    '{}'                  as extends,
    from_unixtime(unix_timestamp()) as gmt_create,
    'ja-flink' as gmt_create_by,
    'ja-flink' as gmt_modified_by
from tmp_source_kafka_01
where
    acquire_timestamp is not null
  and picture_url is not null
  and `method` = 'platform.capture.post';


-- 拍照
insert into device_media_datasource
select
    device_id,
    'PHOTOGRAPH'              as source_id,
    cast(null as string)         as source_name,
    'PICTURE'  as type,
    from_unixtime(message_acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as start_time,
    from_unixtime(message_acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as end_time,
    photo_url                    as url,
    0 as width,
    0 as height,
    message_bid as bid,
    message_tid as tid,
    'WAYLINE_TASK' as b_type,
    '{}'                  as extends,
    from_unixtime(unix_timestamp()) as gmt_create,
    'ja-flink' as gmt_create_by,
    'ja-flink' as gmt_modified_by
from tmp_source_kafka_01
where
    message_acquire_timestamp is not null
  and message_method = 'event.mediaFileUpload.info'
  and photo_url is not null
  and is_capture is null;



-- 设备(红外可见光)检测全量数据入库doris
insert into dwd_photoelectric_target_all_rt_pre
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
insert into dws_photoelectric_target_status_rt_pre
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


