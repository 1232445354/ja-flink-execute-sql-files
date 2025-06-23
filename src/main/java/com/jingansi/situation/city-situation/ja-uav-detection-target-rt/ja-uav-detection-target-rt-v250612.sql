--********************************************************************--
-- author:      write your name here
-- create time: 2025/6/12 15:47:35
-- description: write your description here
-- version: ja-uav-detection-target-rt-v250612 无人机检测目标入库
--********************************************************************--
set 'pipeline.name' = 'ja-uav-detection-target-rt';


SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '4';
-- set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
-- SET 'execution.checkpointing.interval' = '600000';
-- SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-uav-detection-target-rt';



-- 设备检测数据上报 （Source：kafka）
create table iot_device_message_kafka_01 (
                                             productKey    string     comment '产品编码',
                                             deviceId      string     comment '设备id',
                                             type          string     comment '类型',
                                             version       string     comment '版本',
                                             `timestamp`   bigint     comment '时间戳毫秒',
                                             tid           string     comment '当前请求的事务唯一ID',
                                             bid           string     comment '长连接整个业务的ID',
                                             `method`      string     comment '服务&事件标识',
                                             message  row(
                                                 tid                     string, -- 当前请求的事务唯一ID
                                                 bid                     string, -- 长连接整个业务的ID
                                                 version                 string, -- 版本
                                                 `timestamp`             bigint, -- 时间戳
                                                 `method`                string, -- 服务&事件标识
                                                 productKey              string, -- 产品编码
                                                 deviceId                string, -- 设备编码
                                                 `data` row(

                                                 -- 雷达、振动仪检测数据
                                                 targets array<
                                                 row(
                                                 targetId         string   ,-- -目标id
                                                 targetAltitude   double   ,-- 目标海拔高度、
                                                 targetLongitude  double   ,-- 目标经度
                                                 targetLatitude   double   , -- 目标纬度
                                                 distance         double   , -- 上报距离
                                                 status           string   ,-- 0 目标跟踪 1 目标丢失 2 跟踪终止
                                                 sourceType       string   , -- 数据来源类型：M300、RADAR
                                                 targetType       string   , -- 目标类型,信火一体时候的 ，天朗项目-也有，目标识别类型
                                                 `timestamp`      bigint   , -- 录取时间信息（基日）
                                                 confidence       string   , -- 置信度
                                                 imageUrl         string   ,
                                                 bboxLeft         double   ,
                                                 bboxTop          double   ,
                                                 bboxWidth        double   ,
                                                 bboxHeight       double

                                                 )
                                                 >
                                                 )
                                                 )
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.231:30090',
      'properties.group.id' = 'iot-device-message-group1',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1750387320000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 检测目标全量数据入库（Sink：doris）
create table dwd_bhv_uav_detection_target(
                                             id     			string 	comment '目标id',
                                             acquire_time 		string  comment '采集事件',
                                             distance       	double  comment '距离',
                                             source_type		string	comment '检测的无人机',
                                             confidence 		string 	comment '置信度',
                                             `status`			string 	comment '状态',
                                             target_altitude	double 	comment '高度',
                                             longitude			double	comment '经度',
                                             latitude			double	comment '纬度',
                                             object_label      string  comment '目标大类型代码',
                                             object_label_name string  comment '目标大类型名称',
                                             target_type		string	comment '目标类型',
                                             acqu_timestamp	bigint  comment '内部的时间戳',
                                             image_url 		string 	comment '图片url',
                                             bbox_left         double  comment '框信息',
                                             bbox_top          double  comment '框信息',
                                             bbox_width        double  comment '框信息',
                                             bbox_height       double  comment '框信息',
                                             device_id			string	comment '设备id',
                                             product_key		string	comment '产品key',
                                             type				string	comment '类型',
                                             tid				string	comment 'tid',
                                             bid				string	comment 'bid',
                                             `method`			string	comment 'method',
                                             update_time       string  comment '数据入库时间'
)WITH (
     'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     'fenodes' = '172.21.30.105:30030',
-- 'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'sa.dwd_bhv_uav_detection_target',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='5s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 检测目标的图片（Sink：doris）
create table dws_et_uav_detection_target_image(
                                                  id     			string 	comment '目标id',
                                                  device_id			string	comment '设备id',
                                                  acquire_time 		string  comment '采集事件',
                                                  image_url 		string 	comment '图片url',
                                                  bbox_left         double  comment '框信息',
                                                  bbox_top          double  comment '框信息',
                                                  bbox_width        double  comment '框信息',
                                                  bbox_height       double  comment '框信息',
                                                  update_time       string  comment '数据入库时间'
)WITH (
     'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     'fenodes' = '172.21.30.105:30030',
-- 'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'sa.dws_et_uav_detection_target_image',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='5s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-----------------------

-- 数据处理

-----------------------

-- kafka来源的所有数据解析
create view tmp_source_kafka_01 as
select
    coalesce(productKey,message.productKey)   as product_key, -- message_product_key
    coalesce(deviceId,message.deviceId)       as device_id,   -- message_device_id
    coalesce(version,message.version)         as version, -- message_version
    coalesce(`timestamp`,message.`timestamp`) as acquire_timestamp, -- message_acquire_timestamp
    coalesce(tid,message.tid)                 as tid, -- message_tid
    coalesce(bid,message.bid)                 as bid, -- message_bid
    coalesce(`method`,message.`method`)       as `method`, -- message_method
    type,

    -- 雷达、振动仪检测目标数据
    message.`data`.targets     as targets
from iot_device_message_kafka_01
where coalesce(deviceId,message.deviceId) is not null
  -- 小于10天的数据
  and abs(coalesce(`timestamp`,message.`timestamp`)/1000 - UNIX_TIMESTAMP()) <= 864000;


-- 检测数据筛选处理
create view tmp_source_kafka_03 as
select
    tid,
    bid,
    `method`,
    product_key,
    device_id,
    version,
    type,
    acquire_timestamp,
    targets            -- 目标字段
from tmp_source_kafka_01
where `method` = 'event.targetInfo.info'
  and product_key in ('00000000002','zyrFih3kepx');



-- 设备检测数据进一步解析数组
create view tmp_source_kafka_04 as
select
    t1.tid,
    t1.bid,
    t1.`method`,
    t1.product_key,
    t1.device_id,
    t1.version,
    t1.type,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_time,

    t2.targetId          as id ,     -- 目标id
    t2.targetAltitude    as target_altitude, -- 高度
    t2.status            as status,          -- 状态
    t2.targetLongitude   as longitude,       -- 经度
    t2.targetLatitude    as latitude,        -- 纬度
    t2.distance          as distance,
    t2.sourceType        as source_type,
    if(t2.targetType <> '' and t2.targetType <> '未知目标',t2.targetType,'未知')    as target_type,
    t2.`timestamp`       as acqu_timestamp,
    t2.confidence,
    t2.imageUrl as image_url,
    t2.bboxLeft as bbox_left,
    t2.bboxTop as bbox_top,
    t2.bboxWidth as bbox_width,
    t2.bboxHeight as bbox_height

from tmp_source_kafka_03 as t1
         cross join unnest (targets) as t2 (
                                            targetId         ,
                                            targetAltitude   ,
                                            targetLongitude  ,
                                            targetLatitude   ,
                                            distance         ,
                                            status           ,
                                            sourceType       ,
                                            targetType       ,
                                            `timestamp`      ,
                                            confidence       ,
                                            imageUrl         ,
                                            bboxLeft         ,
                                            bboxTop          ,
                                            bboxWidth        ,
                                            bboxHeight
    );


-----------------------

-- 数据插入

-----------------------


begin statement set;

-- 检测全量数据入库doris
insert into dwd_bhv_uav_detection_target
select
    id     			,
    acquire_time 		,
    distance       	,
    source_type		,
    confidence 		,
    `status`			,
    target_altitude	,
    longitude			,
    latitude			,
    'NONE' as object_label,
    '未知'  as object_label_name,
    target_type		,
    acqu_timestamp	,
    image_url 		,
    bbox_left         ,
    bbox_top          ,
    bbox_width        ,
    bbox_height       ,
    device_id			,
    product_key		,
    type				,
    tid				,
    bid				,
    `method`			,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_04;


-- 检测图片数据入库doris
insert into dws_et_uav_detection_target_image
select
    id     			,
    device_id			,
    acquire_time 		,
    image_url 		,
    bbox_left         ,
    bbox_top          ,
    bbox_width        ,
    bbox_height       ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_04
where image_url is not null
  and bbox_left is not null;

end;

