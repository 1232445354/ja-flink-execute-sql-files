--********************************************************************--
-- author:      write your name here
-- create time: 2024/12/2 19:42:10
-- description: 各类事件数据存储
--version:ja-event-save-v1-241203
--********************************************************************--

set 'pipeline.name' = 'ja-event-save';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '4';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-event-save' ;


 -- -----------------------

 -- 数据结构

 -- -----------------------


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
                                                      object_list array<
                                                          row<
                                                          bbox_height          bigint,
                                                      bbox_width           bigint,
                                                      bbox_left            bigint,
                                                      bbox_top             bigint,
                                                      class_id             bigint,
                                                      confidence           double,
                                                      image_path           string,  -- 目标小图
                                                      image_source         string, -- 图片来源设备
                                                      infer_id             bigint,
                                                      object_id            bigint,  -- 可见光、红外检测目标的id
                                                      object_label         string,  -- 舰船
                                                      object_sub_label     string,  -- 舰船
                                                      radar_target_id      string,  -- 对应的雷达检出目标id
                                                      radar_device_id      string,  -- 雷达设备id
                                                      video_time           bigint,
                                                      speed                double,  -- 速度m/s
                                                      distance             double,  -- 距离单位m
                                                      yaw                  double,  -- 目标方位
                                                      source_type          string,  -- 数据来源:VISUAL:可见光,INFRARED:红外,FUSHION:	融合,VIBRATOR: 震动器
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
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'photoelectric_id',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 209的设备检测数据
drop table if exists jiance_209;
create table jiance_209(
                           localLotNo            string,    -- 本地批号
                           RCS                   bigint,    -- RCS值
                           `timestamp`           bigint,    -- 时间ms
                           dataCycle             bigint,    -- 数据周期
                           recognitionRate       bigint,    -- 目标识别概率
                           superiorLotNo         bigint,    -- 上级批号,每个车辆唯一
                           targetType            bigint,    -- 目标类型
                           targetProperty        bigint,    -- 目标属性
                           trackStatus           bigint,    -- 航迹状态
                           targetNum             bigint,    -- 目标数量
                           targetModel           bigint,    -- 目标型号
                           xSpeed                bigint,    -- X向速度
                           ySpeed                bigint,    -- Y向速度
                           zSpeed                bigint,    -- Z向速度
                           xLocation             bigint,    -- 目标位置X
                           yLocation             bigint,    -- 目标位置Y
                           zLocation             bigint,    -- 目标位置Z
                           pitchSystemError      bigint,    -- 俯仰系统误差
                           pitchRandomError      bigint,    -- 俯仰随机误差
                           slantRangeSystemError bigint,    -- 斜距随机误差
                           slantRangeRandomError bigint,    -- 斜距系统误差
                           speedRandomError      bigint,    -- 速度随机误差
                           speedSystemError      bigint,    -- 速度系统误差
                           yawSystemError        bigint,    -- 方位系统误差
                           yawRandomError        bigint,    -- 方位随机误差
                           overallSpeed          bigint,    -- 合速度
                           proctime as PROCTIME()

) WITH (
      'connector' = 'kafka',
      'topic' = 'ja-detection-output',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-detection-output-2091',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 设备（可见光、红外）检测全量数据入库- 可修改的数据表（Sink：doris）
drop table  if exists dwd_photoelectric_target_all_rt;
create table dwd_photoelectric_target_all_rt(
                                                device_id                  string     , -- '设备id',
                                                target_id                  bigint     , -- '目标id',
                                                parent_id                  string     , -- 父设备的id,也就是望楼id
                                                acquire_timestamp_format   string     , -- '上游程序上报时间戳-时间戳格式化',
                                                acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                                source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                                source_type_name           string     , -- 数据设备来源名称，就是设备类型，使用product_key区分的

                                                device_name                string     , -- 设备名称
                                                radar_id                   string     , -- 雷达id
                                                radar_target_id            string     , -- 雷达检测的目标id
                                                radar_device_name          string     , -- 雷达的设备名称
                                                device_info                string     , -- 数据检测的来源拼接 示例：雷达（11）、可见光（22）
                                                record_path                string     , -- 可见光、红外告警视频地址
                                                bbox_height                double     , -- 长度
                                                bbox_left	               double     , -- 左
                                                bbox_top	               double     , -- 上
                                                bbox_width	               double     , -- 宽度
                                                source_frame_height        bigint     , -- 原视频高度
                                                source_frame_width         bigint     , -- 原视频宽度
                                                longitude                  double     , -- 目标经度
                                                latitude                   double 	  , -- 目标纬度
                                                altitude                   double     , -- 高度
                                                big_image_path             string     , -- 大图
                                                small_image_path           string     , -- 小图
                                                image_source               string     , -- 图片来源设备
                                                class_id                   double     ,
                                                confidence                 string     , -- 置信度
                                                infer_id                   double     ,
                                                object_label               string     , -- 目标的类型，人，车
                                                object_sub_label           string     , -- 目标的类型子类型
                                                speed                      double     , -- 目标速度 m/s
                                                distance                   double     , -- 距离 m
                                                yaw                        double     , -- 目标方位
                                                update_time                string      -- 数据入库时间
)WITH (
     'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',  -- k8s部署
     'fenodes' = '172.21.30.202:30030',                                       -- 物理机器部署
     'table.identifier' = 'dushu.dwd_photoelectric_target_all_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='10s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-- 设备（可见光、红外）检测全量数据入库- 不可修改的数据表（Sink：doris）
drop table  if exists dwd_photoelectric_target_all_rt_source;
create table dwd_photoelectric_target_all_rt_source(
                                                       device_id                  string     , -- '设备id',
                                                       target_id                  bigint     , -- '目标id',
                                                       parent_id                  string     , -- 父设备的id,也就是望楼id
                                                       acquire_timestamp_format   string     , -- '上游程序上报时间戳-时间戳格式化',
                                                       acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                                       source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                                       source_type_name           string     , -- 数据设备来源名称，就是设备类型，使用product_key区分的

                                                       device_name                string     , -- 设备名称
                                                       radar_id                   string     , -- 雷达id
                                                       radar_target_id            string     , -- 雷达检测的目标id
                                                       radar_device_name          string     , -- 雷达的设备名称
                                                       device_info                string     , -- 数据检测的来源拼接 示例：雷达（11）、可见光（22）
                                                       record_path                string     , -- 可见光、红外告警视频地址
                                                       bbox_height                double     , -- 长度
                                                       bbox_left	               double     , -- 左
                                                       bbox_top	               double     , -- 上
                                                       bbox_width	               double     , -- 宽度
                                                       source_frame_height        bigint     , -- 原视频高度
                                                       source_frame_width         bigint     , -- 原视频宽度
                                                       longitude                  double     , -- 目标经度
                                                       latitude                   double 	  , -- 目标纬度
                                                       altitude                   double     , -- 高度
                                                       big_image_path             string     , -- 大图
                                                       small_image_path           string     , -- 小图
                                                       image_source               string     , -- 图片来源设备
                                                       class_id                   double     ,
                                                       confidence                 string     , -- 置信度
                                                       infer_id                   double     ,
                                                       object_label               string     , -- 目标的类型，人，车
                                                       object_sub_label           string     , -- 目标的类型子类型
                                                       speed                      double     , -- 目标速度 m/s
                                                       distance                   double     , -- 距离 m
                                                       yaw                        double     , -- 目标方位
                                                       update_time                string      -- 数据入库时间
)WITH (
     'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',  -- k8s部署
     'fenodes' = '172.21.30.202:30030',                                       -- 物理机器部署
     'table.identifier' = 'dushu.dwd_photoelectric_target_all_rt_source',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='10s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-- 209检测数据- 单独表（Sink：doris）
drop table  if exists dwd_event_save_all;
create table dwd_event_save_all(
                                   device_id                  string     , -- '设备id',
                                   target_id                  string     , -- '目标id',
                                   parent_id                  string     , -- 父设备的id,也就是望楼id
                                   acquire_timestamp_format   string     , -- '上游程序上报时间戳-时间戳格式化',
                                   acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',

                                   local_lot_no              varchar(300)  ,-- 本地批号,设备sn号
                                   rcs                       bigint,        -- RCS值
                                   data_cycle                bigint,        -- 数据周期
                                   recognition_rate          bigint,        -- 目标识别概率
                                   superior_lot_no           bigint,        -- 上级批号,每个车辆唯一
                                   target_type               bigint,        -- 目标类型
                                   target_property           bigint,        -- 目标属性
                                   track_status              bigint,        -- 航迹状态
                                   target_num                bigint,        -- 目标数量
                                   target_model              bigint,        -- 目标型号

                                   x_speed                   bigint,        -- X向速度
                                   y_speed                   bigint,        -- Y向速度
                                   z_speed                   bigint,        -- Z向速度
                                   x_location                bigint,        -- 目标位置X
                                   y_location                bigint,        -- 目标位置Y
                                   z_location                bigint,        -- 目标位置Z
                                   pitch_system_error        bigint,        -- 俯仰系统误差
                                   pitch_random_error        bigint,        -- 俯仰随机误差
                                   slant_range_system_error  bigint,        -- 斜距随机误差
                                   slant_range_random_error  bigint,        -- 斜距系统误差
                                   speed_random_error        bigint,        -- 速度随机误差
                                   speed_system_error        bigint,        -- 速度系统误差
                                   yaw_system_error          bigint,        -- 方位系统误差
                                   yaw_random_error          bigint,        -- 方位随机误差
                                   overall_speed             bigint,        -- 合速度

                                   source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                   source_type_name           string     , -- 数据设备来源名称，就是设备类型，使用product_key区分的
                                   device_name                string     , -- 设备名称
                                   device_info                string     , -- 数据检测的来源拼接 示例：雷达（11）、可见光（22）
                                   object_label               string     , -- 目标类型名称-中文
                                   update_time                string      -- 数据入库时间
)WITH (
     'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',  -- k8s部署
     'fenodes' = '172.21.30.202:30030',                                       -- 物理机器部署
     'table.identifier' = 'dushu.dwd_event_save_all',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='10s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-- 建立映射mysql的表（device）
drop table if exists device;
create table device (
                        id	             int,    -- 自增id
                        device_id	     string, -- 设备id
                        type             string, -- 设备名称
                        name             string, -- 设备名称
                        sn               string, -- 设备sn号
                        primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true&autoReconnect=true', -- ECS环境
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true&autoReconnect=true',  -- 201环境
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'device',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '10'
     );


-- 建立映射mysql的表（为了查询用户名称）
drop table if exists iot_device;
create table iot_device (
                            id	             int,    -- 自增id
                            parent_id        string, -- 父设备的id,也就是望楼id
                            device_id	     string, -- 设备id
                            device_name      string, -- 设备名称
                            product_key      string, -- 产品key
                            gmt_create_by	 string, -- 创建用户名
                            primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true&autoReconnect=true', -- ECS环境
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true&autoReconnect=true',    -- 201环境
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'iot_device',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '10'
     );



-- 209目标类型枚举表
drop table if exists enum_target_name;
create table enum_target_name (
                                  id	             bigint,    -- 自增id
                                  target_name      string, -- 目标类型
                                  primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true&autoReconnect=true', -- ECS环境
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true&autoReconnect=true',    -- 201环境
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'enum_target_name',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '10'
     );


-----------------------

-- 数据处理

-----------------------

-- **************************** 来源1*********************************
-- 可见光、红外检测数据处理
drop view if exists tmp_source_kafka_001;
create view tmp_source_kafka_001 as
select
    t1.batch_id,
    t1.image_path as big_image_path,     -- 外层的大图
    t1.record_path,                      -- 告警视频地址
    t1.source_id as device_id,           -- 设备id
    t1.ntp_timestamp as acquire_timestamp,
    -- TO_TIMESTAMP_LTZ(t1.ntp_timestamp,3) as acquire_timestamp_format,
    from_unixtime(t1.ntp_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    t1.source_frame_height,
    t1.source_frame_width,
    t2.image_path small_image_path,   -- 里层的小图
    t2.image_source,
    t2.object_id as target_id,
    t2.radar_target_id,
    t2.radar_device_id as radar_id,
    t2.bbox_height,
    t2.bbox_left,
    t2.bbox_top,
    t2.bbox_width,
    t2.longitude,
    t2.latitude,
    t2.altitude,
    t2.class_id,
    cast(t2.confidence as varchar) as confidence,
    t2.infer_id,
    t2.object_label,
    t2.object_sub_label,
    t2.speed,
    t2.distance,
    t2.yaw,
    t2.source_type,
    PROCTIME()  as proctime

from photoelectric_inspection_result_kafka as t1
         cross join unnest (object_list) as t2 (
                                                bbox_height          ,
                                                bbox_width           ,
                                                bbox_left            ,
                                                bbox_top             ,
                                                class_id             ,
                                                confidence           ,
                                                image_path           , -- 目标小图
                                                image_source         , -- 图片来源设备
                                                infer_id             ,
                                                object_id            , -- 可见光、红外检测的目标id
                                                object_label         , -- 舰船
                                                object_sub_label     , -- 舰船
                                                radar_target_id      , -- 雷达检测到的目标的目标id
                                                radar_device_id      , -- 雷达设备id
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
  and t1.ntp_timestamp is not null -- 类型
  and t2.source_type is not null;



-- 可见光红外数据关联设备表取出设备名称
drop view if exists tmp_source_kafka_002;
create view tmp_source_kafka_002 as
select
    t1.*,
    t2.device_name, -- 设备名称可见光、红外
    t2.parent_id,
    t3.device_name as radar_device_name,    -- 雷达设备名称

    if(source_type = 'FUSHION','融合目标',t2.device_name) as source_type_name,

    case when source_type in ('VISUAL','INFRARED') then -- 可见光、红外
             concat('[{',
                    concat('"deviceName":"',t2.device_name,'",'),
                    concat('"deviceId":"',t1.device_id,'",'),
                    concat('"targetId":"',cast(target_id as varchar),'",'),
                    concat('"type":"',source_type,'"}]')
                 )
         when source_type in ('FUSION') then
             concat('[{',
                    concat('"deviceName":"',t2.device_name,'",'),
                    concat('"deviceId":"',t1.device_id,'",'),
                    concat('"targetId":"',cast(target_id as varchar),'",'),
                    concat('"type":"','"},{'),
                    concat('"deviceName":"',t3.device_name,'",'),
                    concat('"deviceId":"',radar_id,'",'),
                    concat('"targetId":"',radar_target_id,'",'),
                    concat('"type":"RADAR"}]')
                 )
         else cast(null as varchar) end as device_info

from tmp_source_kafka_001 as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2     -- 关联取设备名称，可见光/红外设备名称
                   on t1.device_id = t2.device_id
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t3     -- 关联取设备名称，雷达设备名称
                   on t1.radar_id = t3.device_id;


-- **************************** 来源2*********************************

create view ja_tmp01 as
select
    t1.*,
    t1.localLotNo as sn,
    t1.`timestamp`   as acquire_timestamp,
    from_unixtime(t1.`timestamp`/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    cast(t1.targetModel as varchar)      as target_model,
    t2.device_id,
    if(t3.parent_id is not null and t3.parent_id <> '',t3.parent_id,t2.device_id) as parent_id,
    t2.name as device_name,
    concat(cast(t1.`timestamp` as varchar),cast(RAND_INTEGER(10000) as varchar)) as target_id,

    if(t4.target_name is not null,t4.target_name,'未知')       as object_label,   -- 目标类型对应中文
    if(t2.type is not null,t2.type,'none') as source_type,
    if(t2.name is not null,t2.name,'未知') as source_type_name

from jiance_209 as t1
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t2     -- 关联取设备名称，可见光/红外设备名称
                   on t1.localLotNo = t2.sn
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t3     -- 关联取设备名称，可见光/红外设备名称
                   on t2.device_id = t3.device_id
         left join enum_target_name FOR SYSTEM_TIME AS OF t1.proctime as t4     -- 关联取设备名称，可见光/红外设备名称
                   on t1.targetType = t4.id
where t2.device_id is not null;


-----------------------

-- 数据插入

-----------------------

begin statement set;


-- 红外可见光目标入表 - 可修改表
insert into dwd_photoelectric_target_all_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    source_type,
    source_type_name,
    device_name,
    radar_id,
    radar_target_id,
    radar_device_name,
    device_info,
    record_path,
    bbox_height,
    bbox_left,
    bbox_top,
    bbox_width,
    source_frame_height,
    source_frame_width,
    longitude,
    latitude,
    altitude,
    big_image_path,
    small_image_path,
    image_source,
    class_id,
    confidence,
    infer_id,
    object_label,
    object_sub_label,
    speed,
    distance,
    yaw,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_002;




-- 红外可见光目标入表 - 不可修改表
insert into dwd_photoelectric_target_all_rt_source
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    source_type,
    source_type_name,
    device_name,
    radar_id,
    radar_target_id,
    radar_device_name,
    device_info,
    record_path,
    bbox_height,
    bbox_left,
    bbox_top,
    bbox_width,
    source_frame_height,
    source_frame_width,
    longitude,
    latitude,
    altitude,
    big_image_path,
    small_image_path,
    image_source,
    class_id,
    confidence,
    infer_id,
    object_label,
    object_sub_label,
    speed,
    distance,
    yaw,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_002;


-- 209数据入库
insert into dwd_event_save_all
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    localLotNo             as local_lot_no,
    RCS                    as rcs,
    dataCycle              as data_cycle,
    recognitionRate        as recognition_rate,
    superiorLotNo          as superior_lot_no,
    targetType             as target_type,
    targetProperty         as target_property,
    trackStatus            as track_status,
    targetNum              as target_num,
    targetModel            as target_model,
    xSpeed                 as x_speed,
    ySpeed                 as y_speed,
    zSpeed                 as z_speed,
    xLocation              as x_location,
    yLocation              as y_location,
    zLocation              as z_location,
    pitchSystemError       as pitch_system_error,
    pitchRandomError       as pitch_random_error,
    slantRangeSystemError  as slant_range_system_error,
    slantRangeRandomError  as slant_range_random_error,
    speedRandomError       as speed_random_error,
    speedSystemError       as speed_system_error,
    yawSystemError         as yaw_system_error,
    yawRandomError         as yaw_random_error,
    overallSpeed           as overall_speed,
    source_type,
    source_type_name,
    device_name,
    cast(null as varchar) as device_info,
    object_label,
    from_unixtime(unix_timestamp()) as update_time
from ja_tmp01;

end;




