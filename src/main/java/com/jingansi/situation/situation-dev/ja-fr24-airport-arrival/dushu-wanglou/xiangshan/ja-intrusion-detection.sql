--********************************************************************--
-- author:     yibo@jingan-inc.com
-- create time: 2024/06/28 16:28:19
-- description: 告警、区分object_label类型、并且合并车牌
-- version:ja-intrusion-detection-250101
--********************************************************************--


set 'pipeline.name' = 'ja-intrusion-detection';


-- SET 'parallelism.default' = '2';
SET 'table.exec.state.ttl' = '600000';
SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'sql-client.display.max-column-width' = '100';

-- checkpoint的时间和位置
SET 'execution.checkpointing.interval' = '60000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-intrusion-detection' ;


-- 判断矩形四个点是否在多边形内，这里不是经纬度
create function rectangle_intersect_polygon as 'com.jingan.udf.geohash.RectangleIntersectPolygon';

-- 合并车牌
create function merge_plate_no as 'com.jingan.udf.merge.MergePlateNoAllColumnUdf';

-- 区域告警
create function in_polygon as 'com.jingan.udf.geohash.GeoPolygonUdf';


-----------------------

-- 数据来源写出格式

-----------------------


-- kafka来源的数据（Source：kafka）
create table frame_infer_data (
                                  batch_id                bigint,                        -- 批处理ID
                                  frame_num               int,                           -- 帧编号
                                  ntp_timestamp           bigint,                        -- 时间戳
                                  infer_done              boolean,
                                  image_path              string,                        -- 大图图片存储路径
                                  record_path             string,                        -- 告警视频地址
                                  pts                     bigint,                        -- pts 值
                                  source_id               string,                        -- 数据源ID
                                  source_frame_width      int,                           -- 原始帧宽度
                                  source_frame_height     int,                           -- 原始帧高度
                                  frame_tensor_list       string,                        -- 输出基于帧的特征向量
                                  object_list             array<
                                      row(
                                      object_id           bigint,                -- 目标ID
                                      object_label        string,                -- 目标类型大类(Face,Person,MotorVehicle,NonMotorVehicle),这一版本也更改为中文了
                                      object_sub_label    string,                -- 目标类型小类（轿车、卡车、自行车、摩托车...）
                                      infer_id            int,                   -- 推理算子ID
                                      class_id            int,
                                      bbox_left           int,                   -- 左上角坐标
                                      bbox_top            int,                   -- 左上角坐标
                                      bbox_width          int,                   -- 目标宽度
                                      bbox_height         int,                   -- 目标高度
                                      confidence          decimal(20,18),        -- 目标置信度
                                      image_path          string,                -- 目标图片存储路径
                                      longitude           double,                -- 目标经度
                                      latitude            double,                -- 目标纬度
                                      altitude            double,                -- 高度
                                      speed               double,                -- 速度m/s
                                      distance            double,                -- 距离 单位m
                                      radar_target_id     double,                -- 雷达目标id
                                      radar_device_id     string,                -- 雷达设备id
                                      video_time          double,
                                      yaw                 double,
                                      source_type         string,                -- 来源类型
                                      obj_label_list      array<                 -- 该目标结构化属性信息
                                      row(
                                      label_name   string,
                                      label_value  string
                                      )>,
                                      obj_track_list      string,                -- 该目标在同一个摄像头中的轨迹
                                      obj_tensor_list     array<                 -- 目标特征向量列表
                                      row(
                                      infer_id    int,
                                      tensors     array<
                                      row(
                                      layer_name      string,
                                      dims            string,
                                      tensor          string
                                      -- tensor          array<decimal(20,18)>
                                      )>
                                      )>
                                      )
                                      >
) WITH (
      'connector' = 'kafka',
      'topic' = 'ja-ai-detection-output',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-intrusion-detection',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 天朗雷达
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
                                                 -- 媒体拍照数据
                                                 photoUrl            string, -- 媒体上报的拍照图片url
                                                 height              double,
                                                 isCapture           bigint, -- 筛选过滤字段

                                                 -- 执法仪轨迹
                                                 longitude           double not null, -- 经度
                                                 latitude            double not null, -- 纬度
                                                 deviceName          string, -- 执法仪名称
                                                 attitudeHead        double, -- 无人机机头朝向
                                                 gimbalHead          double, -- 无人机云台朝向

                                                 -- 雷达、振动仪检测数据
                                                 targets array<
                                                 row(
                                                 targetId         string   ,-- -目标id
                                                 xDistance        double   ,-- x距离
                                                 yDistance        double   ,-- y距离
                                                 speed            double   ,-- 速度
                                                 targetPitch      double   ,-- 俯仰角
                                                 targetAltitude   double   ,-- 目标海拔高度
                                                 status           string   ,-- 0 目标跟踪 1 目标丢失 2 跟踪终止
                                                 targetLongitude  double   ,-- 目标经度
                                                 targetLatitude   double   , -- 目标纬度
                                                 distance         double   , -- 上报距离
                                                 targetYaw        double   , -- 水平角度
                                                 utc_time         bigint   , -- 雷达检测数据上报时间戳
                                                 tracked_times    double   , -- 已跟踪次数
                                                 loss_times       double   , -- 连续丢失次数
                                                 sourceType       string   , -- 数据来源类型：M300、RADAR
                                                 targetType       string   , -- 目标类型

                                                 -- 振动仪数据
                                                 objectLabel          string, -- 大类型
                                                 targetCredibility    double,
                                                 `time`               string,
                                                 -- 天朗设备
                                                 RCS               bigint,
                                                 radialDistance    double,
                                                 targetState       bigint,
                                                 `timestamp`       bigint,
                                                 timestampBase     bigint,
                                                 uploadMode        string
                                                 )
                                                 >
                                                 )
                                                 )
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-intrusion-detection1',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1720682617000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 框选监控区域表，这个不是经纬度，是区域点（Source：mysql）
create table video_area (
                            id                             bigint        comment '主键',
                            device_id                      string        comment '设备id',
                            points                         string        comment '点位列表数组(json)',
                            type                           string        comment '框的用途类型',
                            PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'video_area',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );



-- 事件告警数据表（Sink：kafka）
create table event_warn_kafka(
                                 eventId             string     comment '唯一编号',
                                 eventNo             string     comment '事件编号',
                                 eventName           string     comment '事件名称',
                                 deviceId            string     comment '设备id',
                                 deviceName          string     comment '设备名称',
                                 deviceType          string     comment '设备类型',
                                 eventType           string     comment '事件类型,anomaly：异常物,dangerous：危险物 必填',
                                 `level`             string     comment '紧急等级 High、Middle、Low 必填',
                                 eventTime           timestamp  comment '事件发生时间 必填',
                                 sourceFrameWidth    bigint     comment '原始帧宽度',
                                 sourceFrameHeight   bigint     comment '原始帧高度',
                                 sourceImage         string     comment '异常物的大图uri地址',
                                 longitude           double     comment '经度',
                                 latitude            double     comment '纬度',
                                 targetId            string     comment 'mmsi或者是雷达的target目标id',
                                 recordPath          string     comment '告警视频地址',
                                 speed               double     comment '速度m/s',
                                 distance            double     comment '距离m',
                                 radarId             string,
                                 radarTargetId       double,
                                 sourceType          string,
                                 altitude            double,
                                 objList             array<
                                     row(
                                     image               string     , -- 异常物的小图uri地址
                                     leftTopX            int        , -- 左上角x坐标
                                     leftTopY            int        , -- 左上角Y坐标
                                     bboxWidth           int        , -- 目标宽度
                                     bboxHeight          int        , -- 目标高度
                                     confidence          decimal(20,18), -- 置信度
                                     objLabelList        array<
                                     row(
                                     labelName   string, -- 属性名称
                                     labelValue  string  -- 属性值
                                     )
                                     >
                                     )
                                     > ,                                -- 检测出的目标对象列表
                                 reid                boolean     ,  -- '是否为reid告警,true:是。false:否'
                                 primary key (targetId,eventTime) NOT ENFORCED
) with (
      'connector' = 'upsert-kafka',
      'topic' = 'event_warn',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-intrusion-detection',
      'key.format' = 'json',
      'value.format' = 'json'
      );



-- 子父设备表（Source：mysql）
create table iot_device (
                            id                             int           comment 'id',
                            parent_id                      string        comment '父设备id',
                            device_id                      string        comment '子设备id',
                            device_name                    string        comment '设备名称',
                            gmt_create_by                  string        comment '用户名',
                            PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'iot_device',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );


-- 设备表（Source：mysql）
create table device (
                        id                             int           comment '',
                        device_id                      string        comment '设备编码',
                        name                           string        comment '设备名称',
                        username                       string        comment '最近登陆的用户名',
                        type                           string        comment '设备类型',
                        longitude                      decimal(12,8) comment '经度',
                        latitude                       decimal(12,8) comment '纬度',
                        PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );


-- 告警类型枚举表（Source：mysql）
create table event_type (
                            id              int,      -- id
                            event_type      string,   -- 事件类型
                            event_name      string,   -- 事件名称
                            `level`         string,   -- 事件等级
                            PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'event_type',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );


-- 覆盖物区域表（Source：mysql）
create table `overlay` (
                           id                             int           comment '编号',
                           overlay_name                   string        comment '区域名称',
                           overlay_type                   string        comment '区域类型',
                           overlay_positions              string        comment '点',
                           gmt_create_by                  string        comment '创建人',
                           PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'overlay',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );


---------------

-- 数据处理

---------------

-- *********************************** 来源1 望楼可见光/红外、无人机*************************************

-- 数据展开并将数据传入自定义函数中 合并车牌
create view tmp_frame_infer_data_01 as
select
    a.batch_id,
    a.frame_num,
    a.pts,
    to_timestamp(from_unixtime(ntp_timestamp/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as ntp_timestamp,
    a.source_id,
    a.source_frame_width,
    a.source_frame_height,
    a.infer_done,
    a.record_path,
    a.image_path as big_image_path,
    a.frame_tensor_list,
    t.radar_device_id as radar_id,
    t.radar_target_id,
    t.video_time,
    t.yaw,
    t.source_type,
    t.obj_label_list,
    t.obj_track_list,
    t.object_id,
    t.object_label,
    t.object_sub_label,
    t.infer_id,
    t.class_id,
    t.bbox_left,
    t.bbox_top,
    t.bbox_width,
    t.bbox_height,
    t.confidence,
    t.image_path,
    t.longitude,
    t.latitude,
    t.speed,
    t.distance,
    t.altitude,
    t.obj_tensor_list,
    PROCTIME() as proctime
from (select *,merge_plate_no(object_list) as object_list1 from frame_infer_data) a
         cross join unnest (object_list1) as t (
                                                object_id,
                                                object_label,
                                                object_sub_label,
                                                infer_id,
                                                class_id,
                                                bbox_left,
                                                bbox_top,
                                                bbox_width,
                                                bbox_height,
                                                confidence,
                                                image_path,
                                                longitude,
                                                latitude,
                                                altitude,
                                                speed,
                                                distance,
                                                radar_target_id,
                                                radar_device_id,
                                                video_time,
                                                yaw,
                                                source_type,
                                                obj_label_list,
                                                obj_track_list,
                                                obj_tensor_list
    );



-- 判断布控摄像头，并且出现在画框的区域,过滤数据
-- 有区域的在区域内告警，不在区域内不告警
-- 没有区域都告警
create view tmp_frame_infer_data_02 as
select
    t1.*,
    t3.longitude as parent_longitude_join,
    t3.latitude  as parent_latitude_join,
    t4.type      as device_type_join,
    t4.name      as device_name_join
from (
         select
             a.*,
             b.id as video_area_id,
             rectangle_intersect_polygon(a.bbox_left,a.bbox_top,a.bbox_width,a.bbox_height,b.points) as flag   -- 判断是否在区域内
         from tmp_frame_infer_data_01 a
                  left join video_area FOR SYSTEM_TIME AS OF a.proctime as b
                            on a.source_id = b.device_id
     ) as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2        -- 关联父子设备表,取出父设备ID
                   on t1.source_id=t2.device_id
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t3            -- 关联设备信息表,取出父设备的经纬度 ， 给父设备经纬度是因为望楼子设备数据库位置字段是空（后端子设备位置空）
                   on t2.parent_id=t3.device_id
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t4            -- 关联设备信息表,取出设备的类型
                   on t1.source_id=t4.device_id;


-- 重庆南岸、望楼告警、
create view tmp_frame_infer_data_03 as
select
    *,
    case when device_type_join <> 'UAV' and object_label = '人员' and flag = true        then 'climbing'    -- device_id不为空 说明关联上video_area flag为true说明在区域内   人员入侵
         when device_type_join <> 'UAV' and object_label = '人员' and video_area_id is null  then 'person'      -- 无区域  人员告警
         when device_type_join = 'UAV' and object_label = '人员'                         then 'person'      -- 无人机的告警
         when object_label in('摩托车','车','MotorVehicle','NonMotorVehicle','机动车') then 'car'   -- 车辆告警
         when object_label = '非机动车'      then 'non_car'
         when object_label = '交通事故'      then 'traffic_accident'
         when object_label = '烟雾'         then 'smoke'
         when object_label = '烟火'         then 'fire_detection'
        end as eventType
    -- count(*) over(partition by object_id,device_id order by proctime ) as cnt
from tmp_frame_infer_data_02
where (object_sub_label <> 'license_plate' or object_sub_label is null);



-- 关联事件名称
create view tmp_frame_infer_data_04 as
select
    t1.*,
    t2.event_name,
    t2.`level`
from tmp_frame_infer_data_03 as t1
         left join event_type FOR SYSTEM_TIME AS OF t1.proctime as t2        -- 关联父子设备表,取出父设备ID
                   on t1.eventType = t2.event_type;




-- **************************** 来源3 天朗设备*********************************
-- 这是一段临时代码

create view tmp_source_kafka_01 as
select
    coalesce(productKey,message.productKey)   as product_key, -- message_product_key
    coalesce(deviceId,message.deviceId)       as device_id,   -- message_device_id
    coalesce(version,message.version)         as version, -- message_version
    coalesce(`timestamp`,message.`timestamp`) as acquire_timestamp, -- message_acquire_timestamp
    coalesce(tid,message.tid)                 as tid, -- message_tid
    coalesce(bid,message.bid)                 as bid, -- message_bid
    coalesce(`method`,message.`method`)       as `method`, -- message_method

    -- 雷达、振动仪检测目标数据
    message.`data`.targets     as targets,

    PROCTIME()  as proctime
from iot_device_message_kafka_01
where coalesce(deviceId,message.deviceId) is not null
  and coalesce(`timestamp`,message.`timestamp`) > 1735714109000
  and coalesce(`timestamp`,message.`timestamp`) is not null;



-- 关联设备表取出设备名称,取出父设备望楼id，数据来源都是子设备id
create view tmp_source_kafka_02 as
select
    t1.*,
    t2.name        as device_name_join,
    t2.type        as device_type_join
from tmp_source_kafka_01 as t1
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.device_id = t2.device_id
where product_key ='xjWO7NdIOYs'
  and `method` = 'event.targetInfo.info';



-- 设备检测数据（雷达）数据进一步解析数组
create view tmp_source_kafka_03 as
select
    t1.tid,
    t1.bid,
    t1.`method`,
    t1.product_key,
    t1.device_id,
    t1.version,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    t1.acquire_timestamp   as acquire_timestamp,
    t1.device_name_join,
    t1.device_type_join,

    t2.targetId          as target_id,     -- 目标id
    t2.speed             as speed,
    t2.targetAltitude    as target_altitude, -- 高度
    t2.targetLongitude   as longitude,       -- 经度
    t2.targetLatitude    as latitude,        -- 纬度
    coalesce(t2.distance,radialDistance) as distance,

    -- 雷达无目标类型都给未知｜可见光红外需要null、''、未知目标更改为未知｜ 信火一体的需要'' 改为未知
    if(
                coalesce(t2.objectLabel,t2.targetType) is null
                or coalesce(t2.objectLabel,t2.targetType) = ''
                or coalesce(t2.objectLabel,t2.targetType) = '未知目标',
                '未知',coalesce(t2.objectLabel,t2.targetType)
        ) as object_label,
    proctime

from tmp_source_kafka_02 as t1
         cross join unnest (targets) as t2 (
                                            targetId         ,
                                            xDistance        ,
                                            yDistance        ,
                                            speed            ,
                                            targetPitch      ,
                                            targetAltitude   ,
                                            status           ,
                                            targetLongitude  ,
                                            targetLatitude   ,
                                            distance         ,
                                            targetYaw        ,
                                            utc_time         ,
                                            tracked_times    ,
                                            loss_times       ,
                                            sourceType       ,
                                            targetType       ,

    -- 振动仪数据
                                            objectLabel      ,
                                            targetCredibility,
                                            `time`           ,
    -- 天朗雷达
                                            RCS               ,
                                            radialDistance    ,
                                            targetState       ,
                                            `timestamp`       ,
                                            timestampBase     ,
                                            uploadMode

    );


-- 关联区域，整理数据
create view tmp_source_kafka_04 as
select
    *
from (
         select
             uuid()                           as eventId                   , -- 唯一编号 必填
             uuid()                           as eventNo                   , -- 事件编号 必填
             '无人机入侵' as eventName          , -- 事件名称
             a.device_id                      as deviceId                  , -- 设备id  必填
             device_type_join                 as deviceName                , -- 设备名称
             device_type_join                 as deviceType                , -- 设备类型
             'drone_intrusion'                as eventType                 , -- 事件类型
             'High'                           as `level`                   , -- 防护区等级
             to_timestamp(acquire_timestamp_format,'yyyy-MM-dd HH:mm:ss')         as eventTime                 , -- 事件时间
             cast(null as bigint)             as sourceFrameWidth          , -- 原始帧宽度
             cast(null as bigint)             as sourceFrameHeight         , -- 原始帧高度
             cast(null as string)             as sourceImage               , -- 异常物的大图uri地址
             a.longitude                      as longitude                 , -- 经度
             a.latitude                       as latitude                  , -- 纬度
             target_id                        as targetId                  , -- 目标id
             cast(null as string)             as recordPath,
             speed,
             distance,
             a.device_id                      as radarId,
             cast(target_id as double)        as radarTargetId,
             cast(null as string)             as sourceType,
             target_altitude as altitude,
             array[row(
                     cast(null as string) ,
                     0,
                     0,
                     0,
                     0,
                     cast(null as DECIMAL(20, 18)),
                     array[row(cast(null as string),cast(null as string))]
                 )]                               as objList,
             false                            as reid,
             count(1) over(partition by target_id order by a.proctime) as cnt
         from tmp_source_kafka_03 a
                  left join `overlay` FOR SYSTEM_TIME AS OF a.proctime as b
                            on b.gmt_create_by = 'tianlang-dev'

         where in_polygon(coalesce(a.longitude,0),coalesce(a.latitude,0),overlay_positions)= true
           and object_label='9'

     ) a
where cnt<=2;




-----------------------

-- 数据插入

-----------------------


begin statement set;

-- 望楼、重庆南岸、无人机
insert into event_warn_kafka
select
    uuid()                           as eventId                   , -- 唯一编号 必填
    uuid()                           as eventNo                   , -- 事件编号 必填
    event_name                       as eventName                 , -- 事件名称
    source_id                        as deviceId                  , -- 设备id  必填
    device_name_join                 as deviceName                , -- 设备名称
    device_type_join                 as deviceType                , -- 设备类型
    eventType                        as eventType                 , -- 事件类型
    `level`                         ,                  -- 防护区等级
    ntp_timestamp                    as eventTime                 , -- 事件时间
    source_frame_width               as sourceFrameWidth          , -- 原始帧宽度
    source_frame_height              as sourceFrameHeight         , -- 原始帧高度
    big_image_path                   as sourceImage               , -- 异常物的大图uri地址
    coalesce(longitude,parent_longitude_join)  as longitude       , -- 经度
    coalesce(latitude,parent_latitude_join)    as latitude        , -- 纬度
    cast(object_id as string)        as targetId                  , -- 目标id
    record_path                      as recordPath,
    speed,
    distance,
    radar_id                         as radarId,
    radar_target_id                  as radarTargetId,
    source_type                      as sourceType,
    altitude,
    array[row(
            image_path,
            bbox_left,
            bbox_top,
            bbox_width,
            bbox_height,
            confidence,
            obj_label_list
        )]                               as objList,
    false                            as reid
from tmp_frame_infer_data_04;


-- 天朗雷达数据告警
insert into event_warn_kafka
select
    eventId                   , -- 唯一编号 必填
    eventNo                   , -- 事件编号 必填
    eventName                 , -- 事件名称
    deviceId                  , -- 设备id  必填
    deviceName                , -- 设备名称
    deviceType                , -- 设备类型
    eventType                 , -- 事件类型
    `level`                   , -- 防护区等级
    eventTime                 , -- 事件时间
    sourceFrameWidth          , -- 原始帧宽度
    sourceFrameHeight         , -- 原始帧高度
    sourceImage               , -- 异常物的大图uri地址
    longitude                 , -- 经度
    latitude                  , -- 纬度
    targetId                  , -- 目标id
    recordPath,
    speed,
    distance,
    radarId,
    radarTargetId,
    sourceType,
    altitude,
    objList,
    reid
from tmp_source_kafka_04 ;

end;