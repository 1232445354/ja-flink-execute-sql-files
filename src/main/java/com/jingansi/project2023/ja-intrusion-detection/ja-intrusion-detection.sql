--********************************************************************--
-- author:     yibo@jingan-inc.com
-- create time: 2022/12/15 16:28:19
-- description: 重庆南岸项目的入侵检测
--********************************************************************--

set 'pipeline.name' = 'ja-intrusion-detection';

SET 'parallelism.default' = '2';
SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'sql-client.display.max-column-width' = '100';

-- checkpoint的时间和位置
-- SET 'execution.checkpointing.interval' = '60000';
-- SET 'state.checkpoints.dir' = 's3://flink/flink-intrusion-detection-checkpoints' ;

create function rectangle_intersect_polygon as 'RectangleIntersectPolygon';
create function merge_plate_no as 'MergePlateNo';


-- kafka来源的数据（Source：kafka）
drop table  if exists frame_infer_data;
create table frame_infer_data (
                                  batch_id                bigint,                        -- 批处理ID
                                  frame_num               int,                           -- 帧编号
                                  pts                     bigint,                        -- pts 值
                                  ntp_timestamp           bigint,                        -- 时间戳
                                  source_id               string,                        -- 数据源ID
                                  source_frame_width      int,                           -- 原始帧宽度
                                  source_frame_height     int,                           -- 原始帧高度
                                  infer_done              boolean,
                                  image_path              string,                        -- 大图图片存储路径
    --frame_tensor_list     array<string>,
    --obj_label_list        array<string>,
                                  frame_tensor_list       string,                        -- 输出基于帧的特征向量 ，一般null
                                  object_list             array<
                                      row(
                                      object_id           bigint,                -- 目标ID
                                      object_label        string,                -- 目标类型大类(Face,Person,MotorVehicle,NonMotorVehicle)
                                      object_sub_label    string,                -- 目标类型小类（轿车、卡车、自行车、摩托车，车牌...）
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
                                      )
                                      >
                                      )
                                      >
                                      )
                                      >
) WITH (
      'connector' = 'kafka',
      'topic' = 'infer_test',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-intrusion-detection',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 框选监控区域表（Source：mysql）
create table video_area (
                            id                             bigint        comment '主键',
                            device_id                      string        comment '设备id',
                            points                         string        comment '点位列表数组(json)',
                            type                           string        comment '框的用途类型',
                            gmt_create                     timestamp     comment '创建时间',
                            gmt_modified                   timestamp     comment '修改时间',
                            gmt_create_by                  string        comment '创建者',
                            gmt_modified_by                string        comment '修改者',
                            deleted                        tinyint       comment '是否删除,0:未删除,1:已删除',
                            PRIMARY KEY (device_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'video_area',
      'lookup.cache.ttl' = '3s',
      'lookup.cache.max-rows' = '1000'
      );


-- 事件告警数据表（Sink：kafka）
drop table if exists event_warn_kafka;
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
                                     )>
                                     )>                          , -- '检测出的目标对象列表'
                                 reid                boolean     ,  -- '是否为reid告警,true:是。false:否'
                                 primary key (targetId,eventTime) NOT ENFORCED
) with (
      'connector' = 'upsert-kafka',
      'topic' = 'event_warn',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-intrusion-detection',
      'key.format' = 'json',
      'value.format' = 'json'
      );


-- 设备表（Source：mysql）
create table device (
                        id                             int           comment '',
                        device_id                      string        comment '设备编码',
                        name                           string        comment '设备名称',
                        username                       string        comment '最近登陆的用户名',
                        manufacturer                   string        comment '设备厂商',
                        model                          string        comment '设备型号',
                        type                           string        comment '设备类型',
                        firmware                       string        comment '主控版本',
                        is_valid                       string        comment 'YES 有效，NO 无效',
                        transport                      string        comment '传输协议：UDP、TCP',
                        update_version                 int           comment '数据版本',
                        status                         string        comment '是否在线',
                        register_time                  timestamp     comment '注册时间',
                        ip                             string        comment '设备ip地址',
                        port                           int           comment '设备端口',
                        expires                        int           comment '过期时间',
                        remaining_power                int           comment '电量',
                        register_protocol              string        comment '注册设备时所用的协议',
                        owner                          string        comment '所属人',
                        sn                             string        comment '唯一序列号',
                        longitude                      decimal(12,8) comment '经度',
                        latitude                       decimal(12,8) comment '纬度',
                        fuselage_picture               string        comment '设备照片',
                        task_status                    string        comment '任务状态',
                        altitude                       double        comment '高度',
                        parent_device_id               string        comment '父设备',
                        device_level                   int           comment '设备层级',
                        space_id                       string        comment '地图ID',
                        is_virtual                     string        comment '是否为虚拟设备',
                        gmt_create                     timestamp     comment '',
                        gmt_modified                   timestamp     comment '',
                        gmt_create_by                  string        comment '',
                        gmt_modified_by                string        comment '',
                        is_ptz                         string        comment '是否ptz控制',
                        parent_id                      string        comment '临时字段',
                        PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device'
      );


---------------

-- 数据处理

---------------

-- 数据展开并将数据传入自定义函数中
drop view if exists tmp_frame_infer_data_01;
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
    a.image_path as full_image_path,
    a.frame_tensor_list,
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
    t.obj_tensor_list,
    PROCTIME() as proctime
from  (
          select *,merge_plate_no(object_list) as object_list1  from frame_infer_data
      ) a
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
                                                 obj_label_list,
                                                 obj_track_list,
                                                 obj_tensor_list
    );


-- 判断布控摄像头，并且出现在画框的区域,过滤数据
drop view if exists tmp_frame_infer_data_02;
create view tmp_frame_infer_data_02 as
select
    tt.*
from (
         select
             a.*,
             b.device_id,
             rectangle_intersect_polygon(a.bbox_left,a.bbox_top,a.bbox_width,a.bbox_height,b.points) as flag
         from tmp_frame_infer_data_01 a
                  left join video_area FOR SYSTEM_TIME AS OF a.proctime as b
                            on a.source_id = b.device_id
     ) as tt
where (flag = true and device_id is not null)    -- 人员和车都在区域内的
   or device_id is null;


-- 根据类型进行不同告警
drop view if exists tmp_frame_infer_data_03;
create view tmp_frame_infer_data_03 as
select
    *,
    case
        when object_label='Person' and flag = true then 'climbing'             -- device_id不为空 说明关联上video_area flag为true说明在区域内   攀爬告警
        when object_label='Person' and device_id is null then 'person'         -- 人员不在区域内 人员告警
        when object_label in ('MotorVehicle','NonMotorVehicle') then 'car'     -- 车辆告警
        when object_label='Abnormal' and object_sub_label='Abnormal'  then 'abnormal_detection'     -- 车辆告警
    -- when object_label in ('MotorVehicle','NonMotorVehicle') and object_sub_label <> 'license_plate' then 'car'     -- 车辆告警
        end as eventType
    -- count(1) over (partition by object_id order by proctime) as cnt
from tmp_frame_infer_data_02
where object_sub_label <> 'license_plate';



-----------------------

-- 数据插入

-----------------------
insert into event_warn_kafka
select
    uuid()                           as eventId                   , -- 唯一编号 必填
    uuid()                           as eventNo                   , -- 事件编号 必填
    case eventType
        when 'climbing' then '攀爬告警'
        when 'person'   then '人员告警'
        when 'car'      then '车辆告警'
        when 'abnormal_detection' then '异常检测'
        end                              as eventName                 , -- 事件名称
    source_id                        as deviceId                  , -- 设备id  必填
    b.name                           as deviceName                , -- 设备名称
    b.type                           as deviceType                , -- 设备类型
    eventType                        as eventType                 , -- 事件类型
    'High'                           as `level`                   , -- 防护区等级
    ntp_timestamp                    as eventTime                 , -- 事件时间
    source_frame_width               as sourceFrameWidth          , -- 原始帧宽度
    source_frame_height              as sourceFrameHeight         , -- 原始帧高度
    full_image_path                  as sourceImage               , -- 异常物的大图uri地址
    if(a.longitude is not null,a.longitude,b.longitude) as longitude                 , -- 经度
    if(a.latitude is not null,a.latitude,b.latitude)    as latitude                  , -- 纬度
    cast(object_id as string)        as targetId                  , -- 目标id
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
from tmp_frame_infer_data_03 a
         -- (select * from tmp_frame_infer_data_03 where cnt % 5 = 1) a
         left join device FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.source_id = b.device_id;







