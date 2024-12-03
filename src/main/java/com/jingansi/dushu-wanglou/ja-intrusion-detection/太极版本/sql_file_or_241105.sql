--********************************************************************--
-- author:      write your name here
-- create time: 2024/11/4 14:41:49
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ja-intrusion-detection-v2';


-- SET 'parallelism.default' = '2';
SET 'table.exec.state.ttl' = '600000';
SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'sql-client.display.max-column-width' = '100';

-- checkpoint的时间和位置
SET 'execution.checkpointing.interval' = '60000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-intrusion-detection-v2' ;


-- create function block_white_list_flag as 'com.jingan.udf.merge.BlackWhiteCarShipFlagUdf';

create function block_white_list_flag as 'com.jingan.udf.merge.BlackWhiteCarShipFlagUdf';


-----------------------

-- 数据来源写出格式

-----------------------


-- kafka来源的数据（Source：kafka）
drop table  if exists frame_infer_data;
create table frame_infer_data (
                                  batch_id                bigint,                        -- 批处理ID
                                  frame_num               int,                           -- 帧编号
                                  frame_tensor_list       string,                        -- 输出基于帧的特征向量
                                  infer_done              bigint,
                                  ntp_timestamp           bigint,                        -- 时间戳 毫秒
                                  source_frame_width      int,                           -- 原始帧宽度
                                  source_frame_height     int,                           -- 原始帧高度
                                  source_id               string,                        -- 数据源ID
                                  video_infer_done        boolean,
                                  video_path              string,
                                  image_path              string,                       -- 目标图片存储路径
                                  object_list             array <
                                      row(
                                      object_id           bigint,                -- 目标ID
                                      object_label        string,                -- 目标类型大类(Face,Person,MotorVehicle,NonMotorVehicle),这一版本也更改为中文了
                                      object_sub_label    string,                -- 目标类型小类（轿车、卡车、自行车、摩托车...）
                                      class_id            int,
                                      bbox_left           int,                   -- 左上角坐标
                                      bbox_top            int,                   -- 左上角坐标
                                      bbox_width          int,                   -- 目标宽度
                                      bbox_height         int,                   -- 目标高度
                                      confidence          string,        -- 目标置信度
                                      obj_label_list array <
                                      row(
                                      label_value         string,
                                      label_name          string
                                      )
                                      >

                                      )
                                      >
) WITH (
      'connector' = 'kafka',
      'topic' = 'ja-ai-detection-output',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-intrusion-detection-v2',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1730792176000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
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
                                     confidence          string     , -- 置信度
                                     objLabelList        array<
                                     row(
                                     labelValue  string,  -- 属性值
                                     labelName   string
                                     )>
                                     )> ,  -- '检测出的目标对象列表'
                                 primary key (targetId,eventTime) NOT ENFORCED
) with (
      'connector' = 'upsert-kafka',
      'topic' = 'event_warn',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-intrusion-detection-sink',
      'key.format' = 'json',
      'value.format' = 'json'
      );


-- 三方告警
create table open_api_device_event(
                                      `time`              timestamp,
                                      event               string     comment '唯一编号',
                                      data          row(
                                          productKey   string,
                                          deviceId     string,
                                          name         string,
                                          type         string,
                                          data row(
                                          name     string,
                                          message  string
                                          )
                                          ),
                                      primary key (`time`) NOT ENFORCED
) with (
      'connector' = 'upsert-kafka',
      'topic' = 'OPEN_API_DEVICE_EVENT',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-intrusion-detection-sink2',
      'key.format' = 'json',
      'value.format' = 'json'
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
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device'
      );


-- 车辆黑白名单（Source：mysql）
create table car_information_manage (
                                        id                 bigint,     --
                                        car_no             string,  -- 车牌
                                        car_color	       string, -- 车颜色
                                        filter_type	       string, -- 黑白名单
                                        PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/wvp2?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'car_information_manage'
      );



-- 船黑白名单（Source：mysql）
create table sip_information_manage (
                                        id                 bigint,     --
                                        sip_no             string,  -- 船号
                                        sip_type           string,  -- 船类型
                                        filter_type	       string, -- 黑白名单
                                        PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/wvp2?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'sip_information_manage'
      );



---------------

-- 数据处理

---------------

-- 数据展开
create view tmp_frame_infer_data_01 as
select
    a.batch_id,
    a.frame_num,
    to_timestamp(from_unixtime(ntp_timestamp/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as ntp_timestamp,
    a.source_id,          -- 检测设备的id
    a.source_frame_width,
    a.source_frame_height,
    a.video_path,
    a.image_path,

    t.object_id,
    t.object_label,
    t.object_sub_label,
    t.class_id,
    t.bbox_left,
    t.bbox_top,
    t.bbox_width,
    t.bbox_height,
    t.confidence,
    t.obj_label_list,
    PROCTIME() as proctime
from frame_infer_data a
         cross join unnest (object_list) as t (
                                               object_id,
                                               object_label,
                                               object_sub_label,
                                               class_id,
                                               bbox_left,
                                               bbox_top,
                                               bbox_width,
                                               bbox_height,
                                               confidence,
                                               obj_label_list
    );


-- 关联设备的经纬度信息
create view tmp_frame_infer_data_02 as
select
    t1.*,
    t1.obj_label_list[1].label_value as label_value,
    t1.obj_label_list[1].label_name as label_name,
    t2.longitude,
    t2.latitude,
    t2.name as deviceName,
    t2.type
from tmp_frame_infer_data_01 as t1
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t2      -- 关联设备信息表，取出设备的经纬度
                   on t1.source_id = t2.device_id;



-- 过滤车牌、车、船数据
create view tmp_frame_infer_data_03 as
select
    *,
    case
        when object_label = '车牌' and label_name = 'plate_no' then 'plate_no'
        when object_label = '机动车' and label_name = 'vehicle_color' then 'vehicle_color'
        when object_label in ('渔船','帆船','快艇','民船','货轮') then 'ship_no'
        end as define_type,

    'BLACK_LIST' as black_join_flag,
    'WHITE_LIST' as white_join_flag
from tmp_frame_infer_data_02
where object_label in('货轮','机动车','渔船','帆船','快艇','民船','车牌');
--and label_value is not null;


-- 车颜色数据匹配 告警 XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
create view alarm_car_color as
select
    source_id,deviceName,type,ntp_timestamp,source_frame_width,source_frame_height,image_path,longitude,latitude,object_id,
    bbox_left,bbox_top,bbox_width,bbox_height,confidence,obj_label_list,'car' as eventType,'车辆告警' as eventName
from (
         select
             t1.*,
             row_number()over(partition by object_id order by proctime) as rk
         from (select * from tmp_frame_infer_data_03 where define_type = 'vehicle_color') as t1
                  left join car_information_manage FOR SYSTEM_TIME AS OF t1.proctime as t2
                            on t1.label_value = t2.car_color and t1.black_join_flag = t2.filter_type     -- 黑名单

                  left join car_information_manage FOR SYSTEM_TIME AS OF t1.proctime as t3
                            on t1.label_value = t3.car_color and t1.white_join_flag = t3.filter_type     -- 白名单
         where t2.car_color is not null
           and t2.car_color <> ''
           and (t2.car_no = '' or t2.car_no is null)
           and (t3.car_color is null or t3.car_color = '')
           and (t3.car_no = '' or t3.car_no is null)
     ) as tt
where rk =1;



-- 车牌匹配告警 告警 XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
-- 车牌模糊匹配
create view tmp_frame_infer_data_04 as
select
    t1.*,
    block_white_list_flag(label_value,define_type,t2.car_no,t3.car_no,'','','') as alarm_flag   -- 车牌号，类型（车牌还是船牌），黑名单号，白名单号
from (select * from tmp_frame_infer_data_03 where define_type = 'plate_no') as t1

         left join car_information_manage FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 黑名单
                   on t1.black_join_flag = t2.filter_type

         left join car_information_manage FOR SYSTEM_TIME AS OF t1.proctime as t3   -- 白名单
                   on t1.white_join_flag = t3.filter_type
where t2.car_no is not null
  and t2.car_no <> '';



-- 车牌最后一条告警
create view alarm_car_plate_no as
select
    source_id,deviceName,type,ntp_timestamp,source_frame_width,source_frame_height,image_path,longitude,latitude,object_id,
    bbox_left,bbox_top,bbox_width,bbox_height,confidence,obj_label_list,'car' as eventType,'车辆告警' as eventName
from (
         select
             *,
             row_number()over(partition by object_id order by proctime) as rk
         from tmp_frame_infer_data_04
         where alarm_flag is not null
     ) as t1
where rk = 1;



-- 船牌匹配告警 告警 XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
-- 船牌数据模糊匹配
create view tmp_frame_infer_data_05 as
select
    t1.*,
    block_white_list_flag(label_value,define_type,t2.sip_no,t3.sip_no,object_label,t2.sip_type,t3.sip_type) as alarm_flag   -- 车牌号，类型（车牌还是船牌），黑名单号，白名单号
from (select * from tmp_frame_infer_data_03 where define_type = 'ship_no') as t1

         left join sip_information_manage FOR SYSTEM_TIME AS OF t1.proctime as t2  -- 黑名单
                   on t1.black_join_flag = t2.filter_type
    -- and t1.object_label = t2.sip_type

         left join sip_information_manage FOR SYSTEM_TIME AS OF t1.proctime as t3  -- 白名单
                   on t1.white_join_flag = t3.filter_type ;
-- and t1.object_label = t3.sip_type

-- where t2.sip_no is not null
--   and t2.sip_no <> '';


create view alarm_ship_no as
select
    source_id,deviceName,type,ntp_timestamp,source_frame_width,source_frame_height,image_path,longitude,latitude,object_id,
    bbox_left,bbox_top,bbox_width,bbox_height,confidence,obj_label_list,'unusual_ship_near' as eventType,concat(object_label,'船舶告警') as eventName
from (
         select
             *,
             row_number()over(partition by object_id order by proctime) as rk
         from tmp_frame_infer_data_05
         where alarm_flag is not null
     ) as t1
where rk = 1;



create view alarm_all_sink as
select
    source_id,deviceName,type,ntp_timestamp,source_frame_width,source_frame_height,image_path,longitude,latitude,object_id,
    bbox_left,bbox_top,bbox_width,bbox_height,confidence,obj_label_list,'person' as eventType,'人员告警' as eventName,'检测到目标进入指定区域' as temp
from tmp_frame_infer_data_02 where object_label = '人员'
union all
select *,'检测到目标进入指定区域' as temp from alarm_car_color
union all
select *,'检测到目标进入指定区域' as temp from alarm_car_plate_no
union all
select *,'检测到目标进入指定区域' as temp from alarm_ship_no;


-----------------------

-- 数据插入

-----------------------

begin statement set;

-- 告警数据输出event_warn
insert into event_warn_kafka
select
    uuid()              as eventId,
    uuid()              as eventNo,
    eventName,
    source_id           as deviceId,
    deviceName,
    type,
    eventType,
    'High'               as `level`,
    ntp_timestamp        as eventTime,
    source_frame_width   as sourceFrameWidth,
    source_frame_height  as sourceFrameHeight,
    image_path           as sourceImage,
    longitude,
    latitude,
    cast(object_id as string)    as targetId,
    array[row(
            image_path,
            bbox_left,
            bbox_top,
            bbox_width,
            bbox_height,
            confidence,
            obj_label_list
        )
        ] as objList
from alarm_all_sink;



-- 告警数据输出三方
insert into open_api_device_event
select
    ntp_timestamp     as `time`,
    'DEVICE_EVENT'    as event,
    row(
            '',
            source_id,
            'event.controlAlarm.warning',
            'INFO',
            row (
                    eventName,
                    temp
                )
        ) as `data`
from alarm_all_sink;

end;


-- 人员告警
-- insert into event_warn_kafka
-- select
--   uuid()              as eventId,
--   uuid()              as eventNo,
--   '人员告警'           as eventName,
--   source_id           as deviceId,
--   deviceName,
--   type,
--   'person'             as eventType,
--   'High'               as `level`,
--   ntp_timestamp        as eventTime,
--   source_frame_width   as sourceFrameWidth,
--   source_frame_height  as sourceFrameHeight,
--   image_path           as sourceImage,
--   longitude,
--   latitude,
--   cast(object_id as string)    as targetId,
--   array[row(
--         image_path,
--         bbox_left,
--         bbox_top,
--         bbox_width,
--         bbox_height,
--         confidence,
--         obj_label_list
--    )
--   ] as objList

-- from tmp_frame_infer_data_02
-- where object_label = '人员';


-- insert into event_warn_kafka
--   select
--   uuid()              as eventId,
--   uuid()              as eventNo,
--   eventName,
--   source_id           as deviceId,
--   deviceName,
--   type,
--   eventType,
--   'High'               as `level`,
--   ntp_timestamp        as eventTime,
--   source_frame_width   as sourceFrameWidth,
--   source_frame_height  as sourceFrameHeight,
--   image_path           as sourceImage,
--   longitude,
--   latitude,
--   cast(object_id as string)    as targetId,
--   array[row(
--         image_path,
--         bbox_left,
--         bbox_top,
--         bbox_width,
--         bbox_height,
--         confidence,
--         obj_label_list
--    )
--   ] as objList
--  from alarm_car_color;



-- insert into event_warn_kafka
--   select
--   uuid()              as eventId,
--   uuid()              as eventNo,
--   eventName,
--   source_id           as deviceId,
--   deviceName,
--   type,
--   eventType,
--   'High'               as `level`,
--   ntp_timestamp        as eventTime,
--   source_frame_width   as sourceFrameWidth,
--   source_frame_height  as sourceFrameHeight,
--   image_path           as sourceImage,
--   longitude,
--   latitude,
--   cast(object_id as string)    as targetId,
--   array[row(
--         image_path,
--         bbox_left,
--         bbox_top,
--         bbox_width,
--         bbox_height,
--         confidence,
--         obj_label_list
--    )
--   ] as objList
--  from alarm_car_plate_no;



-- insert into event_warn_kafka
--   select
--   uuid()              as eventId,
--   uuid()              as eventNo,
--   eventName,
--   source_id           as deviceId,
--   deviceName,
--   type,
--   eventType,
--   'High'               as `level`,
--   ntp_timestamp        as eventTime,
--   source_frame_width   as sourceFrameWidth,
--   source_frame_height  as sourceFrameHeight,
--   image_path           as sourceImage,
--   longitude,
--   latitude,
--   cast(object_id as string)    as targetId,
--   array[row(
--         image_path,
--         bbox_left,
--         bbox_top,
--         bbox_width,
--         bbox_height,
--         confidence,
--         obj_label_list
--    )
--   ] as objList
--  from alarm_ship_no;








-- insert into event_warn_kafka
-- select
--   uuid()              as eventId,
--   uuid()              as eventNo,
--   eventName,
--   source_id           as deviceId,
--   deviceName,
--   type,
--   eventType,
--   'High'               as `level`,
--   ntp_timestamp        as eventTime,
--   source_frame_width   as sourceFrameWidth,
--   source_frame_height  as sourceFrameHeight,
--   image_path           as sourceImage,
--   longitude,
--   latitude,
--   cast(object_id as string)    as targetId,
--   array[row(
--         image_path,
--         bbox_left,
--         bbox_top,
--         bbox_width,
--         bbox_height,
--         confidence,
--         obj_label_list
--    )
--   ] as objList
-- from (
-- select * from alarm_car_color
--   union all
-- select * from alarm_car_plate_no
--   union all
-- select * from alarm_ship_no
--  ) as tt;





