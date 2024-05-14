--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2023/10/27 15:19:11
-- description: 警航程序
--********************************************************************--

set 'pipeline.name' = 'ja-jinghang-rt';


set 'parallelism.default' = '1';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '600000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

set 'table.exec.sink.not-null-enforcer'='drop';



-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-jinghang-rt';


 -----------------------

 -- 数据结构

 -----------------------

-- 创建kafka全量数据表（Source：kafka）
drop table if exists temp01_kafka;
create table temp01_kafka(
                             action_id               bigint,   -- 行动id
                             action_item_id          bigint,   -- 行动子任务id
                             action_record_id        bigint,   -- 行动记录id
                             action_item_record_id   bigint,   -- 行动子任务记录id
                             device_id               string,   -- 设备id,机器狗、监控、无人机编号
                             device_name             string,   -- 设备名称
                             longitude               string,   -- 经度
                             latitude                string,   -- 维度
                             batch_id                bigint,   --
                             frame_num               bigint,   --
                             frame_tensor_list       string,   --
                             image_path              string,   -- 原图
                             image_draw_path         string,   -- 画框大图
                             infer_done              bigint,   --
                             ntp_timestamp           bigint,   --
                             pts                     bigint,   --
                             source_frame_height     int,   -- 目标分辨率宽
                             source_frame_width      int,   -- 目标分辨率高
                             source_id               string,   --
                             video_infer_done        boolean,  -- 是否全部检测完成
                             video_path              string,
                             object_list array<
                                 row<
                                 bbox_height          int,   -- 目标宽度
                             bbox_width           int,   -- 目标高度
                             bbox_left            int,   --
                             bbox_top             int,   --
                             class_id             bigint,   --
                             confidence           double,   -- 置信度
                             object_id            bigint,   -- 目标唯一标识
                             image_path           string,   --
                             infer_id             bigint,   --
                             video_time           bigint,   -- 目标出现时间unit:ms
                             object_sub_label     string,   --
                             object_label         string,   -- 目标标签
                             obj_tensor_list      string,
                             obj_track_list       string,
                             obj_label_list array<
                                 row<
                                 label_value          string,
                             infer_id             bigint,
                             label_name           string,  -- plate_no：车牌，plate_color：颜色
                             label_id             bigint,
                             label_confidence     double
                                 >
                                 >

                                 >
                                 >


) with (
      'connector' = 'kafka',
      'topic' = 'jinghang_detection_result',
      -- 'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
      'properties.group.id' = 'test-group',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 设备表（Source：mysql）
drop table if exists device;
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
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device'
      );


-- 创建写入mysql的数据表（Sink：mysql）
drop table if exists temporary_detect_result;
create table temporary_detect_result (
                                         action_id                bigint,  -- 行动id
                                         action_item_id           bigint,  -- 行动子任务id
                                         action_record_id         bigint,  -- 行动记录id
                                         action_item_record_id    bigint,  -- 行动子任务记录id
                                         plate_no                 string,  -- 车牌号码
                                         plate_color              string,  -- 车牌颜色
                                         source                   string,  -- 事件来源,机器狗，监控，无人机名称
                                         longitude                double,  -- 经度
                                         latitude                 double,  -- 纬度
                                         result_type              string,  -- 事件类型,anomaly：异常物dangerous：危险物
                                         level                    string,  -- 告警等级:High:紧急,Middle:一般,Low:不紧急
                                         device_id                string,  -- 设备id,机器狗、监控、无人机编号
                                         result_time              string,  -- 事件时间
                                         image                    string,  -- 异常物图片url
                                         source_image             string,  -- 异常物大图url
                                         left_top_x               int,     -- 左上角X坐标
                                         left_top_y               int,     -- 左上角Y坐标
                                         right_btm_x              int,     -- 右下角X坐标
                                         right_btm_y              int,     -- 右下角Y坐标
                                         bbox_width               int,     -- 目标宽度
                                         bbox_height              int,     -- 目标高度
                                         source_frame_width       int,     -- 原始图宽度
                                         source_frame_height      int,     -- 原始图高度
                                         confidence               double,  -- 置信度0~1
                                         device_type              string,  -- 设备类型
                                         device_name              string  -- 设备名称

    -- PRIMARY KEY (device_id,result_time,plate_no) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'temporary_detect_result'
      );


-----------------------

-- 数据处理

-----------------------



-- 计算h3、时间
drop view if exists tmp_table01;
create view tmp_table01 as
select
    a.action_id,
    a.action_item_id,
    a.device_id,
    a.action_record_id,
    a.action_item_record_id,
    a.device_name,
    a.longitude,
    a.latitude,
    a.image_path as out_image_path,
    a.ntp_timestamp,
    a.source_frame_height,
    a.source_frame_width,
    from_unixtime(a.ntp_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as result_time,
    b.bbox_height,
    b.bbox_width,
    b.bbox_left,
    b.bbox_top,
    b.class_id,
    b.confidence,
    b.image_path as in_image_path,
    b.object_label,
    b.obj_label_list[1].label_name as label_name1,
    b.obj_label_list[1].label_value as label_value1,
    b.obj_label_list[2].label_name as label_name2,
    b.obj_label_list[2].label_value as label_value2,
    PROCTIME()  as proctime     -- 维表关联的时间函数
    -- a.batch_id,
    -- a.frame_num,
    -- a.frame_tensor_list,
    -- a.image_draw_path,
    -- a.infer_done,
    -- a.pts,
    -- a.source_id,
    -- a.video_infer_done,
    -- a.video_path,
    -- b.infer_id,
    -- b.video_time,
    -- b.object_sub_label,
    -- b.object_id,
    -- b.obj_tensor_list,
    -- b.obj_track_list,
from temp01_kafka as a
         left join unnest (object_list) as b (
                                              bbox_height         ,
                                              bbox_width          ,
                                              bbox_left           ,
                                              bbox_top            ,
                                              class_id            ,
                                              confidence          ,
                                              object_id           ,
                                              image_path          ,
                                              infer_id            ,
                                              video_time          ,
                                              object_sub_label    ,
                                              object_label        ,
                                              obj_tensor_list     ,
                                              obj_track_list      ,
                                              obj_label_list
    ) on true;



-----------------------

-- 数据插入

-----------------------

-- begin statement set;


insert into temporary_detect_result
select
    c.action_id                ,
    c.action_item_id           ,
    c.action_record_id         ,
    c.action_item_record_id    ,
    if(label_name1 = 'plate_no',c.label_value1,c.label_value2) as plate_no,
    if(label_name1 = 'plate_color',c.label_value1,c.label_value2) as plate_color,
    c.device_name as source    ,
    cast(c.longitude as double) as longitude,
    cast(c.latitude as double) as latitude,
    if(c.object_label = 'Person','person','car') as result_type,
    'High' as `level`          ,
    c.device_id                ,
    c.result_time              ,
    c.in_image_path as image   ,
    c.out_image_path as source_image,
    c.bbox_left as left_top_x,
    c.bbox_top as left_top_y,
    c.bbox_left + c.bbox_width as right_btm_x,
    c.bbox_top - c.bbox_height as right_btm_y,
    c.bbox_width               ,
    c.bbox_height              ,
    c.source_frame_width       ,
    c.source_frame_height      ,
    c.confidence               ,
    d.type as device_type      ,
    c.device_name
from tmp_table01 as c
         left join device
    FOR SYSTEM_TIME AS OF c.proctime as d
                   on c.device_id = d.device_id;


-- end;




