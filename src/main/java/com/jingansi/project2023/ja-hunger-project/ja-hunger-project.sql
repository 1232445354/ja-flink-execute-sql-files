--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2023/2/7 14:21:36
-- description: 饿了么数据存储、外部和内部
--********************************************************************--

SET 'pipeline.name' = 'ja-hunger-project';

SET 'parallelism.default' = '6';
set 'table.exec.state.ttl' = '300000';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'sql-client.display.max-column-width' = '100';

-- -- checkpoint的时间和位置
SET 'execution.checkpointing.interval' = '60000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-chenkpoints/ja-hunger-project-checkpoints' ;


-- kafka来源的数据给外部饿了么测试的（Source：kafka）
drop table if exists frame_infer_data_external_test;
create table frame_infer_data_external_test (
                                                batch_id                bigint,                        -- 批处理ID
                                                frame_num               int,                           -- 帧编号
                                                pts                     bigint,                        -- pts 值
                                                ntp_timestamp           bigint,                        -- 时间戳
                                                source_id               string,                        -- 数据源ID
                                                source_frame_width      int,                           -- 原始帧宽度
                                                source_frame_height     int,                           -- 原始帧高度
                                                infer_done              boolean,
                                                image_path              string,                        -- 大图图片存储路径
                                                frame_tensor_list       string,                        -- 输出基于帧的特征向量
                                                object_list             array<
                                                    row(
                                                    object_id           bigint,                -- 目标ID
                                                    object_label        string,                -- 目标类型(cat,dog,mouse,smoke,naked)
                                                    infer_id            int,                   -- 推理算子ID
                                                    class_id            int,
                                                    bbox_left           int,                   -- 左上角坐标
                                                    bbox_top            int,                   -- 左上角坐标
                                                    bbox_width          int,                   -- 目标宽度
                                                    bbox_height         int,                   -- 目标高度
                                                    confidence          decimal(20,18),        -- 目标置信度
                                                    image_path          string                 -- 检测目标截取图片存储地址
                                                    )
                                                    >,
                                                user_meta
                                                                        row(
                                                    dateTime             string,
                                                    id                   string,
                                                    imageUrl             string,
                                                    name                 string
                                                    ),
                                                rowtime as to_timestamp_ltz(ntp_timestamp,3),
                                                watermark for rowtime as rowtime - interval '5' second
) WITH (
      'connector' = 'kafka',
      'topic' =  'test_infer_result',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'test-infer-result-rt',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1701356400000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      -- 'flink.kafka.poll-timeout' = '600000',
      -- 'flink.kafka.consumer.properties.session.timeout.ms' = '600000'
      );



-- kafka来源的数据靖安内部测试的（Source：kafka）
drop table if exists frame_infer_data_internal_test;
create table frame_infer_data_internal_test (
                                                batch_id                bigint,                        -- 批处理ID
                                                frame_num               int,                           -- 帧编号
                                                pts                     bigint,                        -- pts 值
                                                ntp_timestamp           bigint,                        -- 时间戳
                                                source_id               string,                        -- 数据源ID
                                                source_frame_width      int,                           -- 原始帧宽度
                                                source_frame_height     int,                           -- 原始帧高度
                                                infer_done              boolean,
                                                image_path              string,                        -- 大图图片存储路径
                                                frame_tensor_list       string,                        -- 输出基于帧的特征向量
                                                object_list             array<
                                                    row(
                                                    object_id           bigint,                -- 目标ID
                                                    object_label        string,                -- 目标类型(cat,dog,mouse,smoke,naked)
                                                    infer_id            int,                   -- 推理算子ID
                                                    class_id            int,
                                                    bbox_left           int,                   -- 左上角坐标
                                                    bbox_top            int,                   -- 左上角坐标
                                                    bbox_width          int,                   -- 目标宽度
                                                    bbox_height         int,                   -- 目标高度
                                                    confidence          decimal(20,18),        -- 目标置信度
                                                    image_path          string                 -- 检测目标截取图片存储地址
                                                    )
                                                    >,
                                                user_meta
                                                                        row(
                                                    dateTime             string,
                                                    id                   string,
                                                    imageUrl             string,
                                                    name                 string
                                                    )
) WITH (
      'connector' = 'kafka',
      'topic' =  'test_infer_result2',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'test-infer-result-rt2',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1701356400000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 饿了吗的告警数据写入kafka（Sink：kafka）
drop table if exists hunger_alarm_result_external_kafka;
create table hunger_alarm_result_external_kafka (
                                                    uuid                    string,                        -- UUID
                                                    batch_id                bigint,                        -- 批处理ID
                                                    frame_num               int,                           -- 帧编号
                                                    pts                     bigint,                        -- pts 值
                                                    ntp_timestamp           bigint,                        -- 时间戳
                                                    source_id               string,                        -- 数据源ID
                                                    source_frame_width      int,                           -- 原始帧宽度
                                                    source_frame_height     int,                           -- 原始帧高度
                                                    infer_done              boolean,
                                                    image_path              string,                        -- 大图图片存储路径
                                                    frame_tensor_list       string,                        -- 输出基于帧的特征向量
                                                    object_list             array<
                                                        row(
                                                        object_id           bigint,                -- 目标ID
                                                        object_label        string,                -- 目标类型(cat,dog,mouse,smoke,naked)
                                                        infer_id            int,                   -- 推理算子ID
                                                        class_id            int,                   -- 检测目标的类型
                                                        bbox_left           int,                   -- 左上角坐标
                                                        bbox_top            int,                   -- 左上角坐标
                                                        bbox_width          int,                   -- 目标宽度
                                                        bbox_height         int,                   -- 目标高度
                                                        confidence          decimal(20,18),        -- 目标置信度
                                                        image_path          string                 -- 检测目标截取图片存储地址
                                                        )
                                                        >,
                                                    user_meta
                                                                            row(
                                                        dateTime             string,
                                                        id                   string,
                                                        imageUrl             string,
                                                        name                 string
                                                        ),
                                                    primary key (uuid,ntp_timestamp) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' =  'hunger_alarm_result',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'hunger-alarm-result',
      'key.format' = 'json',
      'value.format' = 'json'
      -- 'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1676428244000',
      -- 'json.fail-on-missing-field' = 'false',
      -- 'json.ignore-parse-errors' = 'true'
      );



-- Doris写入的数据给定外部饿了么测试的（Sink：doris）
drop table if exists dwd_hunger_all_rt_doris_external;
create table dwd_hunger_all_rt_doris_external (
                                                  pts                     bigint      	comment 'pts 值',
                                                  ntp_timestamp           bigint      	comment '时间戳',
                                                  ntp_timestamp_format    string          comment '时间戳格式化',
                                                  batch_id                bigint      	comment '批处理ID',
                                                  frame_num               int         	comment '帧编号',
                                                  source_id               string          comment '数据源ID',
                                                  source_frame_width      int         	comment '原始帧宽度',
                                                  source_frame_height     int         	comment '原始帧高度',
                                                  infer_done              boolean		    comment '',
                                                  full_image_path         string          comment '大图图片存储路径',
                                                  frame_tensor_list       string          comment '输出基于帧的特征向量',
                                                  object_id               bigint          comment '目标ID',
                                                  object_label            string          comment '目标类型(cat,dog,mouse,smoke,naked)',
                                                  infer_id                int             comment '推理算子ID',
                                                  class_id                int             comment '',
                                                  bbox_left               int             comment '左上角坐标',
                                                  bbox_top                int             comment '左上角坐标',
                                                  bbox_width              int             comment '目标宽度',
                                                  bbox_height             int             comment '目标高度',
                                                  confidence              string          comment '目标置信度',
                                                  image_path              string          comment '检测目标截取图片存储地址',
                                                  user_meta_dateTime      string          comment '',
                                                  user_meta_id			string          comment '',
                                                  user_meta_imageUrl      string          comment '',
                                                  user_meta_name          string          comment '',
                                                  create_by     			string          comment '创建人',
                                                  create_on               string          comment '创建时间'
)with (
     'connector' = 'doris',
     'fenodes' = '172.27.95.211:30030',
     'table.identifier' = 'hunger.dwd_hunger_all_rt',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='12s'
-- 'sink.properties.recover_with_empty_tablet' = 'true'
-- 'sink.label-prefix' = 'doris_label11',
-- 'sink.enable-2pc' = 'false',
-- 'sink.properties.format' = 'json',
-- 'sink.properties.read_json_by_line' = 'true'
     );



-- Doris写入的数据给定外部告警的（Sink：doris）
drop table if exists dwd_hunger_alarm_all_rt_external_sink;
create table dwd_hunger_alarm_all_rt_external_sink (
                                                       pts                     bigint      	comment 'pts 值',
                                                       ntp_timestamp           bigint      	comment '时间戳',
                                                       ntp_timestamp_format    string          comment '时间戳格式化',
                                                       batch_id                bigint      	comment '批处理ID',
                                                       frame_num               int         	comment '帧编号',
                                                       source_id               string          comment '数据源ID',
                                                       source_frame_width      int         	comment '原始帧宽度',
                                                       source_frame_height     int         	comment '原始帧高度',
                                                       infer_done              boolean		    comment '',
                                                       full_image_path         string          comment '大图图片存储路径',
                                                       frame_tensor_list       string          comment '输出基于帧的特征向量',
                                                       object_id               bigint          comment '目标ID',
                                                       object_label            string          comment '目标类型(cat,dog,mouse,smoke,naked)',
                                                       infer_id                int             comment '推理算子ID',
                                                       class_id                int             comment '',
                                                       bbox_left               int             comment '左上角坐标',
                                                       bbox_top                int             comment '左上角坐标',
                                                       bbox_width              int             comment '目标宽度',
                                                       bbox_height             int             comment '目标高度',
                                                       confidence              string          comment '目标置信度',
                                                       image_path              string          comment '检测目标截取图片存储地址',
                                                       user_meta_dateTime      string          comment '',
                                                       user_meta_id			string          comment '',
                                                       user_meta_imageUrl      string          comment '',
                                                       user_meta_name          string          comment '',
                                                       create_by     			string          comment '创建人',
                                                       create_on               string          comment '创建时间'
)with (
     'connector' = 'doris',
     'fenodes' = '172.27.95.211:30030',
     'table.identifier' = 'hunger.dwd_hunger_alarm_all_rt',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='12s'
-- 'sink.properties.recover_with_empty_tablet' = 'true'

     );


-- Doris写入的数据给定外部告警的（Source：doris）
drop table if exists dwd_hunger_alarm_all_rt_external_source;
create table dwd_hunger_alarm_all_rt_external_source (
                                                         pts                     bigint      	  comment 'pts 值',
                                                         ntp_timestamp           bigint      	  comment '时间戳',
                                                         class_id                int             comment '检测物ID',
                                                         bbox_left               int             comment '左上角坐标',
                                                         bbox_top                int             comment '左上角坐标',
                                                         bbox_width              int             comment '目标宽度',
                                                         bbox_height             int             comment '目标高度',
                                                         user_meta_id			      string          comment '摄像头ID',
                                                         primary key (class_id,bbox_left,bbox_top,bbox_width,bbox_height,user_meta_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/hunger?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dwd_hunger_alarm_all_rt',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '2000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '1'
      );


-- Doris写入的数据给定靖安内部测试的（Sink：doris）
drop table if exists dwd_hunger_all_rt_doris_internal;
create table dwd_hunger_all_rt_doris_internal (
                                                  pts                     bigint      	comment 'pts 值',
                                                  ntp_timestamp           bigint      	comment '时间戳',
                                                  ntp_timestamp_format    string          comment '时间戳格式化',
                                                  batch_id                bigint      	comment '批处理ID',
                                                  frame_num               int         	comment '帧编号',
                                                  source_id               string          comment '数据源ID',
                                                  source_frame_width      int         	comment '原始帧宽度',
                                                  source_frame_height     int         	comment '原始帧高度',
                                                  infer_done              boolean		    comment '',
                                                  full_image_path         string          comment '大图图片存储路径',
                                                  frame_tensor_list       string          comment '输出基于帧的特征向量',
                                                  object_id               bigint          comment '目标ID',
                                                  object_label            string          comment '目标类型(cat,dog,mouse,smoke,naked)',
                                                  infer_id                int             comment '推理算子ID',
                                                  class_id                int             comment '',
                                                  bbox_left               int             comment '左上角坐标',
                                                  bbox_top                int             comment '左上角坐标',
                                                  bbox_width              int             comment '目标宽度',
                                                  bbox_height             int             comment '目标高度',
                                                  confidence              string          comment '目标置信度',
                                                  image_path              string          comment '检测目标截取图片存储地址',
                                                  user_meta_dateTime      string          comment '',
                                                  user_meta_id			string          comment '',
                                                  user_meta_imageUrl      string          comment '',
                                                  user_meta_name          string          comment '',
                                                  create_by     			string          comment '创建人',
                                                  create_on               string          comment '创建时间'
)with (
     'connector' = 'doris',
     'fenodes' = '172.27.95.211:30030',
     'table.identifier' = 'hunger.dwd_hunger_internal_all_rt',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='12s'
-- 'sink.properties.recover_with_empty_tablet' = 'true'
-- 'sink.label-prefix' = 'doris_label11',
-- 'sink.enable-2pc' = 'false',
-- 'sink.properties.format' = 'json',
-- 'sink.properties.read_json_by_line' = 'true'
     );


---------------

-- 数据处理

---------------

-- 数据展开kafka-topic1给定外部饿了么的数据
drop view if exists tmp_frame_infer_data_external_01;
create view tmp_frame_infer_data_external_01 as
select
    a.batch_id,
    a.frame_num,
    a.pts,
    a.ntp_timestamp,
    from_unixtime(a.ntp_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as ntp_timestamp_format, -- 时间戳格式化
    a.source_id,
    a.source_frame_width,
    a.source_frame_height,
    a.infer_done,
    a.image_path      as full_image_path,
    a.frame_tensor_list,
    a.user_meta.`dateTime`,
    a.user_meta.id,
    a.user_meta.imageUrl,
    a.user_meta.`name`,
    t.object_id,
    t.object_label,
    t.infer_id,
    t.class_id,
    t.bbox_left,
    t.bbox_top,
    t.bbox_width,
    t.bbox_height,
    t.confidence,
    t.image_path
from  frame_infer_data_external_test a
          cross join unnest (object_list) as t (
                                                object_id,
                                                object_label,
                                                infer_id,
                                                class_id,
                                                bbox_left,
                                                bbox_top,
                                                bbox_width,
                                                bbox_height,
                                                confidence,
                                                image_path
    )
where a.ntp_timestamp is not null;


-- 数据展开kafka-topic1给定外部饿了么的数据进行告警筛选
drop view if exists tmp_frame_infer_data_external_02;
create view tmp_frame_infer_data_external_02 as
select
    *,
    PROCTIME() as proctime      -- 维表关联的时间函数
from(
        select
            batch_id,
            frame_num,
            pts,
            ntp_timestamp,
            ntp_timestamp_format,
            source_id,
            source_frame_width,
            source_frame_height,
            infer_done,
            full_image_path,
            frame_tensor_list,
            `dateTime`,
            id,
            imageUrl,
            `name`,
            object_id,
            object_label,
            infer_id,
            class_id,
            bbox_left,
            bbox_top,
            bbox_width,
            bbox_height,
            confidence,
            image_path,
            row_number() over(partition by id,bbox_left,bbox_top,bbox_width,bbox_height,class_id order by ntp_timestamp) as rk
        from tmp_frame_infer_data_external_01
    ) as t1
where t1.rk = 1;


-- 数据展开kafka-topic1给定外部饿了么的数据进行告警筛选之后的数据关联之前告警的数据，已经存在了则这次不进行告警
drop view if exists tmp_frame_infer_data_external_03;
create view tmp_frame_infer_data_external_03 as
select
    t1.*,
    t2.pts as t2_pts,
    t2.ntp_timestamp as t2_ntp_timestamp,
    t2.user_meta_id as t2_user_meta_id
from tmp_frame_infer_data_external_02 as t1
         left join dwd_hunger_alarm_all_rt_external_source
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.id = t2.user_meta_id
                       and t1.bbox_left = t2.bbox_left
                       and t1.bbox_top = t2.bbox_top
                       and t1.bbox_width = t2.bbox_width
                       and t1.bbox_height = t2.bbox_height
                       and t1.class_id = t2.class_id
where t2.user_meta_id is null;



-- 数据展开kafka-topic2给定靖安内部测试数据
drop view if exists tmp_frame_infer_data_internal_01;
create view tmp_frame_infer_data_internal_01 as
select
    a.batch_id,
    a.frame_num,
    a.pts,
    a.ntp_timestamp,
    a.source_id,
    a.source_frame_width,
    a.source_frame_height,
    a.infer_done,
    a.image_path            as full_image_path,
    a.frame_tensor_list,
    a.user_meta.dateTime    as user_meta_dateTime,
    a.user_meta.id          as user_meta_id,
    a.user_meta.imageUrl    as user_meta_imageUrl,
    a.user_meta.name        as user_meta_name,
    t.object_id,
    t.object_label,
    t.infer_id,
    t.class_id,
    t.bbox_left,
    t.bbox_top,
    t.bbox_width,
    t.bbox_height,
    t.confidence,
    t.image_path
from  frame_infer_data_internal_test a
          cross join unnest (object_list) as t (
                                                object_id,
                                                object_label,
                                                infer_id,
                                                class_id,
                                                bbox_left,
                                                bbox_top,
                                                bbox_width,
                                                bbox_height,
                                                confidence,
                                                image_path
    )
where a.ntp_timestamp is not null;


-----------------------

-- 数据插入

-----------------------

begin statement set;

-- 给定饿了吗的数据-全量数据写入doris
insert into dwd_hunger_all_rt_doris_external
select
    pts,
    ntp_timestamp,
    ntp_timestamp_format, -- 时间戳格式化
    batch_id,
    frame_num,
    source_id,
    source_frame_width,
    source_frame_height,
    infer_done,
    full_image_path,
    frame_tensor_list,
    object_id,
    object_label,
    infer_id,
    class_id,
    bbox_left,
    bbox_top,
    bbox_width,
    bbox_height,
    cast(confidence as string) as confidence,
    image_path,
    `dateTime` as user_meta_dateTime,
    id as user_meta_id,
    imageUrl as user_meta_imageUrl,
    `name` as user_meta_name,
    'ja-flink-control-center' as create_by,
    from_unixtime(unix_timestamp()) as create_on
from tmp_frame_infer_data_external_01;


-- 给定饿了吗的数据告警数据写入doris
insert into dwd_hunger_alarm_all_rt_external_sink
select
    pts,
    ntp_timestamp,
    ntp_timestamp_format, -- 时间戳格式化
    batch_id,
    frame_num,
    source_id,
    source_frame_width,
    source_frame_height,
    infer_done,
    full_image_path,
    frame_tensor_list,
    object_id,
    object_label,
    infer_id,
    class_id,
    bbox_left,
    bbox_top,
    bbox_width,
    bbox_height,
    cast(confidence as string) as confidence,
    image_path,
    `dateTime` as user_meta_dateTime,
    id as user_meta_id,
    imageUrl as user_meta_imageUrl,
    `name` as user_meta_name,
    'ja-flink-control-center' as create_by,
    from_unixtime(unix_timestamp()) as create_on
from tmp_frame_infer_data_external_03;


-- 给定饿了吗的数据告警数据写入kafka
insert into hunger_alarm_result_external_kafka
select
    uuid() as uuid,
    batch_id,
    frame_num,
    pts,
    ntp_timestamp,
    source_id,
    source_frame_width,
    source_frame_height,
    infer_done,
    full_image_path as image_path,
    frame_tensor_list,
    array[row(
            object_id    ,
            object_label ,
            infer_id     ,
            class_id     ,
            bbox_left    ,
            bbox_top     ,
            bbox_width   ,
            bbox_height  ,
            confidence   ,
            image_path
        )] as object_list,
    row(
            `dateTime`,
            id,
            imageUrl,
            `name`
        ) as user_meta
from tmp_frame_infer_data_external_03;


-- 靖安内部测试的数据
insert into dwd_hunger_all_rt_doris_internal
select
    pts,
    ntp_timestamp,
    from_unixtime(ntp_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as ntp_timestamp_format, -- 时间戳格式化
    batch_id,
    frame_num,
    source_id,
    source_frame_width,
    source_frame_height,
    infer_done,
    full_image_path,
    frame_tensor_list,
    object_id,
    object_label,
    infer_id,
    class_id,
    bbox_left,
    bbox_top,
    bbox_width,
    bbox_height,
    cast(confidence as string) as confidence,
    image_path,
    user_meta_dateTime,
    user_meta_id,
    user_meta_imageUrl,
    user_meta_name,
    'ja-flink-control-center' as create_by,
    from_unixtime(unix_timestamp()) as create_on
from tmp_frame_infer_data_internal_01;

end;


