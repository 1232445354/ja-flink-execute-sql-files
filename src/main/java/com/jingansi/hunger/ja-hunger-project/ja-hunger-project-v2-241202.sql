--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/3/8 09:33:33
-- description: 饿了么数据存储、内部新版本测试的，现在已经是正式的了
-- 2024-12-02
--********************************************************************--
set 'pipeline.name' = 'ja-hunger-project-v2-241202';

SET 'parallelism.default' = '2';
set 'table.exec.state.ttl' = '300000';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'sql-client.display.max-column-width' = '100';

-- -- checkpoint的时间和位置
SET 'execution.checkpointing.interval' = '300000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-chenkpoints/ja-hunger-project-v2-241202' ;


-- kafka来源的数据给外部饿了么测试的（Source：kafka）
drop table if exists test_infer_result;
create table test_infer_result (
                                   batch_id                bigint,                        -- 批处理ID
                                   is_cover                boolean,
                                   uncover_confidence      string,
                                   cover_confidence        string,
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
                                       id                   string,    -- 摄像头id
                                       imageUrl             string,
                                       name                 string
                                       ),
                                   rowtime as to_timestamp_ltz(ntp_timestamp,3),
                                   watermark for rowtime as rowtime - interval '5' second
) WITH (
      'connector' = 'kafka',
      'topic' =  'ja_infer_result3', --test_infer_result3
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'test-infer-result-rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 全量数据入库（Sink：doris）
drop table if exists dwd_hunger_all_rt_test;
create table dwd_hunger_all_rt_test (
                                        object_id               bigint          comment '目标ID',
                                        ntp_timestamp_format    timestamp       comment '时间戳格式化',
                                        ntp_timestamp           bigint      	comment '时间戳',
                                        pts                     bigint      	comment 'pts 值',
                                        batch_id                bigint      	comment '批处理ID',
                                        frame_num               int         	comment '帧编号',
                                        source_id               string          comment '数据源ID',
                                        source_frame_width      int         	comment '原始帧宽度',
                                        source_frame_height     int         	comment '原始帧高度',
                                        infer_done              boolean		    comment '',
                                        full_image_path         string          comment '大图图片存储路径',
                                        frame_tensor_list       string          comment '输出基于帧的特征向量',

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
                                        is_cover                boolean,
                                        uncover_confidence      string,
                                        cover_confidence 	    string,
                                        update_time             string        comment '数据入库时间'
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'hunger.dwd_hunger_all_rt_test',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='12s'
     );



-- Doris写入的告警数据（Sink：doris）
drop table if exists dwd_hunger_alarm_test;
create table dwd_hunger_alarm_test (
                                       object_id               bigint          comment '目标ID',
                                       ntp_timestamp_format    timestamp       comment '时间戳格式化',
                                       ntp_timestamp           bigint      	comment '时间戳',
                                       pts                     bigint      	comment 'pts 值',
                                       batch_id                bigint      	comment '批处理ID',
                                       frame_num               int         	comment '帧编号',
                                       source_id               string          comment '数据源ID',
                                       source_frame_width      int         	comment '原始帧宽度',
                                       source_frame_height     int         	comment '原始帧高度',
                                       infer_done              boolean		    comment '',
                                       full_image_path         string          comment '大图图片存储路径',
                                       frame_tensor_list       string          comment '输出基于帧的特征向量',

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
                                       is_cover                boolean,
                                       uncover_confidence      string,
                                       cover_confidence 	    string,
                                       update_time             string        comment '数据入库时间'
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'hunger.dwd_hunger_alarm_test',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='12s'
-- 'sink.properties.recover_with_empty_tablet' = 'true'
     );


---------------

-- 数据处理

---------------

-- 数据展开
drop view if exists tmp_frame_infer_data_external_01;
create view tmp_frame_infer_data_external_01 as
select
    a.batch_id,
    a.frame_num,
    a.pts,
    a.ntp_timestamp,
    -- from_unixtime(a.ntp_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as ntp_timestamp_format, -- 时间戳格式化
    TO_TIMESTAMP_LTZ(a.ntp_timestamp,3) as ntp_timestamp_format, -- 时间戳格式化
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
    a.is_cover,
    a.uncover_confidence,
    a.cover_confidence,
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
from  test_infer_result a
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
where a.ntp_timestamp is not null
  and t.object_id is not null;


-- 数据展开kafka-topic1给定外部饿了么的数据进行告警筛选
drop view if exists tmp_frame_infer_data_external_02;
create view tmp_frame_infer_data_external_02 as
select
    *
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
            is_cover,
            uncover_confidence,
            cover_confidence,
            row_number() over(partition by object_id,bbox_left,bbox_top,bbox_width,bbox_height,class_id order by ntp_timestamp) as rk
        from tmp_frame_infer_data_external_01
    ) as t1
where t1.rk = 1;


-----------------------

-- 数据插入

-----------------------

begin statement set;

-- 给定饿了吗的数据-全量数据写入doris
insert into dwd_hunger_all_rt_test
select
    object_id,
    ntp_timestamp_format, -- 时间戳格式化
    ntp_timestamp,
    pts,
    batch_id,
    frame_num,
    source_id,
    source_frame_width,
    source_frame_height,
    infer_done,
    full_image_path,
    frame_tensor_list,
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
    is_cover,
    uncover_confidence,
    cover_confidence,
    from_unixtime(unix_timestamp()) as update_time
from tmp_frame_infer_data_external_01;


-- 给定饿了吗的数据告警数据写入doris
insert into dwd_hunger_alarm_test
select
    object_id,
    ntp_timestamp_format, -- 时间戳格式化
    ntp_timestamp,
    pts,
    batch_id,
    frame_num,
    source_id,
    source_frame_width,
    source_frame_height,
    infer_done,
    full_image_path,
    frame_tensor_list,
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
    is_cover,
    uncover_confidence,
    cover_confidence,
    from_unixtime(unix_timestamp()) as update_time
from tmp_frame_infer_data_external_02;

end;


