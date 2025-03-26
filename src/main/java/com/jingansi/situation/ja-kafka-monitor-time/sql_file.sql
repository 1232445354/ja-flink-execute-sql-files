--********************************************************************--
-- author:      write your name here
-- create time: 2024/12/2 19:42:10
-- description: kafka-topic的最大时间监控
-- version:
--********************************************************************--

set 'pipeline.name' = 'ja-kafka-monitor-time';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '4';
set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-kafka-monitor-time';


 -----------------------

 -- 数据结构

 -----------------------


-- 监控数据 （Source：kafka）
create table topic_latest_msg_time_kafka (
                                             currentTime    string     comment '采集时间',
                                             topic          string     comment 'topic名称',
                                             partitions     array<
                                                 row(
                                                 latestTime       string   , -- 最大时间
                                                 `partition`      bigint    -- 分区

                                                 )
                                                 >
) WITH (
      'connector' = 'kafka',
      'topic' = 'topic_latest_msg_time',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'topic_latest_msg_time_groupid',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1740384023000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 数据入库doris
create table dwd_topic_timestamp_alarm (
                                           topic_name   		string   comment 'topic名称',
                                           topic_partition	bigint 	 comment 'topic的分区',
                                           acquire_time      string   comment '执行程序采集时间',
                                           latest_time		string   comment '该分区最大数据时间',
                                           update_time      string    comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'ja_alarm.dwd_topic_timestamp_alarm',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='10s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );




-----------------------

-- 数据处理

-----------------------

-- kafka来源的所有数据解析
create view tmp01 as
select
    *
from topic_latest_msg_time_kafka
where currentTime is not null
  and topic is not null;



-- 设备检测数据（雷达）数据进一步解析数组
create view tmp02 as
select
    t1.topic          as topic_name,
    t1.currentTime    as acquire_time,
    t2.latestTime     as latest_time,
    t2.`partition`    as topic_partition
from tmp01 as t1
         cross join unnest (partitions) as t2 (
                                               latestTime         ,
                                               `partition`
    )
where t2.`partition` is not null;



-----------------------

-- 数据插入

-----------------------


begin statement set;



-- 属性数据入库
insert into dwd_topic_timestamp_alarm
select
    topic_name,
    topic_partition,
    acquire_time,
    latest_time,
    from_unixtime(unix_timestamp()) as update_time
from tmp02;


end;

