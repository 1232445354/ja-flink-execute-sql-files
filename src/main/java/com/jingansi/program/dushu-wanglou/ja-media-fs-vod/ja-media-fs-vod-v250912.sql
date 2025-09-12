--********************************************************************--
-- author:      write your name here
-- create time: 2024/12/10 16:43:58
-- description:
-- version ja-media-fs-vod-v250912
--********************************************************************--

set 'pipeline.name' = 'ja-media-fs-vod';
SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '6';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-device-vod' ;


-- 设备检测数据上报（Source：kafka）
create table device_vod (
    `Key`    string     comment 'key_name'
) WITH (
      'connector' = 'kafka',
      'topic' = 'fh_minio_sync',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '135.100.11.110:31409',
      'properties.group.id' = 'ja-media-fs-vod',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1733821319932',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



create table minio_events (
                              device_id	     string,
                              `timestamp`      bigint,
                              group_time       int,
                              key_name         string,
                              acquire_timestamp_format string
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     -- 'fenodes' = '172.21.30.202:30030',
     'table.identifier' = 'dushu.minio_events',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='5000',
     'sink.batch.interval'='10s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


create view tmp_source_kafka_01 as
select
    split_index(split_index(`Key`,'/',2),'/',0) as device_id,
    cast(split_index(split_index(`Key`,'/',8),'-',1) as bigint) as `timestamp`,
    -- 雷达
    -- 轨迹和操作日志的
    cast(substr(split_index(split_index(`Key`,'/',8),'-',1), 0,9) as int) as group_time,
    `Key` as key_name,
    from_unixtime(cast(split_index(split_index(`Key`,'/',8),'-',1) as bigint) / 1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format
from device_vod
where split_index(`Key`,'/',1) = 'vod';



-- 执法仪、无人机轨迹数据入库
insert into minio_events
select
    device_id,
    `timestamp`,
    group_time,
    key_name,
    acquire_timestamp_format
from tmp_source_kafka_01;




