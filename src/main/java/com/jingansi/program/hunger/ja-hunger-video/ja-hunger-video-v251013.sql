--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/3/8 09:33:33
-- description: 另一份数据入库
-- version：ja-hunger-video-v251013
--********************************************************************--
set 'pipeline.name' = 'ja-hunger-video';

SET 'parallelism.default' = '6';
set 'table.exec.state.ttl' = '300000';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
-- SET 'sql-client.display.max-column-width' = '200';

-- -- checkpoint的时间和位置
-- SET 'execution.checkpointing.interval' = '300000';
-- SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-hunger-project' ;



-- kafka来源的数据给外部饿了么测试的（Source：kafka）
create table test_infer_result (
                                   id           string,
                                   dateTime     string,
                                   imageUrl     string,
                                   name         string

) WITH (

      'connector' = 'kafka',
      'topic' =  'ja_video_snapshot',
      'properties.bootstrap.servers' = '172.22.219.30:9092',
      'properties.group.id' = 'ja-video-snapshot1',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1759939200000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true',

      -- 新增超时和重试配置
      'properties.request.timeout.ms' = '40000',
      'properties.retry.backoff.ms' = '1000',
      'properties.metadata.max.age.ms' = '300000',
      'properties.connections.max.idle.ms' = '540000',

      -- SASL认证配置
      'properties.security.protocol' = 'SASL_PLAINTEXT',
      'properties.sasl.mechanism' = 'PLAIN',
      'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="jingansi" password="jingansi110";'
      );



-- 全量数据入库（Sink：doris）
create table ja_video_snapshot (
                                   id                string,
                                   acquire_time      string,
                                   name              string,
                                   image_url         string,
                                   update_time       string
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'hunger.ja_video_snapshot',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='10000',
     'sink.batch.interval'='10s'
     );




---------------

-- 数据处理

---------------




-----------------------

-- 数据插入

-----------------------

begin statement set;



-- 给定饿了吗的数据-全量数据写入doris
insert into ja_video_snapshot
select
    id,
    -- DATE_FORMAT(TO_DATE(dateTime, 'yyyyMMdd'), 'yyyy-MM-dd') as acquire_time,
    from_unixtime(unix_timestamp(dateTime,'yyyyMMdd')) as acquire_time,
    name,
    imageUrl     as image_url,
    from_unixtime(unix_timestamp()) as update_time
from test_infer_result;

end;



