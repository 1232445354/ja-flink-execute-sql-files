--********************************************************************--
-- author:      yobo@jingan-inc.com
-- create time: 2023/4/8 15:06:39
-- description: radarbox网站的航空公司数据入库
--********************************************************************--

set 'pipeline.name' = 'ja-radarbox-airline-list-rt';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
-- SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '6';
-- SET 'execution.checkpointing.interval' = '60000';
-- SET 'state.checkpoints.dir' = 's3://flink/ja-chingchi-icos3.0-rt-checkpoint' ;


-- radarbox网站里航空公司数据获取（Source：kafka）
drop table if exists radarbox_airline_list_kafka;
create table radarbox_airline_list_kafka (
                                             alna     string     comment '航空公司',
                                             alia     string     comment 'iata码-2',
                                             alic     string     comment 'icao码-3',
                                             cnt      int        comment '收到的消息'
) WITH (
      'connector' = 'kafka',
      'topic' = 'radarbox_airline_list',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'radarbox-airline-list-rt',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 航空公司数据入库（Sink：doris）
drop table if exists radarbox_airline_list_info;
create table radarbox_airline_list_info (
                                            radarbox_airline               string        comment '航空公司',
                                            iata                           string        comment 'iata码-2',
                                            icao                           string        comment 'icao码-3',
                                            cnt                            int           comment '收到的消息',
                                            airline_chinese                string        comment '航空公司中文',
                                            country_code                   string        comment '国家编码英文',
                                            country                        string        comment '国家',
                                            update_time                    string        comment '入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.202:30030',
      'table.identifier' = 'kesadaran_situasional.radarbox_airline_list_info',
      'username' = 'admin',
      'password' = 'admin',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000'
      );


-----------------------

-- 数据插入

-----------------------

-- 航空公司数据入库doris
insert into radarbox_airline_list_info
select
    alna  as radarbox_airline,
    alia  as iata,
    alic  as icao,
    cnt   ,
    cast(null as varchar) as airline_chinese,
    cast(null as varchar) as country_code,
    cast(null as varchar) as country,
    from_unixtime(unix_timestamp()) as update_time
from radarbox_airline_list_kafka;






