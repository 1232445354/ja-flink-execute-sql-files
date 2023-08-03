--********************************************************************--
-- author:      write your name here
-- create time: 2023/6/10 14:53:29
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ja-poi-data-list-info';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';


 -----------------------

 -- 数据结构

 -----------------------


-- 数据来源kafka POI数据（Source：kafka）
drop table if exists poi_data_info_kafka;
create table poi_data_info_kafka (
                                     line      string     comment '数据',
                                     UUID      string     comment 'UUID'
) WITH (
      'connector' = 'kafka',
      'topic' = 'poi_data_info',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'poi-data-info-rt',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1686379960000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 数据入库（Sink：doris）
drop table  if exists dwd_poi_all_info;
create table dwd_poi_all_info(
                                 uuid          string  comment 'UUID',
                                 name          string  comment '名称',
                                 big_type      string  comment '大类（类型）',
                                 middle_type   string  comment '小类（类型）',
                                 longitude     string  comment '经度',
                                 latitude      string  comment '纬度',
                                 province      string  comment '省份（省）',
                                 city          string  comment '城市（市）',
                                 county        string  comment '区域（县/区)',
                                 update_time   string  comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.27.95.211:30030',
     'table.identifier' = 'sa.dwd_poi_all_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='100000',
     'sink.batch.interval'='10s'
-- 'sink.properties.escape_delimiters' = 'false'，
-- 'sink.properties.column_separator' = '\x01',	 -- 列分隔符
-- 'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
-- 'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-----------------------

-- 数据处理

-----------------------
drop table if exists tmp_source_kafka_01;
create view tmp_source_kafka_01 as
select
    replace(line,'"','') as line,
    UUID as uuid
from poi_data_info_kafka
where UUID is not null
  and UUID <> '';


-----------------------

-- 数据插入

-----------------------
insert into dwd_poi_all_info
select
    uuid,
    split_index(line,'=',0)       as name,
    split_index(line,'=',1)       as big_type,
    split_index(line,'=',2)       as middle_type,
    split_index(line,'=',3)       as longitude,
    split_index(line,'=',4)       as latitude,
    split_index(line,'=',5)       as province,
    split_index(line,'=',6)       as city,
    split_index(line,'=',7)       as county,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_01;











