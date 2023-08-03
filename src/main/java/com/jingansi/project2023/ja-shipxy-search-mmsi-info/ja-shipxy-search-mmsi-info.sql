--********************************************************************--
-- author:      write your name here
-- create time: 2023/8/1 14:56:55
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ja-shipxy-search-mmsi-info';



set 'table.exec.state.ttl' = '500000';
-- set 'parallelism.default' = '4';

set 'execution.checkpointing.interval' = '300000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-shipxy-search-mmsi-info-ck';
-- 空闲分区不用等待
-- set 'table.exec.source.idle-timeout' = '3s';


-----------------------

 -- 数据结构

 -----------------------


-- 创建kafka全量AIS数据来源的表（Source：kafka）
drop table  if exists shipxy_ship_search_mmsi_kafka;
create table shipxy_ship_search_mmsi_kafka(
                                              mmsi      bigint       , -- mmsi
                                              status    bigint       , -- 状态
                                              `data`    array <
                                                  row<
                                                  mmsi         string,     --  mmsi
                                              name          string,     --  船舶英文名称
                                              cnname        string,     --  船舶中文名称
                                              callsign      string

                                                  >
                                                  >

) with (
      'connector' = 'kafka',
      'topic' = 'shipxy_ship_search_mmsi',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'shipxy_ship_search_mmsi',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1690870255000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 创建映射doris的数据表(Sink:doris)
drop table if exists dim_vessel_c_name_info;
create table dim_vessel_c_name_info(
                                       mmsi                   	string  comment 'mmsi',
                                       e_name                    string  comment '船舶英文名称',
                                       acquire_timestamp_format  string  comment '数据采集时间戳格式化',
                                       acquire_timestamp         bigint  comment '数据采集时间戳',
                                       c_name                    string  comment '船舶中文名称',
                                       source					string  comment '数据来源网站简称',
                                       remark                    string  comment '备注',
                                       create_by                 string  comment '创建人',
                                       update_time               string  comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'sa.dim_vessel_c_name_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='10000',
     'sink.batch.interval'='20s'
-- 'sink.properties.escape_delimiters' = 'flase',
-- 'sink.properties.column_separator' = '\x01',	 -- 列分隔符
-- 'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
-- 'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-----------------------

-- 数据处理

-----------------------
drop view if exists temp_shipxy_ship_01;
create view temp_shipxy_ship_01 as
select
    mmsi,
    `data`[1].mmsi     as inside_mmsi,
    `data`[1].name     as e_name,
    `data`[1].cnname   as c_name,
    `data`[1].callsign as callsign
from shipxy_ship_search_mmsi_kafka;



-----------------------

-- 数据插入

-----------------------


insert into dim_vessel_c_name_info
select
    cast(mmsi as varchar) as mmsi ,
    e_name,
    from_unixtime(unix_timestamp()) as acquire_timestamp_format,
    unix_timestamp() as acquire_timestamp,
    c_name,
    'https://www.shipxy.com' as source,
    callsign as remark,
    'yibo' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from temp_shipxy_ship_01
where c_name is not null
  and c_name <> '';









