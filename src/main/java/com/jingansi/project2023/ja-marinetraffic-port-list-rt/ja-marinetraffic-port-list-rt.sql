--********************************************************************--
-- author:      write your name here
-- create time: 2023/7/20 22:18:57
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ja-marinetraffic-port-list-rt';



-- set 'parallelism.default' = '2';
-- set 'execution.type' = 'streaming';
-- set 'table.planner' = 'blink';
-- set 'table.exec.state.ttl' = '600000';
-- set 'sql-client.execution.result-mode' = 'TABLEAU';

-- -- checkpoint的时间和位置
-- set 'execution.checkpointing.interval' = '100000';
-- set 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/xxx-checkpoint';

-- 空闲分区不用等待
-- set 'table.exec.source.idle-timeout' = '3s';

-- 注册自定义函数
-- create function rectangle_intersect_polygon as 'RectangleIntersectPolygon';
-- create function merge_plate_no as 'MergePlateNo';


 -----------------------

 -- 数据结构

 -----------------------

--数据来源kafka（Source：Kafka）
drop table if exists marinetraffic_port_list_kafka;
create table marinetraffic_port_list_kafka(

    PORT array <string>

) with (
      'connector' = 'kafka',
      'topic' = 'marinetraffic_port_list',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'marinetraffic-port-list-rt',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1689855661000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


--（Sink：Doris）
drop table if exists dwd_maric_port_all_info;
create table dwd_maric_port_all_info (
                                         port_id      				string,   -- 港口id
                                         acquire_timestamp_format	string,   -- 时间戳格式化
                                         acquire_timestamp           bigint,   -- 采集时间戳
                                         port_name      				string,   -- 港口名称
                                         longitude      				string,   -- 纬度
                                         latitude      				string,   -- 经度
                                         country_region_id      		string,   -- 国家/地区ID
                                         port_type      				string,   -- 港口类型（P表示港口）
                                         un_locode      				string,   -- Un/locode解锁  港口代码：编码（可能是国际标准化组织的国家代码和港口代码的组合）
                                         create_by 					string,	 -- 创建人
                                         update_time					string	 -- 数据入库时间

) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'table.identifier' = 'sa.dwd_maric_port_all_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='100000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'flase',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );




-----------------------

-- 数据处理

-----------------------




-----------------------

-- 数据插入

-----------------------

insert into dwd_maric_port_all_info
select
    PORT[1] as  port_id,
    from_unixtime(unix_timestamp()) as acquire_timestamp_format,
    cast(null as bigint) as acquire_timestamp,
    PORT[2] as  port_name,
    PORT[3] as  longitude,
    PORT[4] as  latitude,
    PORT[5] as  country_region_id,
    PORT[6] as  port_type,
    PORT[7] as  un_locode,
    'ja-flink-control-center',
    from_unixtime(unix_timestamp()) as update_time

from marinetraffic_port_list_kafka;