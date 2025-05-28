--******************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/04/07 11:28:09
-- description: 采集卫星satellite数据
-- version: ja-satellite-list-rt-v240328
--********************************************************************--
set 'pipeline.name' = 'ja-satellite-list-rt';


set 'table.exec.state.ttl' = '600000';
-- set 'parallelism.default' = '5';

set 'execution.checkpointing.interval' = '100000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-satellite-list-rt';
-- 空闲分区不用等待
-- set 'table.exec.source.idle-timeout' = '3s';


 -----------------------

 -- 数据结构

 -----------------------

-- 创建kafka全量卫星satellite数据来源的表（Source：kafka）
drop table  if exists satellite_collect_list_kafka;
create table satellite_collect_list_kafka(
                                             line0             string       comment 'TLE第一行数据',
                                             line1             string       comment 'TLE第二行数据',
                                             line2             string       comment 'TLE第三行数据',
                                             source            string       comment '来源',
                                             name              string       comment '名称',
                                             `timeStamp`       string       comment '时间（当天）'
) with (
      'connector' = 'kafka',
      'topic' = 'satellite_collect_list',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'satellite-collect-list-kafka-rt-idc',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1716807514000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 数据入库（Sink：doris）
drop table if exists dwd_bhv_satellite_rt;
create table dwd_bhv_satellite_rt (
                                      satellite_no                    varchar(10)      comment '卫星编号',
                                      src_code                        int              comment '数据来源标志1:space-track 2:n2yo',
                                      epoch_time                      string           COMMENT 'TLE生成发布时间',
                                      acquire_time                    string           COMMENT '当天时间',
                                      satellite_name                  varchar(30)      COMMENT '卫星名称',
                                      line1                           varchar(200)     COMMENT '第一行数据',
                                      line2                           varchar(200)     COMMENT '第二行数据',
                                      one_line_no                     varchar(200)     COMMENT 'tle第一行数据行号',
                                      elset_classification            varchar(50)      COMMENT '卫星秘密级别',
                                      international_designator        varchar(50)      COMMENT '国际编号',
                                      utc                             varchar(50)      COMMENT 'tle历时',
                                      mean_motion_1st_derivative      varchar(50)      COMMENT '平均运动的一阶时间导数',
                                      mean_motion_2nd_derivative      varchar(50)      COMMENT '平均运动的二阶时间导数',
                                      bstar_drag_term                 varchar(50)      COMMENT 'bstar拖调制系数',
                                      element_set_type                varchar(50)      COMMENT '美国空军空间指挥中心内部使用',
                                      element_number                  varchar(50)      COMMENT '星历编号',
                                      checksum1                       varchar(50)      COMMENT '校验和1',
                                      two_line_no                     varchar(50)      COMMENT 'tle第二行数据行号',
                                      satellite_no2                   varchar(50)      COMMENT '卫星编号',
                                      orbit_inclination               varchar(50)      COMMENT '轨道的交角（度数：°）',
                                      right_ascension_ascending_node  varchar(50)      COMMENT '升交点赤经',
                                      eccentricity                    varchar(50)      COMMENT '轨道偏心率',
                                      argument_perigee                varchar(50)      COMMENT '近地点角距',
                                      mean_anomaly                    varchar(50)      COMMENT '平近点角',
                                      mean_motion                     varchar(50)      COMMENT '每天环绕地球的圈数',
                                      revolution_epoch_number         varchar(50)      COMMENT '发射以来飞行的圈数',
                                      checksum2                       varchar(50)      COMMENT '校验和2',
                                      update_time                     string           COMMENT '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030,172.21.30.244:8030,172.21.30.246:8030',
      'table.identifier' = 'sa.dwd_bhv_satellite_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='5000',
      'sink.batch.interval'='10s'
      );


-----------------------

-- 数据处理

-----------------------


-- 数据解析
drop view if exists tmp_dwd_satellite_tle_list_01;
create view tmp_dwd_satellite_tle_list_01 as
select
    trim(substring(line1,3, 5)) as satellite_no,
    case when source is null then 1 when source='N2YO' then 2 when source='CeleStrak' then 3 end as src_code,
    cast(timestampadd(SECOND,cast(s as int)+(8*60*60) ,timestampadd(YEAR,y,to_timestamp('2000-01-01 00:00:00','yyyy-MM-dd HH:mm:ss'))) as string) as epoch_time,
    `timeStamp` as acquire_time,
    trim(if(source is null,substring(line0,2),if(name='',cast(null as string),name))) as satellite_name,
    line1 as line1,
    line2 as line2,
    substring(line1,1, 1) as one_line_no,
    substring(line1,8, 1) as elset_classification,
    trim(substring(line1,10, 8)) as international_designator,
    substring(line1,19, 14) as utc,
    trim(substring(line1,34, 10)) as mean_motion_1st_derivative,
    trim(substring(line1,45, 8))  as mean_motion_2nd_derivative,
    trim(substring(line1,54, 8)) as bstar_drag_term,
    substring(line1,63, 1)       as element_set_type,
    trim(substring(line1,65, 4)) as element_number,
    substring(line1,69, 1) as checksum1,
    substring(line2,1, 1) as two_line_no,
    trim(substring(line2,3, 5))  as satellite_no2,
    trim(substring(line2,9, 8))  as orbit_inclination,
    trim(substring(line2,18, 8)) as right_ascension_ascending_node,
    substring(line2,27, 7)          as eccentricity,
    trim(substring(line2,35, 8))    as argument_perigee,
    trim(substring(line2,44, 8))    as mean_anomaly,
    trim(substring(line2,53, 11))   as mean_motion,
    trim(substring(line2,64, 5)) as revolution_epoch_number,
    substring(line2,69, 1) as checksum2
from
    (select *, (cast(trim(substring(line1, 21,12)) as double)-1)*86400 as s, cast(substring(line1, 19, 2) as int) as y from satellite_collect_list_kafka) a
where (source is null or source in('N2YO','CeleStrak'))
  and char_length(line1)>40
  and y >= 20;


-- 筛选数据
drop view if exists tmp_dwd_satellite_tle_list_02;
create view tmp_dwd_satellite_tle_list_02 as
select
    *,
    concat(repeat('0',5 - CHAR_LENGTH(satellite_no)),satellite_no) as satellite_no1,
    from_unixtime(unix_timestamp()) as update_time,
    PROCTIME()  as proctime
from tmp_dwd_satellite_tle_list_01
where satellite_no is not null;



-- 关联实体表看是否有详情
-- drop view if exists tmp_dwd_satellite_tle_list_03;
-- create view tmp_dwd_satellite_tle_list_03 as
-- select
--   t1.satellite_no,
--   t1.satellite_no1,
--   cast(null as varchar) as intl_code,
--   t1.satellite_name,
--   t1.elset_classification as classification,
--   '1' as remark,
--   update_time as create_time,
--   update_time
-- from tmp_dwd_satellite_tle_list_02 as t1
--   left join dws_satellite_entity_info_source
-- FOR SYSTEM_TIME AS OF t1.proctime as t2
-- on t1.satellite_no = t2.satellite_no
--   where t2.satellite_no is null;


-----------------------

-- 数据插入

-----------------------

begin statement set;

-- 数据入全量表
insert into dwd_bhv_satellite_rt
select
    satellite_no1,
    src_code,
    epoch_time,
    acquire_time,
    satellite_name,
    line1,
    line2,
    one_line_no,
    elset_classification,
    international_designator,
    utc,
    mean_motion_1st_derivative,
    mean_motion_2nd_derivative,
    bstar_drag_term,
    element_set_type,
    element_number,
    checksum1,
    two_line_no,
    satellite_no1 as satellite_no2,
    orbit_inclination,
    right_ascension_ascending_node,
    eccentricity,
    argument_perigee,
    mean_anomaly,
    mean_motion,
    revolution_epoch_number,
    checksum2,
    update_time
from tmp_dwd_satellite_tle_list_02;


-- 数据入实体表
-- insert into dws_satellite_entity_info
-- select
--   satellite_no1,
--   intl_code,
--   satellite_name,
--   classification,
--   remark,
--   create_time,
--   update_time
-- from tmp_dwd_satellite_tle_list_03;

end;

