--******************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2023/5/13 11:28:09
-- description: 采集卫星satellite数据
--********************************************************************--
set 'pipeline.name' = 'ja-satellite-list-rt';


set 'table.exec.state.ttl' = '600000';
-- set 'parallelism.default' = '5';

set 'execution.checkpointing.interval' = '100000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-satellite-list-rt-checkpoint';
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
      -- 'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
      'properties.group.id' = 'satellite-collect-list-kafka-rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 数据入库（Sink：doris）
create table satellite_all_info (
                                    satellite_no                   string        comment '卫星编号',
                                    `current_date`                 string        comment '当天时间',
                                    satellite_name                 string        comment '卫星名称',
                                    line1                          string        comment '第一行数据',
                                    line2                          string        comment '第二行数据',
                                    one_line_no                    string        comment 'TLE第一行数据行号',
                                    elset_classification           string        comment '卫星秘密级别',
                                    international_designator       string        comment '国际编号',
                                    utc                            string        comment 'TLE历时',
                                    mean_motion_1st_derivative     string        comment '平均运动的一阶时间导数',
                                    mean_motion_2nd_derivative     string        comment '平均运动的二阶时间导数',
                                    bstar_drag_term                string        comment 'BSTAR拖调制系数',
                                    element_set_type               string        comment '美国空军空间指挥中心内部使用',
                                    element_number                 string        comment '星历编号',
                                    checksum1                      string        comment '校验和1',
                                    two_line_no                    string        comment 'TLE第二行数据行号',
                                    satellite_no2                  string        comment '卫星编号',
                                    orbit_inclination              string        comment '轨道的交角（度数：°）',
                                    right_ascension_ascending_node string        comment '升交点赤经',
                                    eccentricity                   string        comment '轨道偏心率',
                                    argument_perigee               string        comment '近地点角距',
                                    mean_anomaly                   string        comment '平近点角',
                                    mean_motion                    string        comment '每天环绕地球的圈数',
                                    revolution_epoch_number        string        comment '发射以来飞行的圈数',
                                    checksum2                      string        comment '校验和2',
                                    country                        string        comment '国家代码-原始',
                                    country_code                   string        comment '国家code',
                                    country_name                   string        comment '国家中文名称',
                                    launch_year                    string        comment '发射年份',
                                    crash_year                     string        comment '坠毁年份',
                                    perigee                        string        comment '近地点',
                                    apogee                         string        comment '远地点',
                                    update_time                    string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.27.95.211:30030',
      'table.identifier' = 'sa.dwd_satellite_all_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='5000',
      'sink.batch.interval'='10s'
      );


-- 数据入库（Sink：doris）
create table dwd_satellite_tle_list (
                                        `satellite_no` varchar(10) NULL COMMENT '卫星编号',
                                        `src_code` int null comment '数据来源标志1:space-track 2:n2yo',
                                        `epoch_time` string NULL COMMENT 'TLE生成发布时间',
                                        `acquire_time` string NULL COMMENT '当天时间',
                                        `satellite_name` varchar(30) NULL COMMENT '卫星名称',
                                        `line1` varchar(200) NULL COMMENT '第一行数据',
                                        `line2` varchar(200) NULL COMMENT '第二行数据',
                                        `one_line_no` varchar(200) NULL COMMENT 'tle第一行数据行号',
                                        `elset_classification` varchar(50) NULL COMMENT '卫星秘密级别',
                                        `international_designator` varchar(50) NULL COMMENT '国际编号',
                                        `utc` varchar(50) NULL COMMENT 'tle历时',
                                        `mean_motion_1st_derivative` varchar(50) NULL COMMENT '平均运动的一阶时间导数',
                                        `mean_motion_2nd_derivative` varchar(50) NULL COMMENT '平均运动的二阶时间导数',
                                        `bstar_drag_term` varchar(50) NULL COMMENT 'bstar拖调制系数',
                                        `element_set_type` varchar(50) NULL COMMENT '美国空军空间指挥中心内部使用',
                                        `element_number` varchar(50) NULL COMMENT '星历编号',
                                        `checksum1` varchar(50) NULL COMMENT '校验和1',
                                        `two_line_no` varchar(50) NULL COMMENT 'tle第二行数据行号',
                                        `satellite_no2` varchar(50) NULL COMMENT '卫星编号',
                                        `orbit_inclination` varchar(50) NULL COMMENT '轨道的交角（度数：°）',
                                        `right_ascension_ascending_node` varchar(50) NULL COMMENT '升交点赤经',
                                        `eccentricity` varchar(50) NULL COMMENT '轨道偏心率',
                                        `argument_perigee` varchar(50) NULL COMMENT '近地点角距',
                                        `mean_anomaly` varchar(50) NULL COMMENT '平近点角',
                                        `mean_motion` varchar(50) NULL COMMENT '每天环绕地球的圈数',
                                        `revolution_epoch_number` varchar(50) NULL COMMENT '发射以来飞行的圈数',
    -- `perigee` varchar(255) NULL COMMENT '近地点',
    -- `apogee` varchar(255) NULL COMMENT '远地点',
                                        `checksum2` varchar(50) NULL COMMENT '校验和2',
                                        `update_time` string NULL COMMENT '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.27.95.211:30030',
      'table.identifier' = 'sa.dwd_satellite_tle_list',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='5000',
      'sink.batch.interval'='10s'
      );


-- 卫星数据匹配库（Source：doris）
drop table if exists dim_satellite_static_info;
create table dim_satellite_static_info (
                                           satellite_no                   string        comment '卫星编号',
                                           launch_year                    string        comment '发射年份',
                                           crash_year                     string        comment '坠毁年份',
                                           perigee                        int           comment '近地点',
                                           apogee                         int           comment '远地点',
                                           country                        string        comment '国家代码-原始',
                                           country_code                   string        comment '国家或组织代码-优化后可获取国旗的，没有的为none',
                                           country_name                   string        comment '国家或组织名称',
                                           primary key (satellite_no) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_satellite_static_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '40s',
      'lookup.max-retries' = '1'
      );



-- -- 卫星国家数据匹配库（Source：doris）
-- drop table if exists dim_vessel_country_code_list;
-- create table dim_vessel_country_code_list (
--   	country      			string			comment '国家英文',
-- 	flag_country_code	    string			comment '国家的编码',
-- 	country_name			string			comment '国家的中文',
--     primary key (country) NOT ENFORCED
-- ) with (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
--     'username' = 'root',
--     'password' = 'Jingansi@110',
--     'table-name' = 'dim_vessel_country_code_list',
--     'driver' = 'com.mysql.cj.jdbc.Driver',
--     'lookup.cache.max-rows' = '10000',
--     'lookup.cache.ttl' = '60s',
--     'lookup.max-retries' = '1'
-- );



-----------------------

-- 数据处理

-----------------------

drop table if exists tmp_satellite_collect_list_01;
create view tmp_satellite_collect_list_01 as
select
    line1,
    line2,
    substring(`timeStamp`,1,10) as `current_date`,
    substring(line0,2)              as satellite_name,               -- 卫星名称
    substring(line1,1, 1)           as one_line_no,                  -- TLE第一行数据行号
    trim(substring(line1,3, 5))     as satellite_no,                 -- 卫星编号
    substring(line1,8, 1)           as elset_classification,         -- 卫星秘密级别
    trim(substring(line1,10, 8))    as international_designator,     -- 国际编号
    substring(line1,19, 14)         as utc,                          -- TLE历时
    trim(substring(line1,34, 10))   as mean_motion_1st_derivative,   -- 平均运动的一阶时间导数
    trim(substring(line1,45, 8))    as mean_motion_2nd_derivative,   -- 平均运动的二阶时间导数
    trim(substring(line1,54, 8))    as bstar_drag_term,              -- BSTAR拖调制系数
    substring(line1,63, 1)          as element_set_type,             -- 美国空军空间指挥中心内部使用
    trim(substring(line1,65, 4))    as element_number,               -- 星历编号
    substring(line1,69, 1)          as checksum1,                    -- 校验和1
    substring(line2,1, 1)           as two_line_no,                   -- TLE第二行数据行号
    trim(substring(line2,3, 5))     as satellite_no2,                 -- 卫星编号
    trim(substring(line2,9, 8))     as orbit_inclination,             -- 轨道的交角（度数：°）,
    trim(substring(line2,18, 8))    as right_ascension_ascending_node,-- 升交点赤经
    substring(line2,27, 7)          as eccentricity,                  -- 轨道偏心率
    trim(substring(line2,35, 8))    as argument_perigee,              -- 近地点角距
    trim(substring(line2,44, 8))    as mean_anomaly,                  -- 平近点角
    trim(substring(line2,53, 11))   as mean_motion,                   -- 每天环绕地球的圈数
    trim(substring(line2,64, 5))    as revolution_epoch_number,       -- 发射以来飞行的圈数
    substring(line2,69, 1)          as checksum2,                     -- 校验和2
    PROCTIME()  as proctime                                          -- 维表关联的时间函数
from satellite_collect_list_kafka
where source is null;

create view  tmp_dwd_satellite_tle_list_01 as
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
    -- perigee,
    -- apogee,
    substring(line2,69, 1) as checksum2,
    from_unixtime(unix_timestamp()) as update_time
from
    (select *, (cast(trim(substring(line1, 21,12)) as double)-1)*86400 as s, cast(substring(line1, 19, 2) as int) as y from satellite_collect_list_kafka) a
where (source is null or source in('N2YO','CeleStrak'))
  and char_length(line1)>40
  and y >= 20;


-----------------------

-- 数据插入

-----------------------

begin statement set;

insert into satellite_all_info
select
    t1.satellite_no,
    `current_date`,
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
    satellite_no2,
    orbit_inclination,
    right_ascension_ascending_node,
    eccentricity,
    argument_perigee,
    mean_anomaly,
    mean_motion,
    revolution_epoch_number,
    checksum2,
    t2.country,
    t2.country_code as country_code,
    t2.country_name as country_name,
    t2.launch_year,
    t2.crash_year,
    cast(t2.perigee as string) as perigee,
    cast(t2.apogee as string) as apogee,
    from_unixtime(unix_timestamp()) as update_time
from tmp_satellite_collect_list_01 as t1

         left join dim_satellite_static_info
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.satellite_no = t2.satellite_no
where t1.satellite_no is not null;

-- left join dim_vessel_country_code_list
-- FOR SYSTEM_TIME AS OF t1.proctime as t3
-- on t2.country = t3.flag_country_code;

insert into dwd_satellite_tle_list
select * from tmp_dwd_satellite_tle_list_01
where satellite_no is not null;

end;

