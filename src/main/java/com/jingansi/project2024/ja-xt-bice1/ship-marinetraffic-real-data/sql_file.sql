
set 'pipeline.name' = 'ship-marinetraffic-real-data';

set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'sql-client.execution.result-mode' = 'TABLEAU';

set 'table.exec.state.ttl' = '600000';
set 'parallelism.default' = '4';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '300000';
set 'state.checkpoints.dir' = 's3://ja-bice2/flink-checkpoints/ship-marinetraffic-real-data';


 -- 数据结构
drop table if exists marinetraffic_ship_list;
create table marinetraffic_ship_list(
                                        SHIP_ID              string, -- 船舶的唯一标识符
                                        SHIPNAME             string, -- 船舶的名称
                                        SPEED                string, -- 船舶的当前速度，以节（knots）为单位 10倍
                                        `timeStamp`          bigint, -- 采集时间
                                        LON                  string, -- 船舶当前位置的经度值
                                        LAT                  string, -- 船舶当前位置的经度值
                                        ELAPSED              string, -- 自上次位置报告以来经过的时间，以分钟为单位。
                                        COURSE               string, -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方，
                                        FLAG                 string, -- 船舶的国家或地区旗帜标识。
                                        ROT                  string, -- 船舶的旋转率，转向率
                                        proctime             as PROCTIME()
    -- GT_SHIPTYPE          string, -- 船舶的全球船舶类型码
    -- DESTINATION          string, -- 船舶的目的地
    -- W_LEFT               string, -- 舶的左舷吃水线宽度
    -- L_FORE               string, -- 船舶的前吃水线长度
    -- SHIPTYPE             string, -- 船舶的类型码，表示船舶所属的船舶类型。
    -- HEADING              string, -- 船舶的船首朝向
    -- LENGTH               string, -- 船舶的长度，以米为单位
    -- WIDTH                string, -- 船舶的宽度，以米为单位。
    -- DWT                  string, -- 船舶的载重吨位
    -- block_map_index      bigint, -- 地图分层
    -- block_range_x        bigint, -- x块
    -- block_range_y        bigint, -- y块

) with (
      'connector' = 'kafka',
      'topic' = 'marinetraffic_ship_list',
      'properties.bootstrap.servers' = '115.231.236.106:30090',
      'properties.group.id' = 'marinetraffic_ship_list_bice2',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1721208000000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 创建映射doris的全量数据表(Sink:doris)
drop table if exists ods_ship_all_track;
create table ods_ship_all_track(
                                   id                      string,
                                   time1                   string,
                                   ship_name               string,
                                   turning_rate            double,
                                   heading                 double,
                                   longitude02             double,
                                   latitude02              double,
                                   speed_kn                double,
                                   draft                   double,
                                   country_flag            string,
                                   country_chinese_name    string,
                                   navigation_status       string,
                                   source_type             string,
                                   gmt_create              string
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.19.80.4:8030',
     'table.identifier' = 'global_entity.ods_ship_all_track',
     'username' = 'admin',
     'password' = 'yshj@yshj',
     'sink.enable.batch-mode'='true',
     'sink.buffer-flush.max-rows'='50000',
     'sink.buffer-flush.interval'='20s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',     -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'         -- 行分隔符
     );


-- 创建映射doris的状态数据表(Sink:doris)
drop table if exists dwd_ship_all_track;
create table dwd_ship_all_track(
                                   id                      string,
                                   time1                   string,
                                   ship_name               string,
                                   turning_rate            double,
                                   heading                 double,
                                   longitude02             double,
                                   latitude02              double,
                                   speed_kn                double,
                                   draft                   double,
                                   country_flag            string,
                                   country_chinese_name    string,
                                   navigation_status       string,
                                   source_type             string,
                                   gmt_create              string
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.19.80.4:8030',
     'table.identifier' = 'global_entity.dwd_ship_all_track',
     'username' = 'admin',
     'password' = 'yshj@yshj',
     'sink.enable.batch-mode'='true',
     'sink.buffer-flush.max-rows'='50000',
     'sink.buffer-flush.interval'='20s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',     -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'         -- 行分隔符
     );


-- 创建映射doris的实体表(Sink:doris)
drop table if exists dws_ship_entity_info;
create table dws_ship_entity_info(
                                     id                string,
                                     ship_name         string,
                                     country_flag      string,
                                     source_type       string,
                                     gmt_create        string
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.19.80.4:8030',
     'table.identifier' = 'global_entity.dws_ship_entity_info',
     'username' = 'admin',
     'password' = 'yshj@yshj',
     'sink.enable.batch-mode'='true',
     'sink.buffer-flush.max-rows'='50000',
     'sink.buffer-flush.interval'='15s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',     -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'         -- 行分隔符
     );


-- marinetraffic和fleetmon的对应关系(Source:doris)
drop table if exists dim_mt_fm_id_relation;
create table dim_mt_fm_id_relation (
                                       ship_id       bigint  COMMENT '船编号',
                                       vessel_id     bigint  COMMENT 'mmsi',
                                       primary key (ship_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.19.80.4:9030/global_entity?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'yshj@yshj',
      'table-name' = 'dim_reletion_1',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );


-- 实体表(Source:doris)
drop table if exists dws_ship_entity_info_source;
create table dws_ship_entity_info_source (
                                             id    string  COMMENT '船编号',
                                             primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.19.80.4:9030/global_entity?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'yshj@yshj',
      'table-name' = 'dws_ship_entity_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );



-- ---------------------

-- 数据处理

-- ---------------------

-- 筛选数据处理
drop view if exists temp_01;
create view temp_01 as
select
    SHIP_ID           as ship_id,                      -- 船舶的唯一标识符
    SHIPNAME                                as shipname,             -- 船舶的名称
    from_unixtime(`timeStamp`-(cast(ELAPSED as int)*60),'yyyy-MM-dd HH:mm:00') as acquire_timestamp_format, -- 数据产生时间，爬虫采集的时间减 ELAPSED 经过的时间 ，格式化到分钟
    cast(ROT as double)                     as rate_of_turn,         -- 船舶的旋转率，转向率
    cast(COURSE as double)                  as orientation,          -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方，
    cast(LON as double)                    as lng,                  -- 船舶当前位置的经度值
    cast(LAT as double)                    as lat,                  -- 船舶当前位置的纬度值
    cast(SPEED as double)/10               as speed,                -- 船舶的当前速度，以节（knots）为单位 10倍
    FLAG                                   as cn_iso2,              -- 船舶的国家或地区旗帜标识
    proctime
from marinetraffic_ship_list
where `timeStamp` is not null
  and ELAPSED <= 60
  and CHAR_LENGTH(SHIP_ID) <= 30;


-- 关联对应关系
drop view if exists temp_02;
create view temp_02 as
select
    coalesce(t2.vessel_id,cast(t1.ship_id as bigint)) as id, -- bigint类型
    acquire_timestamp_format as time1,
    shipname as ship_name,
    rate_of_turn as turning_rate,
    orientation as heading,
    lng as longitude02,
    lat as latitude02,
    speed as speed_kn,
    cast(null as double) as draft,
    cn_iso2 as country_flag,
    cast(null as varchar) as country_chinese_name,
    cast(null as varchar) as navigation_status,
    from_unixtime(unix_timestamp()) as gmt_create,
    t2.vessel_id,
    'marinetraffic' as source_type,
    proctime
from temp_01 as t1
         left join dim_mt_fm_id_relation
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on cast(t1.ship_id as bigint) = t2.ship_id;



-----------------------

-- 数据插入

-----------------------

begin statement set;


-- 实体表
insert into dws_ship_entity_info
select
    concat('v',cast((t1.id + 1000) as varchar)) as id,
    t1.ship_name,
    t1.country_flag,
    t1.source_type,
    gmt_create
from temp_02 as t1
         left join dws_ship_entity_info_source
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on cast(t1.id as varchar) = t2.id
where t2.id is null;


-- 全量表
insert into ods_ship_all_track
select
    concat('v',cast((id + 1000) as varchar)) as id,
    time1,
    ship_name,
    turning_rate,
    heading,
    longitude02,
    latitude02,
    speed_kn,
    draft,
    country_flag,
    country_chinese_name,
    navigation_status,
    source_type,
    gmt_create
from temp_02;


-- 状态表
insert into dwd_ship_all_track
select
    concat('v',cast((id + 1000) as varchar)) as id,
    time1,
    ship_name,
    turning_rate,
    heading,
    longitude02,
    latitude02,
    speed_kn,
    draft,
    country_flag,
    country_chinese_name,
    navigation_status,
    source_type,
    gmt_create
from temp_02;

end;
