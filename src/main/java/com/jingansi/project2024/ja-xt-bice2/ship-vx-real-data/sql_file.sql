
set 'pipeline.name' = 'ship-vt-real-data';

set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'sql-client.execution.result-mode' = 'TABLEAU';

set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '4';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '300000';
set 'state.checkpoints.dir' = 's3://ja-bice2/flink-checkpoints/ship-vt-real-data';


-- ---------------------
 -- 数据结构
-- ---------------------
-- 创建kafka全量vt数据来源的表（Source：kafka）
drop table if exists ais_vtexplorer_ship_list;
create table ais_vtexplorer_ship_list(
                                         MMSI                 string, -- mmsi
                                         IMO                  string, -- imo
                                         name                 string, -- 名称
                                         callsign             string, -- 呼号
                                         country              string, -- 国家
                                         is_satellite         boolean,-- 是否卫星数据
                                         type                 bigint, -- 类型代码
                                         size                 string, -- 尺寸
                                         destination_code     string, -- 目的地代码
                                         destination_name     string, -- 目的地名称
                                         longitude            double, -- 经度
                                         latitude             double, -- 纬度
                                         speed                double, -- 速度/节
                                         course               int,   -- 航向
                                         draught              double, -- 吃水
                                         ETA                  string, -- 预计到岗时间
                                         `timestamp`          bigint, -- 采集时间戳
                                         ais_time             string,
                                         proctime             as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'ais_vtexplorer_ship_list2',
      'properties.bootstrap.servers' = '115.231.236.106:30090',
      'properties.group.id' = 'ja-vtexplore-ceshi2',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1713341400000',
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
     'fenodes' = '8.130.39.51:8030',
     'table.identifier' = 'global_entity.ods_ship_all_track',
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
     'fenodes' = '8.130.39.51:8030',
     'table.identifier' = 'global_entity.dwd_ship_all_track',
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


-- 创建映射doris的实体表(Sink:doris)
drop table if exists dws_ship_entity_info;
create table dws_ship_entity_info(
                                     id                        string, -- id
                                     ship_name                 string, -- 名称
                                     imo                       string, -- imo
                                     mmsi                      string, -- mmsi
                                     callsign                  string, -- 呼号
                                     `length`                  double, -- 长度
                                     width                     double, -- 宽度
                                     country_flag              string, -- 国家代码
                                     country_chinese_name      string, -- 国家名称
                                     source_type               string,
                                     gmt_create                string
)WITH (
     'connector' = 'doris',
     'fenodes' = '8.130.39.51:8030',
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



-- marinetraffic和vt的对应关系(Source:doris)
drop table if exists dim_mtf_vt_reletion_info;
create table dim_mtf_vt_reletion_info (
                                          vt_mmsi      string        comment 'vt数据的mmsi',
                                          vessel_id    string        comment '编号',
                                          primary key (vt_mmsi) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://8.130.39.51:9030/global_entity?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'yshj@yshj',
      'table-name' = 'dim_reletion_2',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 船国家数据匹配库（Source：doris）
drop table if exists dim_vt_country_code_info;
create table dim_vt_country_code_info (
                                          id              string  comment 'id',
                                          source          string  comment '来源',
                                          e_name          string  comment '英文名称',
                                          c_name          string  comment '中文名称',
                                          vt_c_name       string  comment 'vt的中文名称',
                                          country_code2   string  comment '国家2字代码',
                                          country_code3   string  comment '国家3字代码',
                                          flag_url        string  comment '国旗url',
                                          primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://8.130.39.51:9030/global_entity?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'yshj@yshj',
      'table-name' = 'dim_reletion_3',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 实体表(Source:doris)
drop table if exists dws_ship_entity_info_source;
create table dws_ship_entity_info_source (
                                             id    string  COMMENT '船编号',
                                             primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://8.130.39.51:9030/global_entity?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
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

-- 筛选数据
drop view if exists temp_01;
create view temp_01 as
select
    tt.MMSI                                                   as mmsi,
    if(tt.IMO='0',cast(null as string),tt.IMO)                as imo,  -- imo
    if(tt.callsign <> '',tt.callsign,cast(null as varchar))   as callsign,   -- 呼号
    from_unixtime(`timestamp`,'yyyy-MM-dd HH:mm:ss')          as time1, -- 采集时间戳格式化
    if(tt.name <> '',tt.name,cast(tt.name as varchar))        as ship_name,   -- vt船舶名称
    if(tt.country = '',cast(null as varchar),tt.country)      as country_chinese_name, -- vt国家中文
    tt.longitude                                              as longitude02,  -- 经度
    tt.latitude                                               as latitude02,  -- 纬度
    tt.speed                                                  as speed_kn,  -- 船舶的当前速度，以节（knots）为单位
    tt.course                                                 as heading,  -- 船舶的当前航向
    tt.draught                                                as draft,  -- 吃水
    cast(trim(split_index(tt.`size`,'*',0)) as double)        as `length`,  -- 船舶的长度，以米为单位
    cast(trim(split_index(tt.`size`,'*',1)) as double)        as width,  -- 船舶的宽度，以米为单位
    t3.country_code2                                          as country_flag, -- 船舶的国家或地区旗帜标识

    concat(
            'v',
            cast(
                    if(t2.vessel_id is not null,
                       cast(t2.vessel_id as bigint) + 1000,
                       cast(tt.MMSI as bigint) + 4000000000 + 1000)
                as varchar)

        )as id, -- varchar
    'vt' as source_type,


    from_unixtime(unix_timestamp()) as gmt_create,
    cast(null as varchar) as navigation_status,
    cast(null as double) as turning_rate,
    tt.proctime
from ais_vtexplorer_ship_list as tt
         left join dim_mtf_vt_reletion_info
    FOR SYSTEM_TIME AS OF tt.proctime as t2
                   on tt.MMSI = t2.vt_mmsi

         left join dim_vt_country_code_info
    FOR SYSTEM_TIME AS OF tt.proctime as t3
                   on tt.country  = t3.vt_c_name;



-- ---------------------
-- 数据入库
-- ---------------------

begin statement set;

-- 实体表
insert into dws_ship_entity_info
select
    t1.id       , -- id
    t1.ship_name       , -- 名称
    t1.imo      , -- imo
    t1.mmsi     , -- mmsi
    t1.callsign       , -- 呼号
    t1.`length`       , -- 长度
    t1.width       , -- 宽度
    t1.country_flag     , -- 国家代码
    t1.country_chinese_name     , -- 国家名称
    t1.source_type,
    t1.gmt_create
from temp_01 as t1
         left join dws_ship_entity_info_source
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.id = t2.id
where t2.id is null;


-- 全量表
insert into ods_ship_all_track
select
    id,
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
from temp_01;


-- 状态表
insert into dwd_ship_all_track
select
    id,
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
from temp_01;

end;

