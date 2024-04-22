
set 'pipeline.name' = 'ship-vt-real-data';

set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'sql-client.execution.result-mode' = 'TABLEAU';

set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '4';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '300000';
set 'state.checkpoints.dir' = 's3://ja-bice1/flink-checkpoints/ship-vt-real-data';


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
      'properties.bootstrap.servers' = '47.111.155.82:30097',
      'properties.group.id' = 'ja-vtexplore-ceshi1',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1713341400000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );




-- 创建映射doris的全量数据表(Sink:doris)
drop table if exists dwd_ship_full_data;
create table dwd_ship_full_data(
                                   id        string,
                                   cjsj      string,
                                   mc        string,
                                   zxl       double,
                                   fx        double,
                                   jd        double,
                                   wd        double,
                                   sd        double,
                                   cs        double,
                                   gjdm      string,
                                   gjmc      string,
                                   hxzt      string,
                                   rksj      string
)WITH (
     'connector' = 'doris',
     'fenodes' = '47.92.158.88:8031',
     'table.identifier' = 'situation.dwd_ship_full_data',
     'username' = 'admin',
     'password' = 'dawu@110',
     'sink.enable.batch-mode'='true',
     'sink.buffer-flush.max-rows'='50000',
     'sink.buffer-flush.interval'='15s',
     'sink.properties.escape_delimiters' = 'false',
     'sink.properties.column_separator' = '\x01',     -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'         -- 行分隔符
     );


-- 创建映射doris的状态数据表(Sink:doris)
drop table if exists dws_ship_real_data;
create table dws_ship_real_data(
                                   id          string,    -- id
                                   cjsj        string, -- 采集时间
                                   mc          string, -- 名称
                                   zxl         double, -- 转向率
                                   fx          double, -- 方向
                                   jd          double, -- 经度
                                   wd          double, -- 纬度
                                   sd          double, -- 速度
                                   cs          double, -- 吃水
                                   gjdm        string, -- 国家代码
                                   gjmc        string, -- 国家名称
                                   hxzt        string, -- 航向状态
                                   rksj        string -- 入库时间
)WITH (
     'connector' = 'doris',
     'fenodes' = '47.92.158.88:8031',
     'table.identifier' = 'situation.dws_ship_real_data',
     'username' = 'admin',
     'password' = 'dawu@110',
     'sink.enable.batch-mode'='true',
     'sink.buffer-flush.max-rows'='50000',
     'sink.buffer-flush.interval'='15s',
     'sink.properties.escape_delimiters' = 'false',
     'sink.properties.column_separator' = '\x01',     -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'         -- 行分隔符
     );


-- 创建映射doris的实体表(Sink:doris)
drop table if exists dim_ship_info;
create table dim_ship_info(
                              id        string, -- id
                              mc        string, -- 名称
                              imo       string, -- imo
                              mmsi      string, -- mmsi
                              hh        string, -- 呼号
                              cd        double, -- 长度
                              kd        double, -- 宽度
                              gjdm      string, -- 国家代码
                              gjmc      string, -- 国家名称
                              rksj      string
)WITH (
     'connector' = 'doris',
     'fenodes' = '47.92.158.88:8031',
     'table.identifier' = 'situation.dim_ship_info',
     'username' = 'admin',
     'password' = 'dawu@110',
     'sink.enable.batch-mode'='true',
     'sink.buffer-flush.max-rows'='50000',
     'sink.buffer-flush.interval'='15s',
     'sink.properties.escape_delimiters' = 'false',
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
      'url' = 'jdbc:mysql://47.92.158.88:9031/situation?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'dawu@110',
      'table-name' = 'dim_mtf_vt_reletion_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
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
      'url' = 'jdbc:mysql://47.92.158.88:9031/situation?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'dawu@110',
      'table-name' = 'dim_vt_country_code_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );


-- 实体表(Source:doris)
drop table if exists dim_ship_info_source;
create table dim_ship_info_source (
                                      id    string  COMMENT '船编号',
                                      primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://47.92.158.88:9031/situation?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'dawu@110',
      'table-name' = 'dim_ship_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '1'
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
    if(tt.callsign <> '',tt.callsign,cast(null as varchar))   as hh,   -- 呼号
    from_unixtime(`timestamp`,'yyyy-MM-dd HH:mm:ss')          as cjsj, -- 采集时间戳格式化
    if(tt.name <> '',tt.name,cast(tt.name as varchar))        as mc,   -- vt船舶名称
    if(tt.country = '',cast(null as varchar),tt.country)      as gjmc, -- vt国家中文
    tt.longitude                                              as jd,  -- 经度
    tt.latitude                                               as wd,  -- 纬度
    tt.speed                                                  as sd,  -- 船舶的当前速度，以节（knots）为单位
    tt.course                                                 as fx,  -- 船舶的当前航向
    tt.draught                                                as cs,  -- 吃水
    cast(trim(split_index(tt.`size`,'*',0)) as double)        as cd,  -- 船舶的长度，以米为单位
    cast(trim(split_index(tt.`size`,'*',1)) as double)        as kd,  -- 船舶的宽度，以米为单位
    t3.country_code2                                          as gjdm, -- 船舶的国家或地区旗帜标识
    concat('cs',
           if(t2.vessel_id is not null,
              t2.vessel_id,
              cast((cast(tt.MMSI as bigint) + 4000000000) as varchar))
        ) as id,
    from_unixtime(unix_timestamp()) as rksj,
    cast(null as varchar) as hxzt,
    cast(null as double) as zxl,
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
insert into dim_ship_info
select
    t1.id       , -- id
    t1.mc       , -- 名称
    t1.imo      , -- imo
    t1.mmsi     , -- mmsi
    t1.hh       , -- 呼号
    t1.cd       , -- 长度
    t1.kd       , -- 宽度
    t1.gjdm     , -- 国家代码
    t1.gjmc     , -- 国家名称
    t1.rksj
from temp_01 as t1
         left join dim_ship_info_source
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.id = t2.id
where t2.id is null;


-- 全量表
insert into dwd_ship_full_data
select
    id,
    cjsj,
    mc,
    zxl,
    fx,
    jd,
    wd,
    sd,
    cs,
    gjdm,
    gjmc,
    hxzt,
    rksj
from temp_01;


-- 状态表
insert into dws_ship_real_data
select
    id,
    cjsj,
    mc,
    zxl,
    fx,
    jd,
    wd,
    sd,
    cs,
    gjdm,
    gjmc,
    hxzt,
    rksj
from temp_01;

end;

