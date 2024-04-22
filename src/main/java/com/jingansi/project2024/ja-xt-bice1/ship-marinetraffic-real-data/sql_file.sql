
set 'pipeline.name' = 'ship-real-data';

set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '2';

-- checkpoint的时间和位置
-- set 'execution.checkpointing.interval' = '300000';
-- set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-marinetraffic-list-rt-merge';


 -- 数据结构
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
                                        ROT                  string -- 船舶的旋转率，转向率
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
    -- proctime             as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'marinetraffic_ship_list',
      'properties.bootstrap.servers' = '47.111.155.82:30097',
      'properties.group.id' = 'marinetraffic_ship_list_bice1',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1711980000000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 创建映射doris的全量数据表(Sink:doris)
drop table if exists dwd_ship_full_data;
create table dwd_ship_full_data(
                                   id      	string,	-- id
                                   cjsj			string, -- 采集时间
                                   mc				string, -- 名称
                                   zxl				double, -- 转向率
                                   fx				double, -- 方向
                                   jd 				double, -- 经度
                                   wd 				double, -- 纬度
                                   sd				double, -- 速度
                                   cs        double, -- 吃水
                                   gjdm			string, -- 国家代码
                                   gjmc      string, -- 国家名称
                                   hxzt      string, -- 航向状态
                                   rksj      string -- 入库时间
)WITH (
     'connector' = 'doris',
     'fenodes' = '47.92.158.88:8031',
     'table.identifier' = 'situation.dwd_ship_full_data',
     'username' = 'admin',
     'password' = 'dawu@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='15s',
     'sink.properties.escape_delimiters' = 'false',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 创建映射doris的状态数据表(Sink:doris)
drop table if exists dws_ship_real_data;
create table dws_ship_real_data(
                                   id      	string,	-- id
                                   cjsj			string, -- 采集时间
                                   mc				string, -- 名称
                                   zxl				double, -- 转向率
                                   fx				double, -- 方向
                                   jd 				double, -- 经度
                                   wd 				double, -- 纬度
                                   sd				double, -- 速度
                                   cs        double, -- 吃水
                                   gjdm			string, -- 国家代码
                                   gjmc      string, -- 国家名称
                                   hxzt      string, -- 航向状态
                                   rksj      string -- 入库时间
)WITH (
     'connector' = 'doris',
     'fenodes' = '47.92.158.88:8031',
     'table.identifier' = 'situation.dws_ship_real_data',
     'username' = 'admin',
     'password' = 'dawu@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='15s',
     'sink.properties.escape_delimiters' = 'false',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- ---------------------

-- 数据处理

-- ---------------------

-- 筛选数据处理
drop view if exists temp_01;
create view temp_01 as
select
    concat('cs',SHIP_ID) as ship_id,              -- 船舶的唯一标识符
    SHIPNAME                                as shipname,             -- 船舶的名称
    from_unixtime(`timeStamp`-(cast(ELAPSED as int)*60),'yyyy-MM-dd HH:mm:00') as acquire_timestamp_format, -- 数据产生时间，爬虫采集的时间减 ELAPSED 经过的时间 ，格式化到分钟
    cast(ROT as double)                     as rate_of_turn,         -- 船舶的旋转率，转向率
    cast(COURSE as double)                  as orientation,          -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方，
    cast(LON as double)                    as lng,                  -- 船舶当前位置的经度值
    cast(LAT as double)                    as lat,                  -- 船舶当前位置的纬度值
    cast(SPEED as double)/10               as speed,                -- 船舶的当前速度，以节（knots）为单位 10倍
    FLAG                                   as cn_iso2              -- 船舶的国家或地区旗帜标识
from marinetraffic_ship_list
where `timeStamp` is not null
  and ELAPSED < 60;


-- 规范化字段
drop view if exists temp_02;
create view temp_02 as
select
    ship_id as id,
    acquire_timestamp_format as cjsj,
    shipname as mc,
    rate_of_turn as zxl,
    orientation as fx,
    lng as jd,
    lat as wd,
    speed as sd,
    cast(null as double) as cs,
    cn_iso2 as gjdm,
    cast(null as varchar) as gjmc,
    cast(null as varchar) as hxzt,
    from_unixtime(unix_timestamp()) as update_time
from temp_01;


-----------------------

-- 数据插入

-----------------------

begin statement set;

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
from temp_02;


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
from temp_02;

end;
