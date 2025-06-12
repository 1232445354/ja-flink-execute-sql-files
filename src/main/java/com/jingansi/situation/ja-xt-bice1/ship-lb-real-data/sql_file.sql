--********************************************************************--
-- author:      write your name here
-- create time: 2024/4/13 21:15:54
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ship-lb-real-data';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'parallelism.default' = '4';
SET 'execution.checkpointing.interval' = '600000';
SET 'execution.checkpointing.timeout' = '3600000';
SET 'state.checkpoints.dir' = 's3://ja-bice2/flink-checkpoints/ship-lb-real-data';


create table ais_landbased_list(
                                   `type` TINYINT COMMENT '类型标识',
                                   `forward` TINYINT COMMENT '船首向，单位：度',
                                   `mmsi` VARCHAR(20) COMMENT '海上移动服务身份码',
                                   `ver` INT COMMENT '数据版本号',
                                   `imo` VARCHAR(20) COMMENT '国际海事组织号码',
                                   `callno` VARCHAR(20) COMMENT '船舶呼号',
                                   `shipname` VARCHAR(50) COMMENT '船舶名称',
                                   `shipAndCargType` INT COMMENT '船舶和货物类型代码',
                                   `length` DOUBLE COMMENT '船舶长度，单位：米',
                                   `width` DOUBLE COMMENT '船舶宽度，单位：米',
                                   `devicetype` TINYINT COMMENT '设备类型',
                                   `eta` string COMMENT '预计到达时间',
                                   `dest` VARCHAR(50) COMMENT '目的地',
                                   `draft` DOUBLE COMMENT '船舶吃水深度，单位：米',
                                   `dte` INT COMMENT '动态类型枚举',
                                   `receivetime` BIGINT COMMENT '数据接收时间戳',
                                   `navistat` TINYINT COMMENT '航行状态',
                                   `rot` DOUBLE COMMENT '转向率，单位：度/分钟',
                                   `sog` DOUBLE COMMENT '对地速度，单位：节',
                                   `posacur` TINYINT COMMENT '位置准确性',
                                   `longitude` DOUBLE COMMENT '经度',
                                   `latitude` DOUBLE COMMENT '纬度',
                                   `cog` INT COMMENT '对地航向，单位：度',
                                   `thead` INT COMMENT '船首向真值，单位：度',
                                   `utctime` INT COMMENT 'UTC时间差，单位：分钟',
                                   `indicator` TINYINT COMMENT '指示器',
                                   `raim` TINYINT COMMENT '雷达应答器状态',
                                   proctime          as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'ais-landbased-list',
      'properties.bootstrap.servers' = '115.231.236.106:30090',
      'properties.group.id' = 'ja-lb-ceshi2',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
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


-- 实体详情 - 合并的实体表
create table dws_ship_entity_info (
        id      	 			string	    comment '船ID',
        imo      				string      comment 'IMO',
        mmsi                    string      comment 'mmsi',
        callsign 				string      comment '呼号',
        ship_name 				string      comment '船名',
        `length`                double      comment '长度',
        width                   double      comment '宽度',
        country_flag          	string      comment '标志国家代码',
        country_chinese_name    string      comment '国家中文',
        big_type                string      comment '船类型-大类中文',
        small_type              string      comment '船类别-小类中文',
        source_type         	string      comment '数据来源',
        gmt_create              string      comment '数据入库时间'
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


-- 实体表(Source:doris)
drop table if exists dws_ship_entity_info_source;
create table dws_ship_entity_info_source (
    id         string  COMMENT '船编号',
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


-- 类型对应关系
create table dim_data_type_rele_lb_info (
        `type_code`           int     comment '船舶和货物类型id',
        `small_type_code`     string  comment '类型代码',
        `small_type`          string  comment '类型名称',
        `big_type_code`       string  comment '类别代码',
        `big_type`            string  comment '类别名称',
        primary key (type_code) NOT ENFORCED
) with (
        'connector' = 'jdbc',
        'url' = 'jdbc:mysql://8.130.39.51:9030/global_entity?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
        'username' = 'root',
        'password' = 'yshj@yshj',
        'table-name' = 'dim_data_type_rele_lb_info',
        'driver' = 'com.mysql.cj.jdbc.Driver',
        'lookup.cache.max-rows' = '100000',
        'lookup.cache.ttl' = '86400s',
        'lookup.max-retries' = '10'
);


-- 航行状态对应表
create table dim_navigation_status_info (
       navigation_status_code           string        comment '航向状态代码code',
       navigation_status                string        comment '航行状态名称',
        primary key (navigation_status_code) NOT ENFORCED
) with (
        'connector' = 'jdbc',
        'url' = 'jdbc:mysql://8.130.39.51:9030/global_entity?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
        'username' = 'root',
        'password' = 'yshj@yshj',
        'table-name' = 'dim_navigation_status_info',
        'driver' = 'com.mysql.cj.jdbc.Driver',
        'lookup.cache.max-rows' = '100000',
        'lookup.cache.ttl' = '86400s',
        'lookup.max-retries' = '10'
    );


-----------------------
-- 数据处理
-----------------------


-- 关联数据 - 查看是否已经融合上的
create view temp_01 as
select
    t1.mmsi                                                 as mmsi,-- mmsi
    if(t1.imo in ('0',''),cast(null as string),t1.imo)      as imo, -- imo
    if(t1.callno <> '',t1.callno,cast(null as varchar))     as callsign,-- 呼号
    from_unixtime(receivetime/1000)                         as time1, -- 采集时间戳格式化
    if(t1.shipname <> '',t1.shipname,cast(null as varchar)) as ship_name,  -- 船舶名称
    t1.longitude                                            as longitude02,  -- 经度
    t1.latitude                                             as latitude02,  -- 纬度
    t1.sog/10                                               as speed_kn,  -- 船舶的当前速度，以节（knots）为单位
    t1.cog/10                                               as heading,  -- 船舶的当前航向
    t1.draft/10                                             as draft,  -- 吃水
    t5.big_type                                             as big_type,-- 大类型名称
    t5.small_type                                           as small_type,-- 小类型名称
    t1.rot                                                  as turning_rate,-- 转向率
    t1.length                                               as length,-- 船舶的长度，以米为单位
    t1.width                                                as width,-- 船舶的宽度，以米为单位
    t6.navigation_status                                    as navigation_status, --航行状态名称
    cast(null as varchar)                                   as country_flag, --国家代码
    cast(null as varchar)                                   as country_chinese_name, -- 国家名称
    t1.proctime,
    from_unixtime(unix_timestamp()) as gmt_create,
    concat('cs',
           if(t2.vessel_id is not null,
              t2.vessel_id,
              cast((cast(t1.mmsi as bigint) + 4000000000) as varchar))
        ) as id,
    'lb' as source_type

from ais_landbased_list t1
         left join dim_mtf_vt_reletion_info   -- 对应关系
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.mmsi = t2.vt_mmsi

         left join dim_data_type_rele_lb_info    -- 船舶类型转换
    FOR SYSTEM_TIME AS OF t1.proctime as t5
                   on cast(t1.shipAndCargType as int) = t5.type_code

         left join dim_navigation_status_info    -- 航行状态
    FOR SYSTEM_TIME AS OF t1.proctime as t6
                   on t6.navigation_status_code = cast(t1.navistat as string);


-----------------------

-- 数据插入

-----------------------

begin statement set;


-- 实体表
insert into dws_ship_entity_info
select
    t1.id                    , -- id
    t1.imo                   , -- imo
    t1.mmsi                  , -- mmsi
    t1.callsign              , -- 呼号
    t1.ship_name             , -- 名称
    t1.length                , -- 长度
    t1.width                 , -- 宽度
    t1.country_flag          , -- 国家代码
    t1.country_chinese_name  , -- 国家名称
    t1.big_type              , -- 船类型-大类中文
    t1.small_type            , -- 船类别-小类中文
    t1.source_type           , -- 来源
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


