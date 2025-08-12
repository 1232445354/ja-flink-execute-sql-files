--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/3/1 16:47:59
-- description: 临时性的代码，为了不影响原本的数据
--********************************************************************--

set 'pipeline.name' = 'ja-marinetraffic-list-rt-merge';


set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '10';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '300000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-marinetraffic-list-rt-merge';

-- 空闲分区不用等待
-- set 'table.exec.source.idle-timeout' = '3s';

-- set 'execution.type' = 'streaming';
-- set 'table.planner' = 'blink';
-- set 'table.exec.state.ttl' = '600000';
-- set 'sql-client.execution.result-mode' = 'TABLEAU';


-----------------------

 -- 数据结构

 -----------------------


-- 创建kafka全量marineTraffic数据来源的表（Source：kafka）
create table marinetraffic_ship_list(
                                        SHIP_ID              string, -- 船舶的唯一标识符
                                        SHIPNAME             string, -- 船舶的名称
                                        SPEED                string, -- 船舶的当前速度，以节（knots）为单位 10倍
                                        ROT                  string, -- 船舶的旋转率，转向率
                                        W_LEFT               string, -- 舶的左舷吃水线宽度
                                        L_FORE               string, -- 船舶的前吃水线长度
                                        `timeStamp`          bigint, -- 采集时间
                                        DESTINATION          string, -- 船舶的目的地
                                        LON                  string, -- 船舶当前位置的经度值
                                        ELAPSED              string, -- 自上次位置报告以来经过的时间，以分钟为单位。
                                        COURSE               string, -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方，
                                        GT_SHIPTYPE          string, -- 船舶的全球船舶类型码。
                                        FLAG                 string, -- 船舶的国家或地区旗帜标识。
                                        LAT                  string, -- 船舶当前位置的经度值。
                                        SHIPTYPE             string, -- 船舶的类型码，表示船舶所属的船舶类型。
                                        HEADING              string, -- 船舶的船首朝向
                                        LENGTH               string, -- 船舶的长度，以米为单位
                                        WIDTH                string, -- 船舶的宽度，以米为单位。
                                        DWT                  string, -- 船舶的载重吨位
                                        block_map_index      bigint, -- 地图分层
                                        block_range_x        bigint, -- x块
                                        block_range_y        bigint, -- y块
                                        proctime             as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'marinetraffic_ship_list',
      'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
      'properties.group.id' = 'marinetraffic_ship_list_2',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1708951969000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 新入的船舶没有详情的id打入kafka
drop table if exists marinetraffic_no_detail_list;
create table marinetraffic_no_detail_list(
                                             vessel_id            string, -- 船舶id
                                             update_time          string,  -- 时间
                                             primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'upsert-kafka',
      'topic' = 'marinetraffic_no_detail_list',
      'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
      'properties.group.id' = 'marinetraffic_ship_list_3',
      'key.format' = 'json',
      'value.format' = 'json'
      );


-- 创建映射doris的全量数据表(Sink:doris)
drop table if exists dwd_ais_vessel_all_info;
create table dwd_ais_vessel_all_info(
                                        vessel_id      	 				    bigint		comment '船ID',
                                        acquire_timestamp_format			string      comment '时间戳格式化',
                                        acquire_timestamp					bigint		comment '时间戳',
                                        vessel_name							string      comment '船名称',
                                        c_name                              string      comment '船中文名',
                                        imo                                 string      comment 'imo',
                                        mmsi                                string      comment 'mmsi',
                                        callsign                            string      comment '呼号',
                                        rate_of_turn						double  	comment '转向率',
                                        orientation							double 		comment '方向',
                                        master_image_id						bigint		comment '主图像ID',
                                        lng 					            double 		comment '经度',
                                        lat 					            double 		comment '纬度',
                                        source								string 		comment	'来源类型',
                                        speed								double 		comment '速度',
                                        speed_km		                    double      comment '速度 单位 km/h ',
                                        vessel_class						string		comment '船类型',
                                        vessel_class_name                   string      comment '船类型中文',
                                        vessel_type                         string      comment '船小类别',
                                        vessel_type_name                    string      comment '船类型中文名-小类',
                                        draught								double 		comment '吃水深度',
                                        cn_iso2								string		comment '国家code',
                                        country_name                        string      comment '国家中文',
    -- nation_flag_minio_url_jpg           string      comment '国旗',
                                        nav_status 							double 		comment '航行状态',
                                        nav_status_name                     string      comment '航行状态中文',
                                        dimensions_01						double 		comment '',
                                        dimensions_02						double 		comment '',
                                        dimensions_03						double 		comment '',
                                        dimensions_04						double 		comment '',
                                        block_map_index             		bigint      comment '图层层级',
                                        block_range_x             			double      comment '块x',
                                        block_range_y              			double      comment '块y',
                                        position_country_code2              string      comment '位置所在的国家',
                                        friend_foe 					        string      comment '敌我类型',
                                        sea_id 					            string      comment '海域编号',
                                        sea_name 					        string      comment '中文名称',
                                        update_time             			string      comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'sa.dwd_ais_vessel_all_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='15s',
     'sink.properties.escape_delimiters' = 'flase',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 创建映射doris的状态数据表(Sink:doris)
drop table  if exists dws_ais_vessel_status_info;
create table dws_ais_vessel_status_info(
                                           vessel_id      	 				    bigint		comment '船ID',
                                           acquire_timestamp_format			string      comment '时间戳格式化',
                                           acquire_timestamp					bigint		comment '时间戳',
                                           vessel_name							string      comment '船名称',
                                           c_name                              string      comment '船中文名',
                                           imo                                 string      comment 'imo',
                                           mmsi                                string      comment 'mmsi',
                                           callsign                            string      comment '呼号',
                                           rate_of_turn						double  	comment '转向率',
                                           orientation							double 		comment '方向',
                                           master_image_id						bigint		comment '主图像ID',
                                           lng 					            double 		comment '经度',
                                           lat 					            double 		comment '纬度',
                                           source								string 		comment	'来源类型',
                                           speed								double 		comment '速度',
                                           speed_km		                    double      comment '速度 单位 km/h ',
                                           vessel_class						string		comment '船类别',
                                           vessel_class_name                   string      comment '船类型中文',
                                           vessel_type                         string      comment '船小类别',
                                           vessel_type_name                    string      comment '船类型中文名-小类',
                                           draught								double 		comment '吃水深度',
                                           cn_iso2								string		comment '国家code',
                                           country_name                        string      comment '国家中文',
    -- nation_flag_minio_url_jpg           string      comment '国旗',
                                           nav_status 							double 		comment '航行状态',
                                           nav_status_name                     string      comment '航行状态中文',
                                           dimensions_01						double 		comment '',
                                           dimensions_02						double 		comment '',
                                           dimensions_03						double 		comment '',
                                           dimensions_04						double 		comment '',
                                           block_map_index             		bigint      comment '图层层级',
                                           block_range_x             			double      comment '块x',
                                           block_range_y              			double      comment '块y',
                                           position_country_code2              string      comment '位置所在的国家',
                                           friend_foe 					        string      comment '敌我类型',
                                           sea_id 					            string      comment '海域编号',
                                           sea_name 					        string      comment '中文名称',
                                           update_time             			string      comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'sa.dws_ais_vessel_status_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='15s',
     'sink.properties.escape_delimiters' = 'flase',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-- 船国家数据匹配库（Source：doris）
drop table if exists dim_country_code_name_info;
create table dim_country_code_name_info (
                                            id              string  comment 'id',
                                            source          string  comment '来源',
                                            e_name          string  comment '英文名称',
                                            c_name          string  comment '中文名称',
                                            country_code2   string  comment '国家2字代码',
                                            country_code3   string  comment '国家3字代码',
                                            flag_url        string  comment '国旗url',
                                            primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_code_name_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );


-- fleetmon、marinetraffic的船舶对应关系
create table dim_mt_fm_id_relation (
                                       ship_id                        bigint        comment '船编号',
                                       vessel_id                      bigint        comment '编号',
                                       vessel_type                    string        comment 'FleetMon 船大类',
                                       vessel_type_name               string        comment 'FleetMon 船大类中文',
                                       vessel_class                   string        comment 'FleetMon 船小类',
                                       vessel_class_name              string        comment 'FleetMon 船小类中文',
                                       c_name                         string        comment '船中文名',
                                       imo                            string        comment 'imo',
                                       mmsi                           bigint        comment 'mmsi',
                                       callsign                       string        comment '呼号',
                                       primary key (ship_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_mt_fm_id_relation',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '400000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '3'
      );



-- 海域
drop table if exists dim_sea_area;
create table dim_sea_area (
                              id 			string,  -- 海域编号
                              name 		string,  -- 名称
                              c_name 		string,  -- 中文名称
                              primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_sea_area',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );



-- 军事船舶数据
drop table if exists dws_vessle_nato_malitary;
create table dws_vessle_nato_malitary (
                                          id 			    bigint     ,
                                          navy_class 		string , -- 名称
                                          reletion        string , -- 对应的id
                                          remark          string , -- 备注 没有关联、关联上
                                          primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_vessle_nato_malitary',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );


-- 实体信息表数据
drop table if exists dws_ais_vessel_detail_static_attribute;
create table dws_ais_vessel_detail_static_attribute (
                                                        vessel_id 	     bigint,
                                                        imo              bigint,
                                                        mmsi             bigint,
                                                        callsign         string,
                                                        name             string,
                                                        c_name           string,
                                                        vessel_type      string,
                                                        vessel_type_name string,
                                                        vessel_class     string,
                                                        vessel_class_name string,
                                                        source           string,
                                                        primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_ais_vessel_detail_static_attribute',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '1'
      );



-- -- marinetraffic数据大类型对应关系（Source：doris）
-- drop table if exists dim_mtf_fm_class;
-- create table dim_mtf_fm_class (
--   type_id        int,       -- marinetraffic 大类型id
--   type_e_name    string,    -- marinetraffic 大类型英文名称
--   type_name      string,    -- marinetraffic 大类型中文名称
--   fm_type_id     int,       -- fleetmon 类型id
--   fm_class_code  string,    -- fleetmon 大类型英文编码
--   fm_class_name  string,    -- fleetmon 大类型中文编码
--   fm_type_code   string,    -- fleetmon 小类型编码
--   fm_type_name   string,    -- fleetmon 小类型名称
--   priority_flag  bigint,    -- 优先级标志，如果为1 则应该取该条数据的小类型
--   primary key (type_id) NOT ENFORCED
-- ) with (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
--     'username' = 'root',
--     'password' = 'Jingansi@110',
--     'table-name' = 'dim_mtf_fm_class',
--     'driver' = 'com.mysql.cj.jdbc.Driver',
--     'lookup.cache.max-rows' = '10000',
--     'lookup.cache.ttl' = '86400s',
--     'lookup.max-retries' = '1'
-- );



-- -- marinetraffic数据小类型对应关系（Source：doris）
-- drop table if exists dim_mtf_fm_type;
-- create table dim_mtf_fm_type (
--     type_specific_id      string,  -- marinetraffic 具体小类型编码
--     type_specific_name    string,  -- marinetraffic 具体小类型名称
--     type_id               string,  -- marinetraffic 大类型编码
--     type_name             string,  -- marinetraffic 大类型名称
--     fm_class_code         string,  -- fleetmon 大类型英文名称
--     fm_class_name         string,  -- fleetmon 大类型名称
--     fm_type_code          string,  -- fleetmon 小类型编码
--     fm_type_name          string,  -- fleetmon 小类型名称
--     priority_flag         bigint   -- 优先级标志，如果为1 则应该取该条数据的大类型
--   primary key (type_specific_id) NOT ENFORCED
-- ) with (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
--     'username' = 'root',
--     'password' = 'Jingansi@110',
--     'table-name' = 'dim_mtf_fm_type',
--     'driver' = 'com.mysql.cj.jdbc.Driver',
--     'lookup.cache.max-rows' = '10000',
--     'lookup.cache.ttl' = '86400s',
--     'lookup.max-retries' = '1'
-- );





-----------------------

-- 数据处理

-----------------------

-- create function getCountry as 'GetCountryFromLngLat.getCountryFromLngLat' language python ;
create function getCountry as 'com.jingan.udf.sea.GetCountryFromLngLat';
create function getSeaArea as 'com.jingan.udf.sea.GetSeaArea';



-- 关联数据 - 查看是否已经融合上的
drop view if exists tmp_marinetraffic_ship_list_01;
create view tmp_marinetraffic_ship_list_01 as
select
    t1.SHIP_ID                                as ship_id,              -- 船舶的唯一标识符
    t1.SHIPNAME                               as shipname,             -- 船舶的名称
    t1.`timeStamp`                            as acquire_timestamp,    -- 采集时间
    t1.ELAPSED                                as elapsed,               -- 自上次位置报告以来经过的时间，以分钟为单位。
    from_unixtime(`timeStamp`-(cast(ELAPSED as int) * 60),'yyyy-MM-dd HH:mm:00') as acquire_timestamp_format, -- 数据产生时间，爬虫采集的时间减 ELAPSED 经过的时间 ，格式化到分钟

    cast(t1.ROT as double)                    as rate_of_turn,         -- 船舶的旋转率，转向率
    cast(t1.COURSE as int)                    as orientation,          -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方，
    t1.lon_double                             as lng,                  -- 船舶当前位置的经度值
    t1.lat_double                             as lat,                  -- 船舶当前位置的纬度值
    cast(t1.SPEED as double)/10               as speed,                -- 船舶的当前速度，以节（knots）为单位 10倍
    t1.FLAG                                   as cn_iso2,              -- 船舶的国家或地区旗帜标识

    t1.GT_SHIPTYPE                            as gt_shiptype,          -- 船舶的全球船舶类型码（小类）
    t1.SHIPTYPE                               as shiptype,             -- 船舶的类型码，表示船舶所属的船舶类型 （大类）
    t1.HEADING                                as heading,              -- 船舶的船首朝向
    t1.LENGTH                                 as length,               -- 船舶的长度，以米为单位
    t1.WIDTH                                  as width,                -- 船舶的宽度，以米为单位
    t1.DWT                                    as dwt,                  -- 船舶的载重吨位
    t1.DESTINATION                            as destination,          -- 船舶的目的地
    t1.W_LEFT                                 as w_left,               -- 舶的左舷吃水线宽度
    t1.L_FORE                                 as l_fore,               -- 船舶的前吃水线长度

    t1.proctime,
    t1.block_map_index,
    t1.block_range_x,
    t1.block_range_y,

    t2.vessel_id                         as fleetmon_vessel_id,
    -- t2.c_name                         as fleetmon_c_name,
    -- t2.vessel_type                    as fleetmon_vessel_type,
    -- t2.vessel_type_name               as fleetmon_vessel_type_name,
    -- t2.vessel_class                   as fleetmon_vessel_class,
    -- t2.vessel_class_name              as fleetmon_vessel_class_name,
    -- t2.imo                            as fleetmon_imo,
    -- t2.mmsi                           as fleetmon_mmsi,
    -- t2.callsign                       as fleetmon_callsign,

    coalesce(t2.vessel_id,ship_id_bigint + 1000000000) as vessel_id,

    t3.e_name                         as country_e_name,
    t3.c_name                         as country_c_name,
    coalesce(t3.flag_url,'/ja-acquire-images/ja-vessels-nation-flag-jpg/none.jpg') as nation_flag_minio_url_jpg, -- 国旗url

    getCountry(lon_double,lat_double) as position_country_3code  -- 计算所处国家 - 经纬度位置转换国家

from (
         select
             *,
             cast(SHIP_ID as bigint) as ship_id_bigint,
             cast(LON as double)     as lon_double,
             cast(LAT as double)     as lat_double
         from marinetraffic_ship_list
         where `timeStamp` is not null
           and ELAPSED < 60
     ) t1

         left join dim_mt_fm_id_relation -- fleetmon对应关系
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on ship_id_bigint = t2.ship_id

         left join dim_country_code_name_info  -- 国家表
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.FLAG  = t3.country_code2
                       and 'COMMON' = t3.source;



-- 关联国家表，3字代码转换2字代码
-- 关联海域表，将海域id转换成海域名称
drop table if exists tmp_marinetraffic_ship_list_02;
create view tmp_marinetraffic_ship_list_02 as
select
    t1.vessel_id,
    t1.proctime,
    acquire_timestamp_format,
    acquire_timestamp,
    coalesce(t3.navy_class,t4.name,t1.shipname)      as vessel_name,
    t4.c_name                                        as c_name,
    cast(t4.imo as varchar)                          as imo,
    cast(t4.mmsi as varchar)                         as mmsi,
    t4.callsign,
    rate_of_turn,
    orientation,
    cast(null as bigint)           as master_image_id,    -- 主图像ID
    lng,
    lat,
    '2'                            as source,             -- 数据来源简称
    speed,
    speed * 1.852  as speed_km,                            -- 速度 km/h
    t4.vessel_class,                                       -- 大类型编码
    t4.vessel_class_name,                                  -- 大类型名称
    t4.vessel_type,                                        -- 小类型编码
    t4.vessel_type_name,                                   -- 小类型名称
    cast(null as double) as draught,
    cn_iso2,
    country_c_name as country_name,
    nation_flag_minio_url_jpg,
    cast(null as double)          as nav_status,           -- 航行状态
    cast(null as varchar)         as nav_status_name,      -- 航行状态名称
    cast(null as double)          as dimensions_01,
    cast(null as double)          as dimensions_02,
    cast(null as double)          as dimensions_03,
    cast(null as double)          as dimensions_04,
    block_map_index,
    block_range_x,
    block_range_y,
    position_country_3code,
    case
        when cn_iso2 in('IN','US','JP','AU') and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR') then 'ENEMY' -- 敌 - 美印日澳 军事
        when cn_iso2 ='CN' and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then 'OUR_SIDE'               -- 我 中国 军事
        when cn_iso2 ='CN' and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then 'FRIENDLY_SIDE'      -- 友 中国 非军事
        else 'NEUTRALITY'
        end as friend_foe,     -- 敌我
    t4.vessel_id as t4_vessel_id

from tmp_marinetraffic_ship_list_01 t1

         left join dws_vessle_nato_malitary    -- 军事船舶名单表
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on cast(t1.fleetmon_vessel_id as varchar)  = t3.reletion
                       and '关联上' = t3.remark

         left join dws_ais_vessel_detail_static_attribute   -- 实体信息
    FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.vessel_id = t4.vessel_id;


drop table if exists tmp_marinetraffic_ship_list_03;
create view tmp_marinetraffic_ship_list_03 as
select
    t1.*,
    t2.country_code2 as position_country_2code
from tmp_marinetraffic_ship_list_02 as t1
         left join dim_country_code_name_info
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.position_country_3code = t2.country_code3;



drop table if exists tmp_marinetraffic_ship_list_04;
create view tmp_marinetraffic_ship_list_04 as
select
    *,
    if(position_country_2code is null
           and ((lng between 107.491636 and 124.806089 and lat between 20.522241 and 40.799277)
            or
                (lng between 107.491636 and 121.433286 and lat between 3.011639 and 20.522241)
           )
        ,'CN', position_country_2code) as position_country_code2
from tmp_marinetraffic_ship_list_03;


drop table if exists tmp_marinetraffic_ship_list_05;
create view tmp_marinetraffic_ship_list_05 as
select
    t1.*,
    t2.c_name as sea_name
from (
         select
             *,
             getSeaArea(lng,lat) as sea_id
         from tmp_marinetraffic_ship_list_04 as t1
     ) as t1

         left join dim_sea_area   -- 海域名称
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.sea_id = t2.id;



-----------------------

-- 数据插入

-----------------------

begin statement set;

-- ais的全量数据入库
insert into dwd_ais_vessel_all_info
select
    vessel_id      	 					    ,
    acquire_timestamp_format				,
    acquire_timestamp						,
    vessel_name								,
    c_name                                  ,
    imo                                     ,
    mmsi                                    ,
    callsign                                ,
    rate_of_turn							,
    orientation								,
    master_image_id							,
    lng 						            ,
    lat 						            ,
    source									,
    speed									,
    speed_km                                ,
    vessel_class							,
    vessel_class_name                       ,
    vessel_type							    ,
    vessel_type_name                        ,
    draught			                        ,
    cn_iso2									,
    country_name                            ,
    -- nation_flag_minio_url_jpg               ,
    nav_status 								,
    nav_status_name                         ,
    dimensions_01							,
    dimensions_02							,
    dimensions_03							,
    dimensions_04							,
    block_map_index             			,
    block_range_x             				,
    block_range_y              				,
    position_country_code2                  ,
    friend_foe                              ,
    sea_id,
    sea_name,
    from_unixtime(unix_timestamp()) as update_time
from tmp_marinetraffic_ship_list_05;


-- ais的全量状态数据入库
insert into dws_ais_vessel_status_info
select
    vessel_id      	 					    ,
    acquire_timestamp_format				,
    acquire_timestamp						,
    vessel_name								,
    c_name                                  ,
    imo                                     ,
    mmsi                                    ,
    callsign                                 ,
    rate_of_turn							,
    orientation								,
    master_image_id							,
    lng 						            ,
    lat 						            ,
    source									,
    speed									,
    speed_km                                ,
    vessel_class							,
    vessel_class_name                       ,
    vessel_type							    ,
    vessel_type_name                        ,
    draught			                        ,
    cn_iso2									,
    country_name                            ,
    -- nation_flag_minio_url_jpg               ,
    nav_status 								,
    nav_status_name                         ,
    dimensions_01							,
    dimensions_02							,
    dimensions_03							,
    dimensions_04							,
    block_map_index             			,
    block_range_x             				,
    block_range_y              				,
    position_country_code2                  ,
    friend_foe                              ,
    sea_id,
    sea_name,
    from_unixtime(unix_timestamp()) as update_time
from tmp_marinetraffic_ship_list_05;


-- insert into marinetraffic_no_detail_list
--   select
--     cast(vessel_id - 1000000000 as varchar) as vessel_id,
--     from_unixtime(unix_timestamp()) as update_time
-- from tmp_marinetraffic_ship_list_05
--   where t4_vessel_id is null;

end;
