--********************************************************************--
-- author:      write your name here
-- create time: 2024/9/3 17:58:28
-- description: write your description here
-- version: ja-vessel-fuse-rt-v250325
--********************************************************************--
set 'pipeline.name' = 'ja-vessel-fuse-rt';

set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '5';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '600000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-fuse-checkpoints/ja-vessel-fuse-rt';


-----------------------

 -- 数据结构

 -----------------------

-- 船舶数据
create table vessel_source(
                              id                    bigint, -- id
                              acquireTime           string, -- 采集事件年月日时分秒
                              acquireTimestamp      bigint, -- 采集时间戳
                              vesselName            string, -- 船舶名称,写出给规则引擎
                              cName                 string, -- 船舶中文名称
                              mmsi                  string, -- mmsi
                              imo                   string, -- imo
                              callsign              string, -- 呼号
                              cnIso2                string, -- 国家代码
                              countryName           string, -- 国家名称
                              source                string, -- 来源
                              vesselClass           string, -- 大类型编码
                              vesselClassName       string, -- 大类型名称
                              vesselType            string, -- 小类型编码
                              vesselTypeName        string, -- 小类型名称
                              friendFoe             string, -- 敌我代码
                              positionCountryCode2  string, -- 所处国家
                              seaId                 string, -- 海域id
                              seaName               string, -- 海域名称
                              navStatus             string, -- 航行状态
                              navStatusName         string, -- 航行状态名称
                              lng                   double, -- 经度
                              lat                   double, -- 纬度
                              orientation           double, -- 方向
                              speed                 double, -- 速度 节
                              speedKm               double, -- 速度 km
                              rateOfTurn            double, -- 转向率
                              draught               double, -- 吃水
                              length                double, -- 长度
                              width                 double, -- 宽度
                              height                double, -- 高度
                              sourceShipname        string,
                              sourceCountryCode     string,
                              sourceCountryName     string,
                              sourceMmsi            string,
                              sourceImo             string,
                              sourceCallsign        string,
                              sourceVesselClass     string,
                              sourceVesselClassName string,
                              sourceVesselType      string,
                              sourceVesselTypeName  string,
                              sourceLength          double,
                              sourceWidth           double,
                              sourceHeight          double,
                              blockMapIndex         bigint, -- 图层层级
                              blockRangeX           bigint, -- 块x
                              blockRangeY           bigint, -- 块y
                              targetType            string, -- 实体类型 固定值 VESSEL
                              updateTime            string,-- flink处理时间
                              ex_info               map<String,String>, -- 扩展信息
                              proctime          as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'vessel_source',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-vessel-fuse-rt',
      'scan.startup.mode' = 'group-offsets',
      -- 'properties.auto.offset.reset' = 'earliest',
      -- 'scan.startup.mode' = 'latest-offset',
      --'scan.startup.mode' = 'timestamp',
      --  'scan.startup.timestamp-millis' = '1725336000000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 中间kafka缓存表
drop table if exists vessel_fuse_list;
create table vessel_fuse_list(
                                 vessel_id                      bigint        comment '船舶编号（主键）',
                                 acquire_time                   string        comment '采集时间',
                                 src_code                       int           comment '来源1:fleetmon 2:marinetraffic 3:vt 4:岸基',
                                 src_pk                         string        comment '源网站主键',
                                 vessel_name                    string        comment '船名称',
                                 mmsi                           bigint        comment 'mmsi',
                                 imo                            bigint        comment 'imo',
                                 callsign                       string        comment '呼号',
                                 lng                            double        comment '经度',
                                 lat                            double        comment '纬度',
                                 speed                          double        comment '速度，单位节',
                                 speed_km                       double        comment '速度 单位 km/h',
                                 rate_of_turn                   double        comment '转向率',
                                 orientation                    double        comment '方向',
                                 heading                        double        comment '船舶的船首朝向',
                                 draught                        double        comment '吃水',
                                 nav_status                     double        comment '航行状态',
                                 eta                            string        comment '预计到港口时间',
                                 dest_code                      string        comment '目的地代码',
                                 dest_name                      string        comment '目的地名称英文',
                                 ais_type_code                  int           comment 'ais 船舶类型',
                                 big_type_num_code              int           comment '船舶类型代码--大类',
                                 small_type_num_code            int           comment '船舶类型代码--小类',
                                 length                         double        comment '船舶长度，单位：米',
                                 width                          double        comment '船舶宽度，单位：米',
                                 ais_source_type                int           comment 'ais信号来源类型 1 岸基 2 卫星',
                                 flag_country                   string        comment '旗帜国家',
                                 block_map_index                bigint        comment '图层层级',
                                 block_range_x                  double        comment '块x',
                                 block_range_y                  double        comment '块y',
                                 position_country_code2         string        comment '位置所在的国家',
                                 sea_id                         string        comment '海域编号',
                                 query_cols                     string        comment '需要查询字段  拼接',
                                 extend_info                    string        comment '扩展信息 json 串，每个网站特有的信息,扩展字段只能做展示不能做筛选',
                                 country_name                   string,
                                 b_vessel_id               BIGINT,
                                 b_vessel_name             STRING, -- 船舶英文名称
                                 b_mmsi                    BIGINT, -- mmsi
                                 b_imo                     BIGINT, -- imo
                                 b_callsign                STRING, -- 呼号
                                 b_length                  DOUBLE, -- 长度
                                 b_width                   DOUBLE, -- 宽度
                                 b_country_code            STRING, -- 国家编码
                                 b_ais_type_code           INT, -- ais 船舶类型
                                 b_update_time             TIMESTAMP(3),
                                 src_pks                   STRING,
                                 updateTime                string,-- flink处理时间
                                 proctime             as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'vessel-fuse-list',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-vessel-fuse-rt',
      'scan.startup.mode' = 'group-offsets',
      -- 'properties.auto.offset.reset' = 'earliest',
      -- 'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1725336000000',
      'format' = 'json',
      'key.format' = 'json',
      'key.fields' = 'vessel_id',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 结果表
create table dws_vessel_rl_src_id (
                                      vessel_id                      bigint        comment '船舶编号（主键）',
                                      src_code                       int           comment '来源1:fleetmon 2:marinetraffic 3:vt 4:岸基',
                                      src_pk                         string        comment '源网站主键',
                                      update_time                    string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.244:8030,172.21.30.245:8030,172.21.30.246:8030',
      'table.identifier' = 'sa.dws_vessel_rl_src_id',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='30000',
      'sink.batch.interval'='15s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );

create table dws_vessel_et_info_rt (
                                       vessel_id                      bigint        comment '船舶编号（主键）',
                                       vessel_name                    string        comment '船舶英文名称',
                                       mmsi                           bigint        comment 'mmsi',
                                       imo                            bigint        comment 'imo',
                                       callsign                       string        comment '呼号',
                                       length                         double        comment '长度 / m',
                                       width                          double        comment '宽度 / m',
                                       country_code                   string        comment '国家编码',
                                       country_name                   string        comment '国家名称',
                                       ais_type_code                  int           comment 'ais 船舶类型',
                                       ais_type_name                  string        comment 'ais 船舶类型名称',
                                       search_content                 string        comment '搜索字段 将所有搜索值放在该字段，建立倒排索引',
                                       friend_foe                     string        comment '敌我类型',
                                       update_time                    string        comment '自动更新的时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.244:8030,172.21.30.245:8030,172.21.30.246:8030',
      'table.identifier' = 'sa.dws_vessel_et_info_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='30000',
      'sink.batch.interval'='15s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


drop table if exists vessel_vote_id;
create table vessel_vote_id(
                               marinetrafficId       string, -- marinetraffic船舶id
                               mmsi                  string, -- mmsi 多个网站lb、vt
                               newVesselId           string, -- 数据入库的id
                               updateTime            string -- flink处理时间
) with (
      'connector' = 'kafka',
      'topic' = 'vessel_vote_id',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-vessel-fuse-rt',
      'key.format' = 'json',
      'key.fields' = 'newVesselId',
      'value.format' ='json',
      'format' = 'json'
      );


drop table if exists dws_vessel_bhv_track_rt;
create table dws_vessel_bhv_track_rt(
                                        vessel_id                      bigint        comment '船舶编号（主键）',
                                        acquire_time                   string        comment '采集时间',
                                        src_code                       int           comment '来源1:fleetmon 2:marinetraffic 3:vt 4:岸基',
                                        src_pk                         string        comment '源网站主键',
                                        vessel_name                    string        comment '船名称',
                                        mmsi                           bigint        comment 'mmsi',
                                        imo                            bigint        comment 'imo',
                                        callsign                       string        comment '呼号',
                                        lng                            double        comment '经度',
                                        lat                            double        comment '纬度',
                                        speed                          double        comment '速度，单位节',
                                        speed_km                       double        comment '速度 单位 km/h',
                                        rate_of_turn                   double        comment '转向率',
                                        orientation                    double        comment '方向',
                                        heading                        double        comment '船舶的船首朝向',
                                        draught                        double        comment '吃水',
                                        nav_status                     double        comment '航行状态',
                                        eta                            string        comment '预计到港口时间',
                                        dest_code                      string        comment '目的地代码',
                                        dest_name                      string        comment '目的地名称英文',
                                        ais_type_code                  int           comment 'ais 船舶类型',
                                        big_type_num_code              int           comment '船舶类型代码--大类',
                                        small_type_num_code            int           comment '船舶类型代码--小类',
                                        length                         double        comment '船舶长度，单位：米',
                                        width                          double        comment '船舶宽度，单位：米',
                                        ais_source_type                int           comment 'ais信号来源类型 1 岸基 2 卫星',
                                        flag_country                   string        comment '旗帜国家',
                                        block_map_index                bigint        comment '图层层级',
                                        block_range_x                  double        comment '块x',
                                        block_range_y                  double        comment '块y',
                                        position_country_code2         string        comment '位置所在的国家',
                                        sea_id                         string        comment '海域编号',
                                        query_cols                     string        comment '需要查询字段  拼接',
                                        extend_info                    string        comment '扩展信息 json 串，每个网站特有的信息,扩展字段只能做展示不能做筛选',
                                        update_time                    string        comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.21.30.244:8030,172.21.30.245:8030,172.21.30.246:8030',
     'table.identifier' = 'sa.dws_vessel_bhv_track_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='5',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='30000',
     'sink.batch.interval'='15s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


drop table if exists dws_vessel_bhv_status_rt;
create table dws_vessel_bhv_status_rt(
                                         vessel_id                      bigint        comment '船舶编号（主键）',
                                         acquire_time                   string        comment '采集时间',
                                         src_code                       int           comment '来源1:fleetmon 2:marinetraffic 3:vt 4:岸基',
                                         src_pk                         string        comment '源网站主键',
                                         vessel_name                    string        comment '船名称',
                                         mmsi                           bigint        comment 'mmsi',
                                         imo                            bigint        comment 'imo',
                                         callsign                       string        comment '呼号',
                                         lng                            double        comment '经度',
                                         lat                            double        comment '纬度',
                                         speed                          double        comment '速度，单位节',
                                         speed_km                       double        comment '速度 单位 km/h',
                                         rate_of_turn                   double        comment '转向率',
                                         orientation                    double        comment '方向',
                                         heading                        double        comment '船舶的船首朝向',
                                         draught                        double        comment '吃水',
                                         nav_status                     double        comment '航行状态',
                                         eta                            string        comment '预计到港口时间',
                                         dest_code                      string        comment '目的地代码',
                                         dest_name                      string        comment '目的地名称英文',
                                         ais_type_code                  int           comment 'ais 船舶类型',
                                         big_type_num_code              int           comment '船舶类型代码--大类',
                                         small_type_num_code            int           comment '船舶类型代码--小类',
                                         length                         double        comment '船舶长度，单位：米',
                                         width                          double        comment '船舶宽度，单位：米',
                                         ais_source_type                int           comment 'ais信号来源类型 1 岸基 2 卫星',
                                         flag_country                   string        comment '旗帜国家',
                                         block_map_index                bigint        comment '图层层级',
                                         block_range_x                  double        comment '块x',
                                         block_range_y                  double        comment '块y',
                                         position_country_code2         string        comment '位置所在的国家',
                                         sea_id                         string        comment '海域编号',
                                         query_cols                     string        comment '需要查询字段  拼接',
                                         extend_info                    string        comment '扩展信息 json 串，每个网站特有的信息,扩展字段只能做展示不能做筛选',
                                         update_time                    string        comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.21.30.244:8030,172.21.30.245:8030,172.21.30.246:8030',
     'table.identifier' = 'sa.dws_vessel_bhv_status_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='5',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='30000',
     'sink.batch.interval'='15s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 维表
drop table if exists dws_vessel_rl_src_ids_agg;
create table dws_vessel_rl_src_ids_agg (
                                           vessel_id                      bigint        comment '船舶编号（主键）',
                                           src_pks                         string        comment '源网站主键',
                                           primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.244:9030,172.21.30.245:9030,172.21.30.246:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_vessel_rl_src_ids',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '1000000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );

-- drop table if exists dim_vessel_rl_src_id;
-- create table dim_vessel_rl_src_id (
--     vessel_id                      bigint        comment '船舶编号（主键）',
--     src_code                       int           comment '来源1:fleetmon 2:marinetraffic 3:vt 4:岸基',
--     src_pk                         string        comment '源网站主键',
--     primary key (vessel_id) NOT ENFORCED
-- ) with (
--       'connector' = 'jdbc',
--       'url' = 'jdbc:mysql://172.21.30.244:9030,172.21.30.245:9030,172.21.30.246:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
--       'username' = 'root',
--       'password' = 'Jingansi@110',
--       'table-name' = 'dws_vessel_rl_src_id',
--       'driver' = 'com.mysql.cj.jdbc.Driver',
--       'lookup.cache.max-rows' = '1000000',
--       'lookup.cache.ttl' = '86400s',
--       'lookup.max-retries' = '10'
--   );

create table dim_vessel_et_info_rt (
                                       vessel_id                      bigint        comment '船舶编号（主键）',
                                       vessel_name                    string        comment '船舶英文名称',
                                       mmsi                           bigint        comment 'mmsi',
                                       imo                            bigint        comment 'imo',
                                       callsign                       string        comment '呼号',
                                       length                         double        comment '长度 / m',
                                       width                          double        comment '宽度 / m',
                                       country_code                   string        comment '国家编码',
                                       ais_type_code                  int           comment 'ais 船舶类型',
                                       update_time                    TIMESTAMP(3)  comment '自动更新的时间',
                                       primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.244:9030,172.21.30.245:9030,172.21.30.246:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_vessel_et_info_rt',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '1000000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );

drop table if exists dim_ais_lb_fm_type_info;
create table dim_ais_lb_fm_type_info (
                                         `ship_and_carg_type` int NULL COMMENT '船舶和货物类型id',
                                         `type_name` varchar(20) NULL COMMENT '船舶和货物类型名称',
                                         primary key (ship_and_carg_type) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.244:9030,172.21.30.245:9030,172.21.30.246:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_ais_lb_fm_type_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '500',
      'lookup.cache.ttl' = '864000s',
      'lookup.max-retries' = '10'
      );




create function getVesselId as 'com.jingan.udf.vessel.GetVesselId';

-- create function passThrough as 'com.jingan.udtf.PassThroughUdtf';

-----------------------

-- 数据处理

-----------------------
drop view if exists tmp_mtf_01;
create view tmp_mtf_01 as
select
    id                     , -- 船舶编号（主键）
    acquireTime as acquire_time                   , -- 采集时间
    2 as src_code                       , -- 来源1:fleetmon 2:marinetraffic 3:vt 4:岸基
    ex_info['SHIP_ID'] as src_pk                         , -- 源网站主键
    nullif(sourceShipname,'') as vessel_name                    , -- 船名称
    cast(sourceMmsi as bigint) as mmsi                           , -- mmsi
    cast(sourceImo as bigint) as imo                            , -- imo
    nullif(sourceCallsign,'') as callsign                       , -- 呼号
    lng as lng                            , -- 经度
    lat as lat                            , -- 纬度
    speed as speed                          , -- 速度，单位节
    round(speedKm,4) as speed_km                       , -- 速度 单位 km/h
    rateOfTurn as rate_of_turn                   , -- 转向率
    orientation as orientation                    , -- 方向
    cast(ex_info['HEADING'] as double) as heading                        , -- 船舶的船首朝向
    round(draught,4) as draught                        , -- 吃水
    cast(navStatus as int) as nav_status                     , -- 航行状态
    cast(null as string) as eta                            , -- 预计到港口时间
    cast(null as string) as dest_code                      , -- 目的地代码
    if(ex_info['DESTINATION']='CLASS B' or ex_info['DESTINATION']='',cast(null as string),ex_info['DESTINATION']) as dest_name                      , -- 目的地名称英文
    cast(null as int) as ais_type_code                  , -- ais 船舶类型
    cast(ex_info['SHIPTYPE'] as int) as big_type_num_code              , -- 船舶类型代码 大类
    cast(ex_info['GT_SHIPTYPE'] as int) as small_type_num_code            , -- 船舶类型代码 小类
    sourceLength as length                         , -- 船舶长度 单位：米
    sourceWidth as width                          , -- 船舶宽度 单位：米
    cast(null as int) as ais_source_type                , -- ais信号来源类型 1 岸基 2 卫星
    cnIso2 as flag_country                   , -- 旗帜国家
    countryName as country_name,
    blockMapIndex as block_map_index                , -- 图层层级
    blockRangeX as block_range_x                  , -- 块x
    blockRangeY as block_range_y                  , -- 块y
    positionCountryCode2 as position_country_code2         , -- 位置所在的国家
    seaId as sea_id                        ,  -- 海域编号
    proctime as proctime,
    concat('{',
           concat_ws(',',
                     concat('"w_left":',nullif(ex_info['W_LEFT'],'')),
                     concat('"l_fore":',nullif(ex_info['L_FORE'],'')),
                     concat('"dwt":',nullif(ex_info['DWT'],'')),
                     concat('"invalid_dimensions":',nullif(ex_info['INVALID_DIMENSIONS'],''))
               ),
           '}') as extend_info                     -- 扩展信息 json 串 每个网站特有的信息,扩展字段只能做展示不能做筛选
from vessel_source
where source='2';


drop view if exists tmp_vt_01;
create view tmp_vt_01 as
select
    id                     , -- 船舶编号（主键）
    acquireTime as acquire_time                   , -- 采集时间
    3 as src_code                       , -- 来源1:fleetmon 2:marinetraffic 3:vt 4:岸基
    sourceMmsi as src_pk                         , -- 源网站主键
    nullif(sourceShipname,'') as vessel_name                    , -- 船名称
    cast(sourceMmsi as bigint)  as mmsi                           , -- mmsi
    cast(sourceImo  as bigint) as imo                            , -- imo
    nullif(sourceCallsign,'') as callsign                       , -- 呼号
    lng as lng                            , -- 经度
    lat as lat                            , -- 纬度
    speed as speed                          , -- 速度，单位节
    round(speedKm,4) as speed_km                       , -- 速度 单位 km/h
    rateOfTurn as rate_of_turn                   , -- 转向率
    orientation as orientation                    , -- 方向
    cast(null as double) as heading                        , -- 船舶的船首朝向
    round(draught,4) as draught                        , -- 吃水
    cast(null as int) as nav_status                     , -- 航行状态
    case  -- 转换时间
        when ex_info['ETA'] = '' or ex_info['ETA'] is null
            then cast(null as string)
        when substring(acquireTime, 6, 2)='12' and substring(ex_info['ETA'],1, 2) in ('01','02','03')
            then concat(cast((cast(substring(acquireTime, 1, 4) as int)+1) as string),'-',substring(ex_info['ETA'],1, 2),'-',substring(ex_info['ETA'],3),':00')
        when substring(acquireTime, 6, 2) in ('01','02') and substring(ex_info['ETA'],1, 2) in ('11','12')
            then concat(cast((cast(substring(acquireTime, 1, 4) as int)-1) as string),'-',substring(ex_info['ETA'],1, 2),'-',substring(ex_info['ETA'],3),':00')
        else concat(substring(acquireTime, 1, 4),'-',substring(ex_info['ETA'],1, 2),'-',substring(ex_info['ETA'],3),':00')
        end as eta                            , -- 预计到港口时间
    nullif(ex_info['destination_code'],'') as dest_code                      , -- 目的地代码
    nullif(ex_info['destination_name'],'')  as dest_name                      , -- 目的地名称英文
    cast(ex_info['type'] as int) as ais_type_code                  , -- ais 船舶类型
    cast(null as int) as big_type_num_code              , -- 船舶类型代码--大类
    cast(null as int) as small_type_num_code            , -- 船舶类型代码--小类
    sourceLength as length                         , -- 船舶长度，单位：米
    sourceWidth as width                          , -- 船舶宽度，单位：米
    if(ex_info['destination_name']='true',2,1) as ais_source_type                , -- ais信号来源类型 1 岸基 2 卫星
    cnIso2 as flag_country                   , -- 旗帜国家
    countryName as country_name,
    blockMapIndex as block_map_index                , -- 图层层级
    blockRangeX as block_range_x                  , -- 块x
    blockRangeY as block_range_y                  , -- 块y
    positionCountryCode2 as position_country_code2         , -- 位置所在的国家
    seaId as sea_id                        ,  -- 海域编号
    proctime as proctime,
    cast(null as string) as extend_info                     -- 扩展信息 json 串 每个网站特有的信息,扩展字段只能做展示不能做筛选
from vessel_source
where source='vt';


drop view if exists tmp_lb_01;
create view tmp_lb_01 as
select
    id                      , -- 船舶编号（主键）
    acquireTime as acquire_time                   , -- 采集时间
    4 as src_code                       , -- 来源1:fleetmon 2:marinetraffic 3:vt 4:岸基
    sourceMmsi as src_pk                         , -- 源网站主键
    nullif(sourceShipname,'') as vessel_name                    , -- 船名称
    cast(sourceMmsi as bigint)  as mmsi                           , -- mmsi
    cast(sourceImo as bigint)  as imo                            , -- imo
    nullif(sourceCallsign,'') as callsign                       , -- 呼号
    lng as lng                            , -- 经度
    lat as lat                            , -- 纬度
    speed as speed                          , -- 速度，单位节
    round(speedKm,4) as speed_km                       , -- 速度 单位 km/h
    rateOfTurn as rate_of_turn                   , -- 转向率
    orientation as orientation                    , -- 方向
    cast(ex_info['thead'] as double) as heading                        , -- 船舶的船首朝向
    round(draught,4) as draught                        , -- 吃水
    cast(navStatus as int) as nav_status                     , -- 航行状态
    case  -- 转换时间
        when ex_info['eta'] = '' or ex_info['eta'] is null
            then cast(null as string)
        when substring(acquireTime, 6, 2)='12' and substring(ex_info['eta'],1, 2) in ('01','02','03')
            then date_format(to_timestamp(concat(cast((cast(substring(acquireTime, 1, 4) as int)+1) as string),ex_info['eta'],'00'),'yyyyMMdd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')
        when substring(acquireTime, 6, 2) in ('01','02') and substring(ex_info['eta'],1, 2) in ('11','12')
            then date_format(to_timestamp(concat(cast((cast(substring(acquireTime, 1, 4) as int)-1) as string),ex_info['eta'],'00'),'yyyyMMdd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')
        else date_format(to_timestamp(concat(substring(acquireTime, 1, 4),ex_info['eta'],'00'),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss')
        end as eta                            , -- 预计到港口时间
    cast(null as string) as dest_code                      , -- 目的地代码
    ex_info['dest']  as dest_name                      , -- 目的地名称英文
    cast(ex_info['shipAndCargType'] as int) as ais_type_code                  , -- ais 船舶类型
    cast(ex_info['type'] as int) as big_type_num_code              , -- 船舶类型代码--大类
    cast(ex_info['devicetype'] as int) as small_type_num_code            , -- 船舶类型代码--小类
    sourceLength as length                         , -- 船舶长度，单位：米
    sourceWidth as width                          , -- 船舶宽度，单位：米
    cast(null as int) as ais_source_type                , -- ais信号来源类型 1 岸基 2 卫星
    cnIso2 as flag_country                   , -- 旗帜国家
    countryName as country_name,
    blockMapIndex as block_map_index                , -- 图层层级
    blockRangeX as block_range_x                  , -- 块x
    blockRangeY as block_range_y                  , -- 块y
    positionCountryCode2 as position_country_code2         , -- 位置所在的国家
    seaId as sea_id                        ,  -- 海域编号
    proctime as proctime,
    concat('{',
           concat_ws(',',
                     concat('"forward":',nullif(ex_info['forward'],'')),
                     concat('"ver":',nullif(ex_info['ver'],'')),
                     concat('"dte":',nullif(ex_info['dte'],'')),
                     concat('"posacur":',nullif(ex_info['posacur'],'')),
                     concat('"utctime":',nullif(ex_info['utctime'],'')),
                     concat('"indicator":',nullif(ex_info['indicator'],'')),
                     concat('"raim":',nullif(ex_info['raim'],''))
               ),
           '}') as extend_info                     -- 扩展信息 json 串 每个网站特有的信息,扩展字段只能做展示不能做筛选
from vessel_source
where source='lb';



drop view if exists tmp_vessel_fuse_list_01;
create view tmp_vessel_fuse_list_01 as
select
    a.*,
    getVesselId(id,vessel_name,imo,mmsi,callsign,cast(acquire_time as TIMESTAMP),lng,lat) as vessel_id,
    concat(
            ifnull(cast(lng as string),''),'¥',
            ifnull(cast(lat as string),''),'¥',
            ifnull(cast(speed as string),''),'¥',
            ifnull(cast(rate_of_turn as string),''),'¥',
            ifnull(cast(orientation as string),''),'¥',
            ifnull(cast(heading as string),''),'¥',
            ifnull(cast(draught as string),''),'¥',
            ifnull(cast(nav_status as string),''),'¥',
            ifnull(cast(position_country_code2 as string),''),'¥',
            ifnull(cast(sea_id as string),'')
        )  as query_cols -- ,
    -- from_unixtime(unix_timestamp()) as update_time
from (
         select * from tmp_mtf_01
         union all
         select * from tmp_vt_01
         union all
         select * from tmp_lb_01
     ) a;

drop view if exists tmp_vessel_fuse_list_02;
create view tmp_vessel_fuse_list_02 as
select
    a.vessel_id              as vessel_id                      , -- 船舶编号（主键）',
    a.acquire_time           as acquire_time                   , -- 采集时间',
    a.src_code               as src_code                       , -- 来源1:fleetmon 2:marinetraffic 3:vt 4:岸基',
    a.src_pk                 as src_pk                         , -- 源网站主键',
    a.vessel_name            as vessel_name                    , -- 船名称',
    a.mmsi                   as mmsi                           , -- mmsi',
    a.imo                    as imo                            , -- imo',
    a.callsign               as callsign                       , -- 呼号',
    a.lng                    as lng                            , -- 经度',
    a.lat                    as lat                            , -- 纬度',
    a.speed                  as speed                          , -- 速度，单位节',
    a.speed_km               as speed_km                       , -- 速度 单位 km/h',
    a.rate_of_turn           as rate_of_turn                   , -- 转向率',
    a.orientation            as orientation                    , -- 方向',
    a.heading                as heading                        , -- 船舶的船首朝向',
    a.draught                as draught                        , -- 吃水',
    a.nav_status             as nav_status                     , -- 航行状态',
    a.eta                    as eta                            , -- 预计到港口时间',
    a.dest_code              as dest_code                      , -- 目的地代码',
    a.dest_name              as dest_name                      , -- 目的地名称英文',
    a.ais_type_code          as ais_type_code                  , -- ais 船舶类型',
    a.big_type_num_code      as big_type_num_code              , -- 船舶类型代码--大类',
    a.small_type_num_code    as small_type_num_code            , -- 船舶类型代码--小类',
    a.length                 as length                         , -- 船舶长度，单位：米',
    a.width                  as width                          , -- 船舶宽度，单位：米',
    a.ais_source_type        as ais_source_type                , -- ais信号来源类型 1 岸基 2 卫星',
    a.flag_country           as flag_country                   , -- 旗帜国家',
    a.block_map_index        as block_map_index                , -- 图层层级',
    a.block_range_x          as block_range_x                  , -- 块x',
    a.block_range_y          as block_range_y                  , -- 块y',
    a.position_country_code2 as position_country_code2         , -- 位置所在的国家',
    a.sea_id                 as sea_id                         , -- 海域编号',
    a.query_cols             as query_cols                     , -- 需要查询字段  拼接',
    a.extend_info            as extend_info                    , -- 扩展信息 json 串，每个网站特有的信息,扩展字段只能做展示不能做筛选',
    a.country_name           as country_name,
    b.vessel_id              as b_vessel_id,
    b.vessel_name            as b_vessel_name                    , -- 船舶英文名称
    b.mmsi                   as b_mmsi                           , -- mmsi
    b.imo                    as b_imo                            , -- imo
    b.callsign               as b_callsign                       , -- 呼号
    b.length                 as b_length                         , -- 长度
    b.width                  as b_width                          , -- 宽度
    b.country_code           as b_country_code                   , -- 国家编码
    b.ais_type_code          as b_ais_type_code                  , -- ais 船舶类型
    b.update_time            as b_update_time,
    c.src_pks                as src_pks
from tmp_vessel_fuse_list_01 a
         left join dim_vessel_et_info_rt FOR SYSTEM_TIME AS OF a.proctime as b --
                   on a.vessel_id=b.vessel_id
         left join dws_vessel_rl_src_ids_agg FOR SYSTEM_TIME AS OF a.proctime as c -- VT对应关系
                   on a.vessel_id=c.vessel_id;






-- -- 关联实体表
-- drop view if exists tmp_vessel_et_01;
-- create view tmp_vessel_et_01 as
-- select
--     a.vessel_id     as vessel_id,
--     a.vessel_name   as a_vessel_name                    , -- 船舶英文名称
--     a.mmsi          as a_mmsi                           , -- mmsi
--     a.imo           as a_imo                            , -- imo
--     a.callsign      as a_callsign                       , -- 呼号
--     a.length        as a_length                         , -- 长度
--     a.width         as a_width                          , -- 宽度
--     a.flag_country  as a_country_code                   , -- 国家编码
--     a.country_name  as country_name,
--     a.ais_type_code as a_ais_type_code                  , -- ais 船舶类型
--     b.vessel_id     as b_vessel_id,
--     b.vessel_name   as b_vessel_name                    , -- 船舶英文名称
--     b.mmsi          as b_mmsi                           , -- mmsi
--     b.imo           as b_imo                            , -- imo
--     b.callsign      as b_callsign                       , -- 呼号
--     b.length        as b_length                         , -- 长度
--     b.width         as b_width                          , -- 宽度
--     b.country_code  as b_country_code                   , -- 国家编码
--     b.ais_type_code as b_ais_type_code                  , -- ais 船舶类型
--     a.acquire_time  as acquire_time,
--     a.proctime      as proctime,
--     b.update_time   as b_update_time
-- from vessel_fuse_list a
-- left join dim_vessel_et_info_rt FOR SYSTEM_TIME AS OF a.proctime as b --
-- on a.vessel_id=b.vessel_id ;




-- 投票 的kafka topic
drop view if exists tmp_vessel_vote_id_01;
create view tmp_vessel_vote_id_01 as
select
    src_pks as marinetrafficId,
    cast(mmsi as string) as mmsi,
    cast(vessel_id as string) as newVesselId
from vessel_fuse_list
where b_vessel_id is not null
  and timestampdiff(SECOND, b_update_time, to_timestamp(acquire_time, 'yyyy-MM-dd HH:mm:ss')) > 259200 -- 大于3天
  and (
        (vessel_name is not null and vessel_name<>'' and vessel_name <> b_vessel_name)
        or (imo is not null and imo<>0 and imo <> b_imo)
        or (callsign is not null and callsign<>'' and callsign <> b_callsign)
        or (length is not null and length <> b_length)
        or (width is not null and width <> b_width)
        or (flag_country is not null and flag_country<>'' and flag_country <> b_country_code)
        or (ais_type_code is not null and ais_type_code <> b_ais_type_code)
    );






-- 对应关系表
drop view if exists tmp_vessel_rl_src_id_01;
create view tmp_vessel_rl_src_id_01 as
select
    a.vessel_id                     , -- 船舶编号（主键）
    a.src_code                      , -- 来源1:fleetmon 2:marinetraffic 3:vt 4:岸基
    a.src_pk                          -- 源网站主键
from vessel_fuse_list a
where src_pks is null
   or instr(src_pks,src_pk)=0 ;




-- 实体信息表
drop view if exists tmp_vessel_et_02;
create view tmp_vessel_et_02 as
select
    vessel_id,
    a.vessel_name as vessel_name                   , -- 船舶英文名称
    a.mmsi as mmsi                      , -- mmsi
    a.imo as imo                          , -- imo
    a.callsign as callsign                       , -- 呼号
    a.length   as length                      , -- 长度
    a.width  as width                         , -- 宽度
    a.flag_country  as country_code                  , -- 国家编码
    a.country_name as country_name,
    a.ais_type_code as ais_type_code,                   -- ais 船舶类型
    b.type_name as ais_type_name,                   -- ais 船舶类型
    concat_ws(' ',a.vessel_name,cast(a.mmsi as string),cast(a.imo as string),a.callsign) as search_content                 , -- 搜索字段 将所有搜索值放在该字段，建立倒排索引
    case
        when a.flag_country in('IN','US','JP','AU','TW') and a.ais_type_code=35 then '1' -- 敌 - 美印日澳 军事
        when a.flag_country ='CN' and a.ais_type_code=35  then '2'               -- 我 中国 军事
        when a.flag_country ='CN' and a.ais_type_code<>35 then '3'      -- 友 中国 非军事
        else '4'
        end as friend_foe                      -- 敌我类型
from vessel_fuse_list a
         left join dim_ais_lb_fm_type_info FOR SYSTEM_TIME AS OF a.proctime as b --
                   on a.ais_type_code=b.ship_and_carg_type
where b_vessel_id is null;

-- 轨迹表

drop view if exists tmp_vessel_list_02;
create view tmp_vessel_list_02 as
select
    vessel_id                      , -- 船舶编号（主键）',
    acquire_time                   , -- 采集时间',
    src_code                       , -- 来源1:fleetmon 2:marinetraffic 3:vt 4:岸基',
    src_pk                         , -- 源网站主键',
    vessel_name                    , -- 船名称',
    mmsi                           , -- mmsi',
    imo                            , -- imo',
    callsign                       , -- 呼号',
    lng                            , -- 经度',
    lat                            , -- 纬度',
    speed                          , -- 速度，单位节',
    speed_km                       , -- 速度 单位 km/h',
    rate_of_turn                   , -- 转向率',
    orientation                    , -- 方向',
    heading                        , -- 船舶的船首朝向',
    draught                        , -- 吃水',
    nav_status                     , -- 航行状态',
    eta                            , -- 预计到港口时间',
    dest_code                      , -- 目的地代码',
    dest_name                      , -- 目的地名称英文',
    ais_type_code                  , -- ais 船舶类型',
    big_type_num_code              , -- 船舶类型代码--大类',
    small_type_num_code            , -- 船舶类型代码--小类',
    length                         , -- 船舶长度，单位：米',
    width                          , -- 船舶宽度，单位：米',
    ais_source_type                , -- ais信号来源类型 1 岸基 2 卫星',
    flag_country                   , -- 旗帜国家',
    block_map_index                , -- 图层层级',
    block_range_x                  , -- 块x',
    block_range_y                  , -- 块y',
    position_country_code2         , -- 位置所在的国家',
    sea_id                         , -- 海域编号',
    query_cols                     , -- 需要查询字段  拼接',
    extend_info                     -- 扩展信息 json 串，每个网站特有的信息,扩展字段只能做展示不能做筛选',
from vessel_fuse_list;


-----------------------

-- 数据入库

-----------------------

begin statement set;

insert into vessel_fuse_list
select
    *,
    from_unixtime(unix_timestamp()) as update_time
from tmp_vessel_fuse_list_02;
;



-- 关系表
insert into dws_vessel_rl_src_id
select
    *,
    from_unixtime(unix_timestamp()) as update_time
from tmp_vessel_rl_src_id_01;

-- 投票表
insert  into vessel_vote_id
select
    *,
    from_unixtime(unix_timestamp()) as update_time
from tmp_vessel_vote_id_01;


-- 实体表
insert  into dws_vessel_et_info_rt
select
    *,
    from_unixtime(unix_timestamp()) as update_time
from tmp_vessel_et_02;

-- 轨迹表
insert  into dws_vessel_bhv_track_rt
select
    *,
    from_unixtime(unix_timestamp()) as update_time
from tmp_vessel_list_02;

-- 状态表
insert  into dws_vessel_bhv_status_rt
select
    *,
    from_unixtime(unix_timestamp()) as update_time
from tmp_vessel_list_02;

end;

