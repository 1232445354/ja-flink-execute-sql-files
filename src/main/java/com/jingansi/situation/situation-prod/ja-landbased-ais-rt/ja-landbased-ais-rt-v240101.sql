--********************************************************************--
-- author:      write your name here
-- create time: 2024/4/13 21:15:54
-- description: 岸基船舶轨迹
--********************************************************************--
set
''pipeline.name'' = ''ja-landbased-ais-rt'';

SET
''execution.type'' = ''streaming'';
SET
''table.planner'' = ''blink'';
SET
''table.exec.state.ttl'' = ''
300000'';
SET
''sql-client.execution.result-mode'' = ''TABLEAU'';

SET
''parallelism.default'' = ''
5'';
SET
''execution.checkpointing.interval'' = ''
600000'';
SET
''execution.checkpointing.timeout'' = ''
3600000'';
SET
''state.checkpoints.dir'' = ''s3://ja-flink/flink-checkpoints/ja-landbased-ais-rt'';


-----------------------

 -- 数据结构

 -----------------------


drop table if exists ais_landbased_list;
create table ais_landbased_list
(
    `type`            TINYINT COMMENT '' 类型标识 '',
    `forward`         TINYINT COMMENT '' 船首向，单位：度 '',
    `mmsi`            VARCHAR(20) COMMENT '' 海上移动服务身份码 '',
    `ver`             INT COMMENT '' 数据版本号 '',
    `imo`             VARCHAR(20) COMMENT '' 国际海事组织号码 '',
    `callno`          VARCHAR(20) COMMENT '' 船舶呼号 '',
    `shipname`        VARCHAR(50) COMMENT '' 船舶名称 '',
    `shipAndCargType` INT COMMENT '' 船舶和货物类型代码 '',
    `length`          DOUBLE COMMENT '' 船舶长度，单位：米 '',
    `width`           DOUBLE COMMENT '' 船舶宽度，单位：米 '',
    `devicetype`      TINYINT COMMENT '' 设备类型 '',
    `eta`             string COMMENT '' 预计到达时间 '',
    `dest`            VARCHAR(50) COMMENT '' 目的地 '',
    `draft`           DOUBLE COMMENT '' 船舶吃水深度，单位：米 '',
    `dte`             INT COMMENT '' 动态类型枚举 '',
    `receivetime`     BIGINT COMMENT '' 数据接收时间戳 '',
    `navistat`        TINYINT COMMENT '' 航行状态 '',
    `rot`             DOUBLE COMMENT '' 转向率，单位：度/分钟 '',
    `sog`             DOUBLE COMMENT '' 对地速度，单位：节 '',
    `posacur`         TINYINT COMMENT '' 位置准确性 '',
    `longitude`       DOUBLE COMMENT '' 经度 '',
    `latitude`        DOUBLE COMMENT '' 纬度 '',
    `cog`             INT COMMENT '' 对地航向，单位：度 '',
    `thead`           INT COMMENT '' 船首向真值，单位：度 '',
    `utctime`         INT COMMENT '' UTC时间差，单位：分钟 '',
    `indicator`       TINYINT COMMENT '' 指示器 '',
    `raim`            TINYINT COMMENT '' 雷达应答器状态 '',
    proctime as PROCTIME()
) with (
      '' connector '' = '' kafka '',
      '' topic '' = '' ais-landbased-list '',
      '' properties.bootstrap.servers '' = '' kafka.base.svc.cluster.local:9092 '',
      '' properties.group.id '' = '' ja-landbased-ais-rt-idc '',
      -- ''scan.startup.mode'' = ''group-offsets'',
      '' scan.startup.mode '' = '' latest- offset '',
      -- ''scan.startup.mode'' = ''timestamp'',
      -- ''scan.startup.timestamp-millis'' = ''0'',
      '' format '' = '' json '',
      '' json.fail- on -missing-field '' = '' false '',
      '' json.ignore-parse-errors '' = '' true ''
      );


-- 全量数据表
drop table if exists dwd_ais_landbased_vessel_list;
create table dwd_ais_landbased_vessel_list
(
    `mmsi`               bigint NULL COMMENT '' 海上移动服务身份码 '',
    `acquire_time`       string NULL COMMENT '' 时间戳格式化 '',
    `receive_time`       bigint NULL COMMENT '' 数据接收时间戳 '',
    `ship_name`          varchar(50) NULL COMMENT '' 船舶名称 '',
    `imo`                bigint NULL COMMENT '' 国际海事组织号码 '',
    `call_no`            varchar(20) NULL COMMENT '' 船舶呼号 '',
    `type`               int NULL COMMENT '' 类型标识 '',
    `ship_and_carg_type` int NULL COMMENT '' 船舶和货物类型代码 '',
    `device_type`        int NULL COMMENT '' 设备类型 '',
    `longitude`          double NULL COMMENT '' 经度 '',
    `latitude`           double NULL COMMENT '' 纬度 '',
    `sog`                double NULL COMMENT '' 对地速度，单位：节 '',
    `rot`                double NULL COMMENT '' 转向率，单位：度/分钟 '',
    `forward`            int NULL COMMENT '' 船首向，单位：度 '',
    `cog`                int NULL COMMENT '' 对地航向，单位：度 '',
    `thead`              int NULL COMMENT '' 船首向真值，单位：度 '',
    `draft`              double NULL COMMENT '' 船舶吃水深度，单位：米 '',
    `navi_stat`          int NULL COMMENT '' 航行状态 '',
    `eta`                string NULL COMMENT '' 预计到达时间 '',
    `dest`               varchar(50) NULL COMMENT '' 目的地 '',
    `dte`                int NULL COMMENT '' 动态类型枚举 '',
    `posacur`            int NULL COMMENT '' 位置准确性 '',
    `raim`               int NULL COMMENT '' 雷达应答器状态 '',
    `indicator`          int NULL COMMENT '' 指示器 '',
    `length`             double NULL COMMENT '' 船舶长度，单位：米 '',
    `width`              double NULL COMMENT '' 船舶宽度，单位：米 '',
    `ver`                int NULL COMMENT '' 数据版本号 '',
    `utc_time`           int NULL COMMENT '' UTC时间差，单位：分钟 '',
    `update_time`        string NULL COMMENT '' 数据入库时间 ''
) with (
      '' connector '' = '' doris '',
      '' fenodes '' = '' 172.21.30.245:8030 '',
      '' table.identifier '' = '' sa.dwd_ais_landbased_vessel_list '',
      '' username '' = '' admin '',
      '' password '' = '' Jingansi@110 '',
      '' doris.request.tablet.size '' = '' 5 '',
      '' doris.request.read.timeout.ms '' = '' 30000 '',
      '' sink.batch.size '' = '' 100000 '',
      '' sink.batch.interval '' = '' 20s ''
      );


-- 全量状态表 - 最后位置
drop table if exists dws_ais_landbased_vessel_status;
create table dws_ais_landbased_vessel_status
(
    `mmsi`               bigint NULL COMMENT '' 海上移动服务身份码 '',
    `acquire_time`       string NULL COMMENT '' 时间戳格式化 '',
    `receive_time`       bigint NULL COMMENT '' 数据接收时间戳 '',
    `ship_name`          varchar(50) NULL COMMENT '' 船舶名称 '',
    `imo`                bigint NULL COMMENT '' 国际海事组织号码 '',
    `call_no`            varchar(20) NULL COMMENT '' 船舶呼号 '',
    `type`               int NULL COMMENT '' 类型标识 '',
    `ship_and_carg_type` int NULL COMMENT '' 船舶和货物类型代码 '',
    `device_type`        int NULL COMMENT '' 设备类型 '',
    `longitude`          double NULL COMMENT '' 经度 '',
    `latitude`           double NULL COMMENT '' 纬度 '',
    `sog`                double NULL COMMENT '' 对地速度，单位：节 '',
    `rot`                double NULL COMMENT '' 转向率，单位：度/分钟 '',
    `forward`            int NULL COMMENT '' 船首向，单位：度 '',
    `cog`                int NULL COMMENT '' 对地航向，单位：度 '',
    `thead`              int NULL COMMENT '' 船首向真值，单位：度 '',
    `draft`              double NULL COMMENT '' 船舶吃水深度，单位：米 '',
    `navi_stat`          int NULL COMMENT '' 航行状态 '',
    `eta`                string NULL COMMENT '' 预计到达时间 '',
    `dest`               varchar(50) NULL COMMENT '' 目的地 '',
    `dte`                int NULL COMMENT '' 动态类型枚举 '',
    `posacur`            int NULL COMMENT '' 位置准确性 '',
    `raim`               int NULL COMMENT '' 雷达应答器状态 '',
    `indicator`          int NULL COMMENT '' 指示器 '',
    `length`             double NULL COMMENT '' 船舶长度，单位：米 '',
    `width`              double NULL COMMENT '' 船舶宽度，单位：米 '',
    `ver`                int NULL COMMENT '' 数据版本号 '',
    `utc_time`           int NULL COMMENT '' UTC时间差，单位：分钟 '',
    `update_time`        string NULL COMMENT '' 数据入库时间 ''
) with (
      '' connector '' = '' doris '',
      '' fenodes '' = '' 172.21.30.245:8030 '',
      '' table.identifier '' = '' sa.dws_ais_landbased_vessel_status '',
      '' username '' = '' admin '',
      '' password '' = '' Jingansi@110 '',
      '' doris.request.tablet.size '' = '' 5 '',
      '' doris.request.read.timeout.ms '' = '' 30000 '',
      '' sink.batch.size '' = '' 100000 '',
      '' sink.batch.interval '' = '' 20s ''
      );


-- 实体详情 - 合并的实体表
drop table if exists dws_ais_vessel_detail_static_attribute;
create table dws_ais_vessel_detail_static_attribute
(
    vessel_id         bigint comment '' 船ID '',
    imo               bigint comment '' IMO '',
    mmsi              bigint comment '' mmsi '',
    callsign          string comment '' 呼号 '',
    `name`            string comment '' 船名 '',
    length            double comment '' 长度 '',
    width             double comment '' 宽度 '',
    flag_country_code string comment '' 标志国家代码 '',
    country_name      string comment '' 国家中文 '',
    source            string comment '' 数据来源 '',
    vessel_type       string comment '' 船类型 '',
    vessel_type_name  string comment '' 船类别-小类中文 '',
    vessel_class      string comment '' 船类别 '',
    vessel_class_name string comment '' 船类型-大类中文 '',
    -- year_built                	double      comment ''建成年份'',
    -- service_status            	string	 	comment ''服务状态'',
    -- service_status_name         string      comment ''服务状态中文'',
    -- gross_tonnage             	double      comment ''总吨位'',
    -- deadweight               	double      comment ''重物，载重吨位'',
    -- `timestamp`					bigint 		comment ''采集船只详情数据时间'',
    -- height                   	double      comment ''高度'',
    -- draught_average           	double      comment ''吃水平均值'',
    -- speed_average             	double      comment ''速度平均值'',
    -- speed_max                 	double      comment ''速度最大值'',
    --  	`owner`             		string      comment ''所有者'',
    -- risk_rating               	string	 	comment ''风险评级'',
    --  	risk_rating_name            string      comment ''风险评级中文'',
    -- rate_of_turn 				double		comment ''转向率'',
    -- is_on_my_fleet				boolean		comment ''是我的船队'',
    -- is_on_shared_fleet			boolean		comment ''是否共享船队'',
    -- is_on_own_fleet				boolean 	comment ''是否拥有船队'',
    update_time       string comment '' 数据入库时间 ''
) with (
      '' connector '' = '' doris '',
      '' fenodes '' = '' 172.21.30.245:8030 '',
      '' table.identifier '' = '' sa.dws_ais_vessel_detail_static_attribute '',
      '' username '' = '' admin '',
      '' password '' = '' Jingansi@110 '',
      '' sink.batch.size '' = '' 100000 '',
      '' sink.batch.interval '' = '' 20s '',
      '' sink.properties.escape_delimiters '' = '' false '',
      '' sink.properties.column_separator '' = ''\x01 '', -- 列分隔符
      '' sink.properties.escape_delimiters '' = '' true '', -- 类似开启的意思
      '' sink.properties.line_delimiter '' = ''\x02 '' -- 行分隔符
      );


-- 海域
drop table if exists dim_sea_area;
create table dim_sea_area
(
    id     string, -- 海域编号
    name   string, -- 名称
    c_name string, -- 中文名称
    primary key (id) NOT ENFORCED
) with (
      '' connector '' = '' jdbc '',
      '' url '' = '' jdbc:mysql://172.21.30.245:9030/sa?useSSL = false &useUnicode = true &characterEncoding = UTF-8&characterSetResults = UTF-8&zeroDateTimeBehavior = CONVERT_TO_NULL&serverTimezone = UTC&autoReconnect = true '',
      '' username '' = '' root '',
      '' password '' = '' Jingansi@110 '',
      '' table - name '' = '' dim_sea_area '',
      '' driver '' = '' com.mysql.cj.jdbc.Driver '',
      '' lookup.cache.max- rows '' = '' 50000 '',
      '' lookup.cache.ttl '' = '' 86400s '',
      '' lookup.max-retries '' = '' 10 ''
      );


-- 船国家数据匹配库（Source：doris）
drop table if exists dim_country_code_name_info;
create table dim_country_code_name_info
(
    id            string comment '' id '',
    source        string comment '' 来源 '',
    e_name        string comment '' 英文名称 '',
    c_name        string comment '' 中文名称 '',
    country_code2 string comment '' 国家2字代码 '',
    country_code3 string comment '' 国家3字代码 '',
    flag_url      string comment '' 国旗url '',
    primary key (id) NOT ENFORCED
) with (
      '' connector '' = '' jdbc '',
      '' url '' = '' jdbc:mysql://172.21.30.245:9030/sa?useSSL = false &useUnicode = true &characterEncoding = UTF-8&characterSetResults = UTF-8&zeroDateTimeBehavior = CONVERT_TO_NULL&serverTimezone = UTC&autoReconnect = true '',
      '' username '' = '' root '',
      '' password '' = '' Jingansi@110 '',
      '' table - name '' = '' dim_country_code_name_info '',
      '' driver '' = '' com.mysql.cj.jdbc.Driver '',
      '' lookup.cache.max- rows '' = '' 50000 '',
      '' lookup.cache.ttl '' = '' 86400s '',
      '' lookup.max-retries '' = '' 10 ''
      );


-- 实体信息表数据
drop table if exists dws_ais_vessel_detail_static_attribute_source;
create table dws_ais_vessel_detail_static_attribute_source
(
    vessel_id         bigint,
    imo               bigint,
    mmsi              bigint,
    callsign          string,
    name              string,
    c_name            string,
    vessel_type       string,
    vessel_type_name  string,
    vessel_class      string,
    vessel_class_name string,
    source            string,
    flag_country_code string,
    country_name      string,
    length            double,
    width             double,
    height            double,
    primary key (vessel_id) NOT ENFORCED
) with (
      '' connector '' = '' jdbc '',
      '' url '' = '' jdbc:mysql://172.21.30.245:9030/sa?useSSL = false &useUnicode = true &characterEncoding = UTF-8&characterSetResults = UTF-8&zeroDateTimeBehavior = CONVERT_TO_NULL&serverTimezone = UTC&autoReconnect = true '',
      '' username '' = '' root '',
      '' password '' = '' Jingansi@110 '',
      '' table - name '' = '' dws_ais_vessel_detail_static_attribute '',
      '' driver '' = '' com.mysql.cj.jdbc.Driver '',
      '' lookup.cache.max- rows '' = '' 1000000 '',
      '' lookup.cache.ttl '' = '' 86400s '',
      '' lookup.max-retries '' = '' 10 ''
      );


-- marinetraffic、vt的船舶对应关系
drop table if exists dim_mtf_vt_reletion_info;
create table dim_mtf_vt_reletion_info
(
    vessel_id string comment '' 编号 '',
    vt_mmsi   string comment '' vt数据的mmsi '',
    merge_way string comment '' 融合方式 '',
    primary key (vt_mmsi) NOT ENFORCED
) with (
      '' connector '' = '' jdbc '',
      '' url '' = '' jdbc:mysql://172.21.30.245:9030/sa?useSSL = false &useUnicode = true &characterEncoding = UTF-8&characterSetResults = UTF-8&zeroDateTimeBehavior = CONVERT_TO_NULL&serverTimezone = UTC&autoReconnect = true '',
      '' username '' = '' admin '',
      '' password '' = '' Jingansi@110 '',
      '' table - name '' = '' dim_mtf_vt_reletion_info '',
      '' driver '' = '' com.mysql.cj.jdbc.Driver '',
      '' lookup.cache.max- rows '' = '' 1000000 '',
      '' lookup.cache.ttl '' = '' 86400s '',
      '' lookup.max-retries '' = '' 10 ''
      );


-- 船舶类型对应关系
drop table if exists dim_ais_lb_fm_type_info;
create table dim_ais_lb_fm_type_info
(
    `ship_and_carg_type` int NULL COMMENT '' 船舶和货物类型id '',
    `vessel_type`        varchar(20) NULL COMMENT '' 类型代码 '',
    `vessel_type_name`   varchar(20) NULL COMMENT '' 类型名称 '',
    `vessel_class`       varchar(20) NULL COMMENT '' 类别代码 '',
    `vessel_class_name`  varchar(20) NULL COMMENT '' 类别名称 '',
    primary key (ship_and_carg_type) NOT ENFORCED
) with (
      '' connector '' = '' jdbc '',
      '' url '' = '' jdbc:mysql://172.21.30.245:9030/sa?useSSL = false &useUnicode = true &characterEncoding = UTF-8&characterSetResults = UTF-8&zeroDateTimeBehavior = CONVERT_TO_NULL&serverTimezone = UTC&autoReconnect = true '',
      '' username '' = '' admin '',
      '' password '' = '' Jingansi@110 '',
      '' table - name '' = '' dim_ais_lb_fm_type_info '',
      '' driver '' = '' com.mysql.cj.jdbc.Driver '',
      '' lookup.cache.max- rows '' = '' 1000000 '',
      '' lookup.cache.ttl '' = '' 86400s '',
      '' lookup.max-retries '' = '' 10 ''
      );


-- 航行状态数据匹配库（Source：doris）
drop table if exists dim_vessel_nav_status_list;
create table dim_vessel_nav_status_list
(
    nav_status_num_code string comment '' 航向状态代码code '',
    nav_status_name     string comment '' 航行状态名称 '',
    primary key (nav_status_num_code) NOT ENFORCED
) with (
      '' connector '' = '' jdbc '',
      '' url '' = '' jdbc:mysql://172.21.30.245:9030/sa?useSSL = false &useUnicode = true &characterEncoding = UTF-8&characterSetResults = UTF-8&zeroDateTimeBehavior = CONVERT_TO_NULL&serverTimezone = UTC&autoReconnect = true '',
      '' username '' = '' root '',
      '' password '' = '' Jingansi@110 '',
      '' table - name '' = '' dim_vessel_nav_status_list '',
      '' driver '' = '' com.mysql.cj.jdbc.Driver '',
      '' lookup.cache.max- rows '' = '' 50000 '',
      '' lookup.cache.ttl '' = '' 86400s '',
      '' lookup.max-retries '' = '' 10 ''
      );


-- ****************************规则引擎写入数据******************************** --

drop table if exists vessel_source;
create table vessel_source
(
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

    -- masterImageId         bigint, -- 图像id
    -- dimensions01          double,
    -- dimensions02          double,
    -- dimensions03          double,
    -- dimensions04          double,
    blockMapIndex         bigint, -- 图层层级
    blockRangeX           bigint, -- 块x
    blockRangeY           bigint, -- 块y
    targetType            string, -- 实体类型 固定值 VESSEL
    ex_info               map<String,
    String>,                      -- 扩展信息
    updateTime            string  -- flink处理时间
) with (
      '' connector '' = '' kafka '',
      '' topic '' = '' vessel_source '',
      '' properties.bootstrap.servers '' = '' kafka.base.svc.cluster.local:9092 '',
      '' properties.group.id '' = '' lb_vessel_source_sink '',
      '' key.format '' = '' json '',
      '' key.fields '' = '' mmsi '',
      '' format '' = '' json ''
      );



-----------------------

-- 数据处理

-----------------------

create function getCountry as ''com.jingan.udf.sea.GetCountryFromLngLat'';
create function getSeaArea as ''com.jingan.udf.sea.GetSeaArea'';


-- 1.关联对应关系，2.关联船类型 3. 关联船航行状态
drop view if exists tmp_01;
create view tmp_01 as
select t1.mmsi                                                                      as mmsi,                   -- mmsi
       if(t1.imo in (''0 '', ''''), cast(null as string), t1.imo)                   as imo,                    -- imo
       if(t1.callno <> '''', t1.callno, cast(null as varchar))                      as callsign,-- 呼号
       t1.receivetime / 1000                                                        as acquire_timestamp,      -- 采集时间
       from_unixtime(receivetime / 1000)                                            as acquire_time,           -- 采集时间戳格式化
       if(t1.shipname <> '''', t1.shipname, cast(null as varchar))                  as name,                   -- 船舶名称
       t1.longitude                                                                 as lng,                    -- 经度
       t1.latitude                                                                  as lat,                    -- 纬度
       t1.sog / 10                                                                  as speed,                  -- 船舶的当前速度，以节（knots）为单位
       t1.sog / 10 * 1.852                                                          as speed_km,               -- 速度km/h
       t1.cog / 10                                                                  as orientation,            -- 船舶的当前航向
       t1.draft / 10                                                                as draught,                -- 吃水
       t1.shipAndCargType,                                                                                     -- 船舶和货物类型代码
       t5.vessel_class,                                                                                        -- 大类型编码
       t5.vessel_class_name,                                                                                   -- 大类型名称
       t5.vessel_type,                                                                                         -- 小类型编码
       t5.vessel_type_name,                                                                                    -- 小类型名称
       t1.rot                                                                       as rate_of_turn,           -- 转向率
       length                                                                       as length,                 -- 船舶的长度，以米为单位
       width                                                                        as width,                  -- 船舶的宽度，以米为单位
       t1.eta                                                                       as eta,                    -- 预计到岗时间
       t1.dest                                                                      as dest_name,              -- 目的地名称
       cast(t1.navistat as varchar)                                                 as nav_status,             -- 航行状态
       t6.nav_status_name                                                           as nav_status_name,        --航行状态名称
       coalesce(cast(t2.vessel_id as bigint), cast(t1.mmsi as bigint) + 4000000000) as vessel_id,              -- 之后入库的船舶id bigint
       getCountry(t1.longitude, t1.latitude)                                        as position_country_3code, -- 计算所处国家 - 经纬度位置转换国家
       getSeaArea(t1.longitude, t1.latitude)                                        as sea_id,                 -- 计算海域id
       MAP[''                                                                          type'', cast(type as string),
       ''                                                                              forward'', cast(forward as string),
       ''                                                                              ver'', cast(ver as string),
       ''                                                                              shipAndCargType'', cast(shipAndCargType as string),
       ''                                                                              devicetype'', cast(devicetype as string),
       ''                                                                              eta'', eta,
       ''                                                                              dest'', dest,
       -- ''draft'', cast(draft as string),
       ''                                                                              dte'', cast(dte as string),
       ''                                                                              posacur'', cast(posacur as string),
       ''                                                                              thead'', cast(thead as string),
       ''                                                                              utctime'', cast(utctime as string),
       ''indicator'', cast(`indicator` as string),
      ''raim'', cast(raim as string)
     ]   as ex_info,
  t1.proctime

from ais_landbased_list t1
  left join dim_mtf_vt_reletion_info FOR SYSTEM_TIME AS OF t1.proctime as t2 -- VT对应关系
    on t1.mmsi = t2.vt_mmsi

  left join dim_ais_lb_fm_type_info FOR SYSTEM_TIME AS OF t1.proctime as t5 -- 船舶类型转换
    on cast(t1.shipAndCargType as int) = t5.ship_and_carg_type

  left join dim_vessel_nav_status_list FOR SYSTEM_TIME AS OF t1.proctime as t6  -- 航行状态
    on t6.nav_status_num_code = cast(t1.navistat as string);



drop view if exists tmp_02;
create view tmp_02 as
select t1.vessel_id                                                        as id,                                                                                                                                                   -- bigint
       acquire_time                                                        as acquireTime,
       acquire_timestamp                                                   as acquireTimestamp,
       coalesce(t4.name, t1.name)                                          as vesselName,
       t4.c_name                                                           as cName,
       t1.mmsi                                                             as mmsi,
       coalesce(cast(t4.imo as varchar), t1.imo)                           as imo,
       coalesce(t4.callsign, t1.callsign)                                  as callsign,
       t4.flag_country_code                                                as cnIso2,
       if(t4.country_name <> '''', t4.country_name, cast(null as varchar)) as countryName,
       ''                                                                     lb''                                                as source,             -- 数据来源简称 coalesce(t4.vessel_class, t1.vessel_class) as vesselClass, -- 大类型编码
       coalesce(t4.vessel_class_name, t1.vessel_class_name)                as vesselClassName,                                                                                                                                      -- 大类型名称
       coalesce(t4.vessel_type, t1.vessel_type)                            as vesselType,                                                                                                                                           -- 小类型编码
       coalesce(t4.vessel_type_name, t1.vessel_type_name)                  as vesselTypeName,                                                                                                                                       -- 小类型名称
       t1.nav_status                                                       as navStatus,                                                                                                                                            -- 航行状态
       t1.nav_status_name                                                  as navStatusName,                                                                                                                                        --航行状态名称
       lng,
       lat,
       orientation,
       speed,
       speed_km                                                            as speedKm,
       rate_of_turn                                                        as rateOfTurn,
       t1.draught,-- 吃水
       coalesce(t4.length, cast(t1.length as double))                      as length,
       coalesce(t4.width, cast(t1.width as double))                        as width,
       t4.height                                                           as height,
       sea_id                                                              as seaId,
       t3.c_name                                                           as seaName,
       t4.vessel_id                                                        as t4_vessel_id,
       t1.name                                                             as sourceShipname,
       cast(null as varchar)                                               as sourceCountryCode,
       cast(null as varchar)                                               as sourceCountryName,
       t1.mmsi                                                             as sourceMmsi,
       t1.imo                                                              as sourceImo,
       t1.callsign                                                         as sourceCallsign,
       t1.vessel_class                                                     as sourceVesselClass,
       t1.vessel_class_name                                                as sourceVesselClassName,
       t1.vessel_type                                                      as sourceVesselType,
       t1.vessel_type_name                                                 as sourceVesselTypeName,
       t1.length                                                           as sourceLength,
       t1.width                                                            as sourceWidth,
       cast(null as double)                                                as sourceHeight,
       cast(null as bigint)                                                as blockMapIndex,
       cast(null as bigint)                                                as blockRangeX,
       cast(null as bigint)                                                as blockRangeY,

       case
           when t4.flag_country_code in (''IN'', ''US'', ''JP'', ''AU'') and t4.vessel_type in
                                                                             (''PTA'', ''FRT'', ''SRV'', ''CRO'',
                                                                              ''AMT'', ''FPS'', ''MOU'', ''DMN'',
                                                                              ''SMN'', ''PTH'', ''ICN'', ''ESC'',
                                                                              ''LCR'', ''VDO'', ''CGT'', ''COR'',
                                                                              ''DES'', ''AMR'')
               then ''ENEMY'' -- 敌 - 美印日澳 军事
           when t4.flag_country_code = ''CN'' and t4.vessel_type in
                                                  (''PTA'', ''FRT'', ''SRV'', ''CRO'', ''AMT'', ''FPS'', ''MOU'',
                                                   ''DMN'', ''SMN'', ''PTH'', ''ICN'', ''ESC'', ''LCR'', ''VDO'',
                                                   ''CGT'', ''COR'', ''DES'', ''AMR'') then ''OUR_SIDE''               -- 我 中国 军事
        when t4.flag_country_code =''CN'' and t4.vessel_type in (''PTA'',''FRT'',''SRV'',''CRO'',''AMT'',''FPS'',''MOU'',''DMN'',''SMN'',''PTH'',''ICN'',''ESC'',''LCR'',''VDO'',''CGT'',''COR'',''DES'',''AMR'')  then ''FRIENDLY_SIDE''      -- 友 中国 非军事
        else ''NEUTRALITY''
end
as friendFoe,

    if(t2.country_code2 is null
      and ((lng between 107.491636 and 124.806089 and lat between 20.522241 and 40.799277)
        or (lng between 107.491636 and 121.433286 and lat between 3.011639 and 20.522241)
       ),''CN'',t2.country_code2
   ) as positionCountryCode2  ,
    ex_info


from tmp_01 t1
  left join dws_ais_vessel_detail_static_attribute_source FOR SYSTEM_TIME AS OF t1.proctime as t4 -- 实体信息
    on t1.vessel_id = t4.vessel_id

    left join dim_country_code_name_info FOR SYSTEM_TIME AS OF t1.proctime as t2
  on t1.position_country_3code = t2.country_code3

  left join dim_sea_area
  FOR SYSTEM_TIME AS OF t1.proctime as t3
  on t1.sea_id = t3.id;


-----------------------

-- 数据插入

-----------------------

begin statement
set;


insert into dws_ais_vessel_detail_static_attribute
select id                              as vessel_id,
       cast(imo as bigint)             as imo,
       cast(mmsi as bigint)            as mmsi,
       callsign,
       vesselName                      as name,
       length,
       width,
       cnIso2                          as flag_country_code,
       countryName,
       source,
       vesselClass,
       vesselClassName,
       vesselType,
       vesselTypeName,
       from_unixtime(unix_timestamp()) as update_time
from tmp_02
where t4_vessel_id is null;



insert into dwd_ais_landbased_vessel_list
select cast(mmsi as bigint)              as mmsi,
       from_unixtime(receivetime / 1000) as acquire_time,
       receivetime                       as receive_time,
       shipname                          as ship_name,
       cast(imo as bigint)               as imo,
       callno                            as call_no,
       type,
       shipAndCargType                   as ship_and_carg_type,
       devicetype                        as device_type,
       longitude,
       latitude,
       sog,
       rot,
       forward,
       cog,
       thead,
       draft,
       navistat                          as navi_stat,
       eta,
       dest,
       dte,
       posacur,
       raim,
       `indicator`,
       length,
       width,
       ver,
       utctime,
       from_unixtime(unix_timestamp())   as update_time
from ais_landbased_list;


insert into dws_ais_landbased_vessel_status
select cast(mmsi as bigint)              as mmsi,
       from_unixtime(receivetime / 1000) as acquire_time,
       receivetime                       as receive_time,
       shipname                          as ship_name,
       cast(imo as bigint)               as imo,
       callno                            as call_no,
       type,
       shipAndCargType                   as ship_and_carg_type,
       devicetype                        as device_type,
       longitude,
       latitude,
       sog,
       rot,
       forward,
       cog,
       thead,
       draft,
       navistat                          as navi_stat,
       eta,
       dest,
       dte,
       posacur,
       raim,
       `indicator`,
       length,
       width,
       ver,
       utctime,
       from_unixtime(unix_timestamp())   as update_time
from ais_landbased_list;


-- ****************************规则引擎写入数据******************************** --

insert into vessel_source
select id,
       acquireTime,
       acquireTimestamp,
       vesselName,
       cName,
       mmsi,
       imo,
       callsign,
       cnIso2,
       countryName,
       source,
       vesselClass,
       vesselClassName,
       vesselType,
       vesselTypeName,
       friendFoe,
       positionCountryCode2,
       seaId,
       seaName,
       navStatus,
       navStatusName,
       lng,
       lat,
       orientation,
       speed,
       speedKm,
       rateOfTurn,
       draught,
       length,
       width,
       height,
       sourceShipname,
       sourceCountryCode,
       sourceCountryName,
       sourceMmsi,
       sourceImo,
       sourceCallsign,
       sourceVesselClass,
       sourceVesselClassName,
       sourceVesselType,
       sourceVesselTypeName,
       sourceLength,
       sourceWidth,
       sourceHeight,
       blockMapIndex,
       blockRangeX,
       blockRangeY,
       ''                                 VESSEL''                        as targetType, ex_info,
       from_unixtime(unix_timestamp()) as updateTime
from tmp_02;

end;


