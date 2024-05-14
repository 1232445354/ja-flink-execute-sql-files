--********************************************************************--
-- author:      write your name here
-- create time: 2023/5/14 18:50:16
-- description: write your description here
--********************************************************************--
set
''pipeline.name'' = ''ja-ais-vessel-detail-port-rt'';


-- set ''parallelism.default'' = ''2'';
set
''execution.type'' = ''streaming'';
set
''table.planner'' = ''blink'';
-- set ''table.exec.state.ttl'' = ''600000'';
set
''sql-client.execution.result-mode'' = ''TABLEAU'';

-- checkpoint的时间和位置
set
''execution.checkpointing.interval'' = ''
100000'';
set
''state.checkpoints.dir'' = ''s3://ja-flink/flink-checkpoints/ja-ais-vessel-detail-port-rt-checkpoint'';


 -----------------------

 -- 数据结构

 -----------------------

-- -- 创建kafka数据来源的表港口数据（Source：kafka）
drop table if exists ais_fleetmon_vessel_port_info_kafka;
create table ais_fleetmon_vessel_port_info_kafka
(
    vesselId         bigint,    -- 船ID
    acquireTimestamp bigint comment '' 数据采集时间戳 '',
    `data`           row ( vessels array<
        row (
        voyage row (
        lastPort row (
        port row (
        portId bigint ,         -- 出发港口-ID
        name string ,           -- 出发港口-名称
        countryCode string ,    -- 出发港口-所属国家-简称
        locode string ,         -- 出发港口-定位器
        __typename string
        ),
        atd bigint ,            -- 实际出发时间
        __typename string
        ),
        nextPort row (
        port row (
        portId bigint ,         -- 目的港口-ID
        name string ,           -- 目的港口-名称
        countryCode string ,    -- 目的港口-所属国家-简称
        locode string ,         -- 目的港口-定位器
        __typename string
        ),
        eta bigint ,            -- 由fleetmon提供，预计到达时间
        distanceToPort double , -- 距离到达目的港口
        __typename string
        ),
        __typename string
        ),
        __typename string
        )
        >
        )
) with (
      '' connector '' = '' kafka '',
      '' topic '' = '' ais_vessel_voyage_port_item '',
      -- ''properties.bootstrap.servers'' = ''kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092'',
      '' properties.bootstrap.servers '' = '' kafka.kafka.svc.cluster.local:9092 '',
      '' properties.group.id '' = '' ais_fleetmon_detail_single_item-rt '',
      -- ''scan.startup.mode'' = ''latest-offset'',
      '' scan.startup.mode '' = '' timestamp '',
      '' scan.startup.timestamp-millis '' = '' 1702987489000 '',
      '' format '' = '' json '',
      '' json.fail- on -missing-field '' = '' false '',
      '' json.ignore-parse-errors '' = '' true ''
      );


-- 创建kafka数据来源的表详情数据（Source：kafka）
drop table if exists ais_detail_single_kafka;
create table ais_detail_single_kafka
(
    vesselId bigint comment '' 船ID '',
    `data`   row(                     -- 数据
        vessels array<                -- 船
        row (
        myFleetInformation row (      -- 船队信息
        __typename string,            -- 类型名称flag
        isOnMyFleet boolean,          -- 是我的船队
        isOnSharedFleet boolean,      -- 是否共享船队
        isOnOwnFleet boolean          -- 是否拥有船队
        ),
        staticInformation row (       -- 静态信息
        eta bigint,                   -- 预计到达时间，在军用词汇中经常用到
        draught double,               -- 吃水深度
        __typename string,            -- 类型名称flag
        destination string,           -- 目的地
        `timestamp` bigint            -- 船只时间
        ),
        `identity` row (              -- 身份
        vesselId bigint,              -- 船ID
        mmsi bigint,                  -- 船MMSI
        __typename string,            -- 类型名称flag
        name string,                  -- 船名称
        callsign string,              -- 呼号
        imo bigint                    -- IMO
        ),
        __typename string,            -- 类型名称flag
        navigationalInformation row ( -- 导航信息
        rateOfTurn double,            -- 转向率
        heading double,               -- 船首向
        __typename string,            -- 类型名称
        courseOverGround double,      -- 对地航向
        location row (
        longitude double,             -- 导航经度
        latitude double,              -- 导航纬度
        __typename string,            -- 类型名称flag
        locationDescription array <   -- 位置描述
        row (
        __typename string,            -- 类型名称flag
        shortName string              -- 短名称
        )
        >
        ),
        navigationalStatus string,    -- 导航状态
        source string,                -- 导航数据来源
        speed double,                 -- 导航速度
        `timestamp` bigint            -- 导航时间戳
        ),
        datasheet row (               -- 数据表
        owner string,                 -- 所有者
        draughtAverage double,        -- 吃水平均值
        grossTonnage double,          -- 总吨数
        speedMax double,              -- 速度最大值
        __typename string,            -- 类型名称flag
        length double,                -- 长度
        speedAverage double,          -- 速度平均值
        flagCountryCode string,       -- 标志国家代码
        yearBuilt double,             -- 建成年份
        vesselClass string,           -- 船类别
        riskRating string,            -- 风险评级
        vesselType string,            -- 船类型
        serviceStatus string,         -- 服务状态
        width double,                 -- 宽度
        deadweight double,            -- 重物，载重吨位
        height double                 -- 高度
        )
        )
        >
) ) with (
    ''connector'' = ''kafka'',
    ''topic'' = ''ais_fleetmon_detail_single_item'',
    -- ''properties.bootstrap.servers'' = ''kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092'',
    ''properties.bootstrap.servers'' = ''kafka.kafka.svc.cluster.local:9092'',
   ''properties.group.id'' = ''ais_fleetmon_detail_single_item-rt'',
    -- ''scan.startup.mode'' = ''latest-offset'',
    ''scan.startup.mode'' = ''timestamp'',
    ''scan.startup.timestamp-millis'' = ''1702987489000'',
    ''format'' = ''json'',
    ''json.fail-on-missing-field'' = ''false'',
    ''json.ignore-parse-errors'' = ''true''
);


--船港口信息详情全量数据入库（Sink：Doris）
drop table if exists dwd_ais_vessel_port_all_info;
create table dwd_ais_vessel_port_all_info
(
    vessel_id                bigint comment '' 船ID '',
    acquire_timestamp_format string comment '' 时间戳格式化 '',
    acquire_timestamp        bigint comment '' 时间戳 '',
    last_port_id             bigint comment '' 出发港口-ID '',
    last_port_name           string comment '' 出发港口-名称 '',
    last_port_name_chinese   string comment '' 出发港口-名称中文 '',
    last_port_country_code   string comment '' 出发港口-所属国家-简称 '',
    last_port_country_name   string comment '' 出发港口-所属国家名称中文 '',
    last_port_locode         string comment '' 出发港口-定位器 '',
    last_port_atd            bigint comment '' 实际出发时间 '',
    last_port_atd_format     string comment '' 出发时间格式化 '',
    next_port_id             bigint comment '' 目的港口-ID '',
    next_port_name           string comment '' 目的港口-名称 '',
    next_port_name_chinese   string comment '' 目的港口-名称中文 '',
    next_port_country_code   string comment '' 目的港口-所属国家-简称 '',
    next_port_country_name   string comment '' 目的港口-所属国家名称中文 '',
    next_port_locode         string comment '' 目的港口-定位器 '',
    next_port_eta            bigint comment '' 由网站提供，预计到达时间 '',
    next_port_eta_format     string comment '' 预计到达时间格式化 '',
    distance_to_port         double comment '' 距离到达目的港口 '',
    update_time              string comment '' 数据入库时间 ''
) with (
      '' connector '' = '' doris '',
      '' fenodes '' = '' 172.27.95.211:30030 '',
      '' table.identifier '' = '' sa.dwd_ais_vessel_port_all_info '',
      '' username '' = '' admin '',
      '' password '' = '' Jingansi@110 '',
      '' doris.request.tablet.size '' = '' 1 '',
      '' doris.request.read.timeout.ms '' = '' 30000 '',
      '' sink.batch.size '' = '' 10000 '',
      '' sink.batch.interval '' = '' 10s ''
      --  	''sink.properties.escape_delimiters'' = ''flase'',
      -- ''sink.properties.column_separator'' = ''\x01'',	 -- 列分隔符
      -- ''sink.properties.escape_delimiters'' = ''true'',    -- 类似开启的意思
      -- ''sink.properties.line_delimiter'' = ''\x02''		 -- 行分隔符
      );


--船港口信息详情状态数据入库（Sink：Doris）
drop table if exists dws_ais_vessel_port_status_info;
create table dws_ais_vessel_port_status_info
(
    vessel_id                bigint comment '' 船ID '',
    acquire_timestamp_format string comment '' 时间戳格式化 '',
    acquire_timestamp        bigint comment '' 时间戳 '',
    last_port_id             bigint comment '' 出发港口-ID '',
    last_port_name           string comment '' 出发港口-名称 '',
    last_port_name_chinese   string comment '' 出发港口-名称中文 '',
    last_port_country_code   string comment '' 出发港口-所属国家-简称 '',
    last_port_country_name   string comment '' 出发港口-所属国家名称中文 '',
    last_port_locode         string comment '' 出发港口-定位器 '',
    last_port_atd            bigint comment '' 实际出发时间 '',
    last_port_atd_format     string comment '' 出发时间格式化 '',
    next_port_id             bigint comment '' 目的港口-ID '',
    next_port_name           string comment '' 目的港口-名称 '',
    next_port_name_chinese   string comment '' 目的港口-名称中文 '',
    next_port_country_code   string comment '' 目的港口-所属国家-简称 '',
    next_port_country_name   string comment '' 目的港口-所属国家名称中文 '',
    next_port_locode         string comment '' 目的港口-定位器 '',
    next_port_eta            bigint comment '' 由网站提供，预计到达时间 '',
    next_port_eta_format     string comment '' 预计到达时间格式化 '',
    distance_to_port         double comment '' 距离到达目的港口 '',
    update_time              string comment '' 数据入库时间 ''
) with (
      '' connector '' = '' doris '',
      '' fenodes '' = '' 172.27.95.211:30030 '',
      '' table.identifier '' = '' sa.dws_ais_vessel_port_status_info '',
      '' username '' = '' admin '',
      '' password '' = '' Jingansi@110 '',
      '' doris.request.tablet.size '' = '' 1 '',
      '' doris.request.read.timeout.ms '' = '' 30000 '',
      '' sink.batch.size '' = '' 10000 '',
      '' sink.batch.interval '' = '' 10s ''
      --  	''sink.properties.escape_delimiters'' = ''flase'',
      -- ''sink.properties.column_separator'' = ''\x01'',	 -- 列分隔符
      -- ''sink.properties.escape_delimiters'' = ''true'',    -- 类似开启的意思
      -- ''sink.properties.line_delimiter'' = ''\x02''		 -- 行分隔符
      );


-- 船详情的数据（Sink：doris）
drop table if exists vessel_detail_static_attribute;
create table vessel_detail_static_attribute
(
    vessel_id           bigint comment '' 船ID '',
    imo                 bigint comment '' IMO '',
    mmsi                bigint comment '' mmsi '',
    callsign            string comment '' 呼号 '',
    year_built          double comment '' 建成年份 '',
    vessel_type         string comment '' 船类型 '',
    vessel_type_name    string comment '' 船类别-小类中文 '',
    vessel_class        string comment '' 船类别 '',
    vessel_class_name   string comment '' 船类型-大类中文 '',
    `name`              string comment '' 船名 '',
    draught_average     double comment '' 吃水平均值 '',
    speed_average       double comment '' 速度平均值 '',
    speed_max           double comment '' 速度最大值 '',
    length              double comment '' 长度 '',
    width               double comment '' 宽度 '',
    height              double comment '' 高度 '',
    `owner`             string comment '' 所有者 '',
    risk_rating         string comment '' 风险评级 '',
    risk_rating_name    string comment '' 风险评级中文 '',
    service_status      string comment '' 服务状态 '',
    service_status_name string comment '' 服务状态中文 '',
    flag_country_code   string comment '' 标志国家代码 '',
    country_name        string comment '' 国家中文 '',
    source              string comment '' 数据来源 '',
    gross_tonnage       double comment '' 总吨位 '',
    deadweight          double comment '' 重物，载重吨位 '',
    rate_of_turn        double comment '' 转向率 '',
    is_on_my_fleet      boolean comment '' 是我的船队 '',
    is_on_shared_fleet  boolean comment '' 是否共享船队 '',
    is_on_own_fleet     boolean comment '' 是否拥有船队 '',
    `timestamp`         bigint comment '' 采集船只详情数据时间 '',
    update_time         string comment '' 数据入库时间 ''
) with (
      '' connector '' = '' doris '',
      '' fenodes '' = '' 172.27.95.211:30030 '',
      '' table.identifier '' = '' sa.dws_ais_vessel_detail_static_attribute '',
      '' username '' = '' admin '',
      '' password '' = '' Jingansi@110 '',
      '' sink.batch.size '' = '' 10000 '',
      '' sink.batch.interval '' = '' 10s '',
      '' sink.properties.escape_delimiters '' = '' flase '',
      '' sink.properties.column_separator '' = ''\x01 '', -- 列分隔符
      '' sink.properties.escape_delimiters '' = '' true '', -- 类似开启的意思
      '' sink.properties.line_delimiter '' = ''\x02 '' -- 行分隔符
      );


-- 国家数据匹配库（Source：doris）
drop table if exists dim_vessel_country_code_list;
create table dim_vessel_country_code_list
(
    country           string comment '' 国家英文 '',
    flag_country_code string comment '' 国家的编码 '',
    country_name      string comment '' 国家的中文 '',
    belong_type       string comment '' 国家所属 '',
    primary key (country) NOT ENFORCED
) with (
      '' connector '' = '' jdbc '',
      '' url '' = '' jdbc:mysql://172.27.95.211:31030/sa?useSSL = false &useUnicode = true &characterEncoding = UTF-8&characterSetResults = UTF-8&zeroDateTimeBehavior = CONVERT_TO_NULL&serverTimezone = UTC '',
      '' username '' = '' root '',
      '' password '' = '' Jingansi@110 '',
      '' table - name '' = '' dim_vessel_country_code_list '',
      '' driver '' = '' com.mysql.cj.jdbc.Driver '',
      '' lookup.cache.max- rows '' = '' 10000 '',
      '' lookup.cache.ttl '' = '' 60s '',
      '' lookup.max-retries '' = '' 1 ''
      );


-- 船类别（大类）数据匹配库（Source：doris）
drop table if exists dim_vessel_class_list;
create table dim_vessel_class_list
(
    vessel_class      string comment '' 船类型 '',
    vessel_class_code string comment '' 请求详情接口返回的的船类型 '',
    vessel_class_name string comment '' 注释 '',
    primary key (vessel_class) NOT ENFORCED
) with (
      '' connector '' = '' jdbc '',
      '' url '' = '' jdbc:mysql://172.27.95.211:31030/sa?useSSL = false &useUnicode = true &characterEncoding = UTF-8&characterSetResults = UTF-8&zeroDateTimeBehavior = CONVERT_TO_NULL&serverTimezone = UTC '',
      '' username '' = '' root '',
      '' password '' = '' Jingansi@110 '',
      '' table - name '' = '' dim_vessel_class_list '',
      '' driver '' = '' com.mysql.cj.jdbc.Driver '',
      '' lookup.cache.max- rows '' = '' 10000 '',
      '' lookup.cache.ttl '' = '' 60s '',
      '' lookup.max-retries '' = '' 1 ''
      );


-- 船（大类）数据匹配库（Source：doris）
drop table if exists dim_vessel_type_categorie_list;
create table dim_vessel_type_categorie_list
(
    vessel_type_categorie       string comment '' 类别 '',
    vessel_type_categories_code string comment '' 请求详情接口返回的，也是网站请求的 '',
    vessel_type_categorie_name  string comment '' 船类别名称 '',
    primary key (vessel_type_categorie) NOT ENFORCED
) with (
      '' connector '' = '' jdbc '',
      '' url '' = '' jdbc:mysql://172.27.95.211:31030/sa?useSSL = false &useUnicode = true &characterEncoding = UTF-8&characterSetResults = UTF-8&zeroDateTimeBehavior = CONVERT_TO_NULL&serverTimezone = UTC '',
      '' username '' = '' root '',
      '' password '' = '' Jingansi@110 '',
      '' table - name '' = '' dim_vessel_type_categorie_list '',
      '' driver '' = '' com.mysql.cj.jdbc.Driver '',
      '' lookup.cache.max- rows '' = '' 10000 '',
      '' lookup.cache.ttl '' = '' 60s '',
      '' lookup.max-retries '' = '' 1 ''
      );


-- 风险等级数据匹配库（Source：doris）
drop table if exists dim_vessel_risk_rating_list;
create table dim_vessel_risk_rating_list
(
    risk_rating      string comment '' 风险等级 '',
    risk_rating_name string comment '' 风险等级名称 '',
    primary key (risk_rating) NOT ENFORCED
) with (
      '' connector '' = '' jdbc '',
      '' url '' = '' jdbc:mysql://172.27.95.211:31030/sa?useSSL = false &useUnicode = true &characterEncoding = UTF-8&characterSetResults = UTF-8&zeroDateTimeBehavior = CONVERT_TO_NULL&serverTimezone = UTC '',
      '' username '' = '' root '',
      '' password '' = '' Jingansi@110 '',
      '' table - name '' = '' dim_vessel_risk_rating_list '',
      '' driver '' = '' com.mysql.cj.jdbc.Driver '',
      '' lookup.cache.max- rows '' = '' 10000 '',
      '' lookup.cache.ttl '' = '' 60s '',
      '' lookup.max-retries '' = '' 1 ''
      );


-- 服务状态数据匹配库（Source：doris）
drop table if exists dim_vessel_service_status_list;
create table dim_vessel_service_status_list
(
    service_status      string comment '' 服务状态 '',
    servcie_status_code string comment '' 详情接口返回的服务状态类型 '',
    service_status_name string comment '' 服务状态名称 '',
    primary key (service_status) NOT ENFORCED
) with (
      '' connector '' = '' jdbc '',
      '' url '' = '' jdbc:mysql://172.27.95.211:31030/sa?useSSL = false &useUnicode = true &characterEncoding = UTF-8&characterSetResults = UTF-8&zeroDateTimeBehavior = CONVERT_TO_NULL&serverTimezone = UTC '',
      '' username '' = '' root '',
      '' password '' = '' Jingansi@110 '',
      '' table - name '' = '' dim_vessel_service_status_list '',
      '' driver '' = '' com.mysql.cj.jdbc.Driver '',
      '' lookup.cache.max- rows '' = '' 10000 '',
      '' lookup.cache.ttl '' = '' 60s '',
      '' lookup.max-retries '' = '' 1 ''
      );


-----------------------

-- 数据处理

-----------------------

-- 港口数据最外层的对象拆解
drop table if exists tmp_vessel_port_01;
create view tmp_vessel_port_01 as
select vesselId                 as vessel_id,
       acquireTimestamp         as acquire_timestamp,
       `data`.vessels[1].voyage as voyage,
       PROCTIME()               as proctime
from ais_fleetmon_vessel_port_info_kafka
where acquireTimestamp is not null
  and vesselId is not null;


-- 港口信息
drop table if exists tmp_vessel_port_03;
create view tmp_vessel_port_03 as
select vessel_id,
       acquire_timestamp,
       voyage.lastPort.port.portId      as last_port_id,
       voyage.lastPort.port.name        as last_port_name,
       voyage.lastPort.port.countryCode as last_port_country_code,
       voyage.lastPort.port.locode      as last_port_locode,
       voyage.lastPort.atd              as last_port_atd,
       voyage.nextPort.port.portId      as next_port_id,
       voyage.nextPort.port.name        as next_port_name,
       voyage.nextPort.port.countryCode as next_port_country_code,
       voyage.nextPort.port.locode      as next_port_locode,
       voyage.nextPort.eta              as next_port_eta,
       voyage.nextPort.distanceToPort   as distance_to_port,
       proctime
from tmp_vessel_port_01;


-- 港口数据关联唯表
drop table if exists tmp_vessel_port_04;
create view tmp_vessel_port_04 as
select t1.vessel_id,
       t1.last_port_id,
       t1.last_port_name,
       t1.acquire_timestamp,
       from_unixtime(t1.acquire_timestamp / 1000, ''yyyy-MM-dd HH:mm:ss'') as acquire_timestamp_format,
       -- concat(cast(CURRENT_DATE as string),'' 00:00:00'')  as acquire_timestamp_format,
       cast(null as varchar)                                               as last_port_name_chinese,
       t1.last_port_country_code                                           as last_port_country_code,
       t2.country_name                                                     as last_port_country_name,
       t1.last_port_locode,
       t1.last_port_atd,
       from_unixtime(t1.last_port_atd, ''yyyy-MM-dd HH:mm:ss'')            as last_port_atd_format,
       t1.next_port_id,
       t1.next_port_name,
       cast(null as varchar)                                               as next_port_name_chinese,
       t1.next_port_country_code                                           as next_port_country_code,
       t3.country_name                                                     as next_port_country_name,
       t1.next_port_locode,
       t1.next_port_eta,
       from_unixtime(t1.next_port_eta, ''yyyy-MM-dd HH:mm:ss'')            as next_port_eta_format,
       t1.distance_to_port
from tmp_vessel_port_03 as t1
         left join dim_vessel_country_code_list
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.last_port_country_code = t2.flag_country_code
                       and ''FLEETMON'' = t2.belong_type
left join dim_vessel_country_code_list
  FOR SYSTEM_TIME AS OF t1.proctime as t3
  on t1.next_port_country_code = t3.flag_country_code
  and ''FLEETMON'' = t3.belong_type;


-- 详情数据解析数据第1步
drop table if exists tem_ais_kafka_01;
create view tem_ais_kafka_01 as
select vesselId          as vessel_id,
       `data`.vessels[1] as tmp_object_data
from ais_detail_single_kafka
where vesselId is not null;


-- 详情数据解析数据第2步
drop table if exists tem_ais_kafka_02;
create view tem_ais_kafka_02 as
select vessel_id,
       tmp_object_data.staticInformation as static_information,
       tmp_object_data.
        __typename as vessels_typename,
        tmp_object_data.`identity`.imo as imo,
        tmp_object_data.`identity`.mmsi as mmsi,
        tmp_object_data.`identity`.name as name,
        tmp_object_data.`identity`.callsign as callsign,
        tmp_object_data.navigationalInformation.source as source,
        tmp_object_data.navigationalInformation.rateOfTurn as rate_of_turn,
        tmp_object_data.datasheet.owner as owner,
        tmp_object_data.datasheet.draughtAverage as draught_average,
        tmp_object_data.datasheet.grossTonnage as gross_tonnage,
        tmp_object_data.datasheet.speedMax as speed_max,
        tmp_object_data.datasheet.length as length,
        tmp_object_data.datasheet.speedAverage as speed_average,
        tmp_object_data.datasheet.flagCountryCode as flag_country_code,
        tmp_object_data.datasheet.yearBuilt as year_built,
        tmp_object_data.datasheet.vesselClass as vessel_class,
        tmp_object_data.datasheet.riskRating as risk_rating,
        tmp_object_data.datasheet.vesselType as vessel_type,
        tmp_object_data.datasheet.serviceStatus as service_status,
        tmp_object_data.datasheet.width as width,
        tmp_object_data.datasheet.deadweight as deadweight,
        tmp_object_data.datasheet.height as height,
        tmp_object_data.myFleetInformation.isOnMyFleet as is_on_my_fleet,
        tmp_object_data.myFleetInformation.isOnSharedFleet as is_on_shared_fleet,
        tmp_object_data.myFleetInformation.isOnOwnFleet as is_on_own_fleet,
        tmp_object_data.navigationalInformation.`timestamp` as `timestamp`,
        PROCTIME() as proctime -- 维表关联的时间函数
        from tem_ais_kafka_01;
-- where tmp_object_data is not null;


-----------------------

-- 数据插入

-----------------------

begin statement
set;


-- 港口全量数据入库doris
insert into dwd_ais_vessel_port_all_info
select vessel_id,
       acquire_timestamp_format,
       acquire_timestamp,
       last_port_id,
       last_port_name,
       last_port_name_chinese,
       last_port_country_code,
       last_port_country_name,
       last_port_locode,
       last_port_atd,
       last_port_atd_format,
       next_port_id,
       next_port_name,
       next_port_name_chinese,
       next_port_country_code,
       next_port_country_name,
       next_port_locode,
       next_port_eta,
       next_port_eta_format,
       distance_to_port,
       from_unixtime(unix_timestamp()) as update_time
from tmp_vessel_port_04;


-- 港口状态数据入库doris
insert into dws_ais_vessel_port_status_info
select vessel_id,
       acquire_timestamp_format,
       acquire_timestamp,
       last_port_id,
       last_port_name,
       last_port_name_chinese,
       last_port_country_code,
       last_port_country_name,
       last_port_locode,
       last_port_atd,
       last_port_atd_format,
       next_port_id,
       next_port_name,
       next_port_name_chinese,
       next_port_country_code,
       next_port_country_name,
       next_port_locode,
       next_port_eta,
       next_port_eta_format,
       distance_to_port,
       from_unixtime(unix_timestamp()) as update_time
from tmp_vessel_port_04;


-- 详情数据数据写入
insert into vessel_detail_static_attribute
select t1.vessel_id,
       t1.imo,
       t1.mmsi,
       t1.callsign,
       t1.year_built,
       if(t1.vessel_type = '''', cast(null as varchar), t1.vessel_type)             as vessel_type,
       t3.vessel_type_categorie_name                                                as vessel_type_name,
       if(t1.vessel_class = '''', cast(null as varchar), t1.vessel_class)           as vessel_class,
       t2.vessel_class_name                                                         as vessel_class_name,
       t1.`name`,
       t1.draught_average,
       t1.speed_average,
       t1.speed_max,
       t1.length,
       t1.width,
       t1.height,
       if(t1.`owner` = '''', cast(null as varchar), t1.`owner`)                     as `owner`,
       t1.risk_rating,
       t4.risk_rating_name                                                          as risk_rating_name,
       if(t1.service_status is not null, t1.service_status, ''UN_KNOWN'')              service_status,
       t5.service_status_name                                                       as service_status_name,
       if(t1.flag_country_code = '''', cast(null as varchar), t1.flag_country_code) as flag_country_code,
       t6.country_name                                                              as country_name,
       t1.source,
       t1.gross_tonnage,
       t1.deadweight,
       t1.rate_of_turn,
       t1.is_on_my_fleet,
       t1.is_on_shared_fleet,
       t1.is_on_own_fleet,
       t1.`timestamp`,
       from_unixtime(unix_timestamp())                                              as update_time
from tem_ais_kafka_02 as t1
         left join dim_vessel_class_list
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.vessel_class = t2.vessel_class_code

         left join dim_vessel_type_categorie_list
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.vessel_type = t3.vessel_type_categories_code

         left join dim_vessel_risk_rating_list
    FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.risk_rating = t4.risk_rating

         left join dim_vessel_service_status_list
    FOR SYSTEM_TIME AS OF t1.proctime as t5
                   on if(t1.service_status is not null, t1.service_status, ''UN_KNOWN'') = t5.servcie_status_code

         left join dim_vessel_country_code_list
    FOR SYSTEM_TIME AS OF t1.proctime as t6
                   on t1.flag_country_code = t6.flag_country_code
                       and ''FLEETMON'' = t6.belong_type;


end;



