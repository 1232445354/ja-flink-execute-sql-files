--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/3/22 19:52:04
-- description: vt数据入库船舶融合表
-- version: ja-vtexplore-vessel-list-merge-250325
--********************************************************************--
set 'pipeline.name' = 'ja-vtexplore-vessel-list-merge';


set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '15';
SET 'sql-client.execution.result-mode'='TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '300000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-vtexplore-vessel-list-merge';
set 'execution.checkpointing.timeout' = '3600000';

-- 空闲分区不用等待
-- set 'table.exec.source.idle-timeout' = '3s';

-- set 'execution.type' = 'streaming';
-- set 'table.planner' = 'blink';
-- set 'table.exec.state.ttl' = '600000';
-- set 'sql-client.execution.result-mode' = 'TABLEAU';


-----------------------

 -- 数据结构

 -----------------------


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
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-vt-list-idc',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1724580000000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 实体详情 - 合并的实体表
drop table if exists dws_ais_vessel_detail_static_attribute;
create table dws_ais_vessel_detail_static_attribute (
                                                        vessel_id      	 			bigint		comment '船ID',
                                                        imo      					bigint      comment 'IMO',
                                                        mmsi						bigint      comment 'mmsi',
                                                        callsign 					string      comment '呼号',
                                                        `name` 						string      comment '船名',
                                                        length                   	double		comment '长度',
                                                        width                    	double      comment '宽度',
                                                        flag_country_code          	string      comment '标志国家代码',
                                                        country_name                string      comment '国家中文',
                                                        source         				string      comment '数据来源',
                                                        update_time					string		comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_ais_vessel_detail_static_attribute',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'sink.batch.size'='50000',
      'sink.batch.interval'='20s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
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
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_sea_area',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
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
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_vt_country_code_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 实体信息表数据
drop table if exists dws_ais_vessel_detail_static_attribute_source;
create table dws_ais_vessel_detail_static_attribute_source (
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
                                                               flag_country_code  string,
                                                               country_name       string,
                                                               source           string,
                                                               length           double,
                                                               width            double,
                                                               height           double,
                                                               primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_ais_vessel_detail_static_attribute',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- marinetraffic、vt的船舶对应关系
drop table if exists dim_mtf_vt_reletion_info;
create table dim_mtf_vt_reletion_info (
                                          vessel_id    string        comment '编号',
                                          vt_mmsi      string        comment 'vt数据的mmsi',
                                          merge_way    string        comment '融合方式',
                                          primary key (vt_mmsi) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_mtf_vt_reletion_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- ****************************规则引擎写入数据******************************** --
create table vessel_source(
                              id                    bigint, -- id
                              source                string, -- 来源
                              acquireTime           string, -- 采集事件年月日时分秒
                              acquireTimestamp      bigint, -- 采集时间戳
                              vesselName            string, -- 船舶名称,写出给规则引擎
                              cName                 string, -- 船舶中文名称
                              mmsi                  string, -- mmsi
                              imo                   string, -- imo
                              callsign              string, -- 呼号
                              cnIso2                string, -- 国家代码
                              countryName           string, -- 国家名称
                              vesselClass           string, -- 大类型编码
                              vesselClassName       string, -- 大类型名称
                              vesselType            string, -- 小类型编码
                              vesselTypeName        string, -- 小类型名称
                              friendFoe             string, -- 敌我代码
                              positionCountryCode2  string, -- 所处国家
                              seaId                 string, -- 海域id
                              seaName               string, -- 海域名称
    -- navStatus             string, -- 航行状态
    -- navStatusName         string, -- 航行状态名称
                              lng                   double, -- 经度
                              lat                   double, -- 纬度
                              orientation           double, -- 方向
                              speed                 double, -- 速度 节
                              speedKm               double, -- 速度 km
    -- rateOfTurn            double, -- 转向率
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
    -- sourceVesselClass     string,
    -- sourceVesselClassName string,
    -- sourceVesselType      string,
    -- sourceVesselTypeName  string,
                              sourceLength          double,
                              sourceWidth           double,
    -- sourceHeight          double,
    -- blockMapIndex         bigint, -- 图层层级
    -- blockRangeX           bigint, -- 块x
    -- blockRangeY           bigint, -- 块y
                              targetType            string, -- 实体类型 固定值 VESSEL
                              ex_info               map<String,String>, -- 扩展信息
                              updateTime            string -- flink处理时间
) with (
      'connector' = 'kafka',
      'topic' = 'vessel_source',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'lb_vessel_source_idc1',
      'key.format' = 'json',
      'key.fields' = 'mmsi',
      'format' = 'json'
      );





-----------------------

-- 数据处理

-----------------------

create function getCountry as 'com.jingan.udf.sea.GetCountryFromLngLat';
create function getSeaArea as 'com.jingan.udf.sea.GetSeaArea';


-- 1.关联对应关系 2.关联国家对应表 3.计算海域
create view temp_01 as
select
    t1.MMSI                                                 as mmsi,                     -- mmsi
    if(t1.IMO='0',cast(null as string),t1.IMO)              as imo,                      -- imo
    if(t1.callsign <> '',t1.callsign,cast(null as varchar)) as callsign,                 -- 呼号
    t1.`timestamp`                                          as acquire_timestamp,        -- 采集时间
    from_unixtime(`timestamp`,'yyyy-MM-dd HH:mm:ss')        as acquire_timestamp_format, -- 采集时间戳格式化
    if(t1.name <> '',t1.name,cast(t1.name as varchar))      as name,  -- vt船舶名称
    t1.longitude                                            as lng,   -- 经度
    t1.latitude                                             as lat,   -- 纬度
    t1.speed,                                               -- 船舶的当前速度，以节（knots）为单位
    t1.speed * 1.852                                        as speed_km,       -- 速度km/h
    t1.course                                               as orientation,    -- 船舶的当前航向
    t1.draught,                                                                -- 吃水
    t1.type,                                                                   -- 船舶类型代码 - 未对应
    cast(trim(split_index(t1.`size`,'*',0)) as double)      as length,         -- 船舶的长度，以米为单位
    cast(trim(split_index(t1.`size`,'*',1)) as double)      as width,          -- 船舶的宽度，以米为单位
    if(t1.is_satellite,1,0)                                 as is_satellite,   -- 是否卫星
    t1.ETA                                                  as eta,       -- 预计到岗时间
    t1.destination_code                                     as dest_code, -- 目的地代码
    t1.destination_name                                     as dest_name, -- 目的地名称
    t1.ais_time,                                                      -- ais采集时间 - 自己加的
    t1.proctime,
    t3.country_code2                                         as country_code,                      -- 船舶的国家或地区旗帜标识
    coalesce(t3.c_name,t1.country)                           as country_name,                      -- 国家中文名称
    coalesce(cast(t2.vessel_id as bigint),cast(t1.MMSI as bigint) + 4000000000) as vessel_id,   -- 之后入库的船舶id bigint
    getCountry(t1.longitude,t1.latitude) as position_country_3code,  -- 计算所处国家 - 经纬度位置转换国家
    getSeaArea(t1.longitude,t1.latitude) as sea_id, -- 计算海域id
    MAP['is_satellite', cast(is_satellite as string),
    'type', cast(type as string),
    'destination_code', destination_code ,
    'destination_name', destination_name ,
    'ETA', ETA
    ]   as ex_info

from ais_vtexplorer_ship_list t1
  left join dim_mtf_vt_reletion_info FOR SYSTEM_TIME AS OF t1.proctime as t2 -- VT对应关系
    on t1.MMSI = t2.vt_mmsi

  left join dim_vt_country_code_info FOR SYSTEM_TIME AS OF t1.proctime as t3 -- 国家表
      on t1.country  = t3.vt_c_name;



create view temp_02 as
select
    t1.vessel_id                              as id, -- bigint
    acquire_timestamp_format                  as acquireTime,
    acquire_timestamp                         as acquireTimestamp,
    coalesce(t4.name,t1.name)                 as vesselName,
    t4.c_name                                 as cName,
    t1.mmsi                                   as mmsi,
    coalesce(cast(t4.imo as varchar),t1.imo)  as imo,
    coalesce(t4.callsign,t1.callsign)         as callsign,
    country_code                              as countryCode,
    t1.country_name                           as countryName,
    'vt'                                      as source,
    t4.vessel_class                           as vesselClass,    -- 大类型编码
    t4.vessel_class_name                      as vesselClassName,-- 大类型名称
    t4.vessel_type                            as vesselType,     -- 小类型编码
    t4.vessel_type_name                       as vesselTypeName, -- 小类型名称
    lng,
    lat,
    orientation,
    speed,
    speed_km              as speedKm,-- 速度 km/h
    draught,
    coalesce(t4.length,t1.length)    as length,
    coalesce(t4.width,t1.width)      as width,
    t4.height,
    t1.name                          as sourceShipname,
    t1.country_code                  as sourceCountryCode,
    t1.country_name                  as sourceCountryName,
    t1.mmsi            as sourceMmsi,
    t1.imo             as sourceImo,
    t1.callsign        as sourceCallsign,
    t1.sea_id          as seaId,
    t3.c_name          as seaName,
    t4.vessel_id       as t4_vessel_id,
    cast(t1.length as double)       as sourceLength,
    cast(t1.width as double)        as sourceWidth,

    case
        when country_code in('IN','US','JP','AU') and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR') then '1' -- 敌 - 美印日澳 军事
        when country_code ='CN' and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then '2'               -- 我 中国 军事
        when country_code ='CN' and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then '3'      -- 友 中国 非军事
        else '4'
        end as friendFoe,     -- 敌我

    if(t2.country_code2 is null
           and ((lng between 107.491636 and 124.806089 and lat between 20.522241 and 40.799277)
            or (lng between 107.491636 and 121.433286 and lat between 3.011639 and 20.522241)
           ),'CN',t2.country_code2
        ) as positionCountryCode2,
    ex_info

from temp_01 t1
         left join dws_ais_vessel_detail_static_attribute_source FOR SYSTEM_TIME AS OF t1.proctime as t4  -- 实体信息
                   on t1.vessel_id = t4.vessel_id
         left join dim_vt_country_code_info FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.position_country_3code = t2.country_code3

         left join dim_sea_area FOR SYSTEM_TIME AS OF t1.proctime as t3 -- 海域名称
                   on t1.sea_id = t3.id;



-----------------------

-- 数据插入

-----------------------

begin statement set;

insert into dws_ais_vessel_detail_static_attribute
select
    id     as vessel_id ,
    cast(imo as bigint) as imo,
    cast(mmsi as bigint) as mmsi,
    callsign,
    vesselName as name,
    length,
    width,
    countryCode as flagCountryCode,
    countryName,
    source,
    from_unixtime(unix_timestamp()) as update_time
from temp_02
where t4_vessel_id is null;


-- ****************************规则引擎写入数据******************************** --

insert into vessel_source
select
    id,
    source,
    acquireTime,
    acquireTimestamp,
    vesselName,
    cName,
    mmsi,
    imo,
    callsign,
    countryCode as cnIso2,
    countryName,
    vesselClass,
    vesselClassName,
    vesselType,
    vesselTypeName,
    friendFoe,
    positionCountryCode2,
    seaId,
    seaName,
    lng,
    lat,
    orientation,
    speed,
    speedKm,
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
    sourceLength,
    sourceWidth,
    -- blockMapIndex,
    -- blockRangeX,
    -- blockRangeY,
    'VESSEL'                        as targetType,
    ex_info,
    from_unixtime(unix_timestamp()) as updateTime
from temp_02;


end;




