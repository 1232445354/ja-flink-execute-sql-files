--********************************************************************--
-- author:      yibo
-- create time: 2024/07/30 22:44:32
-- description: flightaware 飞机数据入库
-- version: ja-flightaware-list-v250930
--********************************************************************--
set 'pipeline.name' = 'ja-flightaware-list';

set 'parallelism.default' = '4';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '300000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '60000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-flightaware-list';


 -----------------------

 -- 数据结构

 -----------------------

-- 创建kafka全量数据表（Source：kafka）
create table flightaware_list_kafka(
                                       type                 string,               -- 固定值特征Feature
                                       reponseTime          string,               -- 数据采集的时间，自己带出来的
                                       geometry row<                              -- 包含所有飞机信息的数组
                                           type                 string,           -- 点状地理数据
                                       coordinates array<double>              -- [经度, 纬度] - 飞机的当前坐标位置
                                           >,
                                       properties row<                            -- 属性信息
                                           prefix               string,           -- 航班呼号前缀
                                       groundspeed          double,           -- 地速（单位：节）
                                       type                 string,           -- 飞机型号代码（EC35=EC135直升机，A21N=A321neo）
                                       flight_id            string,           -- 航班唯一标识符（包含时间戳等信息）
                                       projected            int,              -- 是否为预测位置（0=实时位置）
                                       origin row<                            -- 起飞机场
                                           isUSAirport          boolean,      -- 是否美国机场
                                       iata                 string,       -- iata
                                       icao                 string        -- icao
                                           >,
                                       destination row<                       --  目的地机场
                                           isUSAirport          boolean,      -- 是否美国机场
                                       TZ                   string,       -- 时区
                                       icao                 string,       -- icao
                                       iata                 string        -- iata
                                           >,
                                       ident                string,           -- 航班号
                                       altitude             double,           -- 海拔高度（单位：百英尺，330表示33,000英尺）
                                       landingTimes row<                      -- 预计降落时间（Unix时间戳）秒级别的时间戳
                                           estimated            string
                                           >,
                                       ga                   boolean,          -- 是否通用航空（true=是，false=否）
                                       direction            double,           -- 航向角度（0-360度）
                                       flightType           string,           -- 航班类型（ga=通用航空，airline=航空公司）
                                       altitudeChange       string,           -- 高度变化：C=爬升，D=下降
                                       icon                 string,           -- 显示图标类型（helicopter=直升机，airliner=客机）
                                       prominence           int               -- 突出程度/重要性评分
                                           >

) with (
      'connector' = 'kafka',
      'topic' = 'flightaware-list',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'flightaware-list-group-idc1',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1760254172000', -- 1760254172000
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 创建写入doris全量数据表（Sink：doris）
create table dwd_bhv_flightaware_all_rt (
                                            flight_no             string  , --  飞机航班号,
                                            acquire_time          string  , --  数据时间,
                                            reponse_time          string  , --  数据采集的时间，自己带出来的,
                                            longitude             double  , --  经度,
                                            latitude              double  , --  经度,
                                            altitude              double  , --  高度m,
                                            speed                 double  , --  速度-节,
                                            speed_km              double  , --  速度km/h,
                                            direction             double  , --  方向,
                                            origin_icao           string  , --  来源机场icao,
                                            origin_iata           string  , --  来源机场iata,
                                            origin_is_us_airport  int  	, --  来源机场是否美国机场,
                                            dest_icao             string  , --  到达机场icao,
                                            dest_iata             string  , --  到达机场iata,
                                            dest_tz               string  , --  到达机场时区,
                                            dest_is_us_airport    int  	, --  到达机场是否美国机场,
                                            altitude_change       string  , --  高度变化:C=爬升,D=下降,
                                            flight_id             string  , --  完整拼接的飞机id,
                                            prefix                string  , --  航班呼号前缀,
                                            flight_type           string  , --  飞机型号代码,
                                            icon                  string  , --  显示图标类型(helicopter=直升机,airliner=客机),
                                            service_category      string  , --  航班类型(ga=通用航空,airline=航空公司),
                                            ga                    int     , --  是否通用航空 1:是,0否,
                                            prominence            int     , --  突出程度/重要性评分,
                                            projected             int     , --  是否为预测位置(0=实时位置),
                                            landing_times         string  , --  预计着路时间,
                                            update_time           string    --  更新时间
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.244:8030',
      'table.identifier' = 'sa.dwd_bhv_flightaware_all_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='5000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 飞机实体表（Source：doris）
create table dws_et_aircraft_info (
                                      flight_id           string        comment '',
                                      icao_code           string        comment '飞机的 24 位 ICAO 标识符，为 6 个十六进制数字 大写',
                                      registration        string        comment '地区国家三位编码',
                                      flight_type         string        comment '飞机的机型型码，用于标识不同类型的飞机',
                                      is_military         int           comment '是否军用飞机',
                                      category_code       string,
                                      category_name       string,
                                      country_code        string,
                                      country_name        string,
                                      airlines_icao       string,
                                      airlines_e_name     string,
                                      airlines_c_name     string,
                                      friend_foe          string,
                                      primary key (flight_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_aircraft_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '700000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );


-- 机场名称
create table dws_et_airport_info (
                                     id              string comment 'icao',
                                     iata 			string comment 'iata',
                                     e_name 		    string comment '机场英文名称',
                                     c_name 	        string comment '机场中文名称',
                                     longitude       double,
                                     latitude        double,
                                     primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_airport_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '20000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );



-- 航班号对应关系（Source：doris）
create table dim_rel_flightaware_no (
                                        flight_no             string       comment '飞机航班号',
                                        flight_id             string       comment '飞机id-icaoCode',
                                        acquire_time          timestamp    comment '数据时间',
                                        primary key (flight_no) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_rel_flightaware_no',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- 国家数据匹配库（Source：doris）
create table dim_country_code_name_info (
                                            id                        string        comment '国家英文-id',
                                            source                    string        comment '来源',
                                            e_name                    string        comment '国家的英文',
                                            c_name                    string        comment '国家的中文',
                                            country_code2             string        comment '国家的编码2',
                                            country_code3             string        comment '国家的编码3',
                                            primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_code_name_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- 海域表
create table dim_sea_area (
                              id 			varchar(5) COMMENT '海域编号',
                              name 		varchar(60) COMMENT '名称',
                              c_name 		varchar(60) COMMENT '中文名称',
                              primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_sea_area',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- **************************** 规则引擎写入数据 aircraft_source ******************************** --
create table aircraft_source(
                                id                         string, -- id
                                srcCode                    bigint, -- 网站标识
                                acquireTime                string, -- 采集事件年月日时分秒
                                icaoCode                   string, -- icao
                                registration               string, -- 注册号
                                flightNo                   string, -- 航班号
                                callsign                   string, -- 呼号
                                flightType                 string, -- 飞机型号
                                isMilitary                 bigint, -- 是否军用
                                pkType                     string, -- flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex
                                srcPk                      string, -- 源网站主键
                                flightCategory             string, -- 飞机类别
                                flightCategoryName         string, -- 飞机类别名称
                                lng                        double, -- 经度
                                lat                        double, -- 纬度
                                speed                      double, -- 速度节
                                speedKm                    double, -- 速度 km
                                altitudeBaro               double, -- 气压高度 海拔 航班当前高度，单位为（ft）
                                altitudeBaroM              double, -- 气压高度 海拔 单位米
                                altitudeGeom               double, -- 海拔高度 海拔 航班当前高度，单位为（ft）
                                altitudeGeomM              double, -- 海拔高度 海拔 单位米
                                heading                    double, -- 方向
                                squawkCode                 string, -- 应答器代码
                                flightStatus               bigint, -- 飞机状态
                                special                    int,    -- 是否有特殊情况
                                originAirport3Code         string, -- 起飞机场的iata代码
                                originAirportEName         string, -- 来源机场英文
                                originAirportCName         string, -- 来源机场中文
                                originLng                  double, -- 来源机场经度
                                originLat                  double, -- 来源机场纬度
                                destAirport3Code           string, -- 目的机场3字代码
                                destAirportEName           string, -- 目的机场英文
                                destAirportCName           string, -- 目的机场中文
                                destLng                    double, -- 目的地坐标经度
                                destLat                    double, -- 目的地坐标纬度
                                flightPhoto                string, -- 飞机的图片
                                flightDepartureTime        string, -- 飞机起飞时间
                                expectedLandingTime        string, -- 预计降落时间
                                toDestinationDistance      double, -- 距离目的地距离
                                estimatedLandingDuration   string, -- 预计还要多久着陆
                                airlinesIcao               string, -- 航空公司icao
                                airlinesEName              string, -- 航空公司英文
                                airlinesCName              string, -- 航空公司中文
                                countryCode                string, -- 国家代码
                                countryName                string, -- 国家名称
                                dataSource                 string, -- 数据来源的系统
                                source                     string, -- 来源
                                positionCountryCode2       string, -- 所处国家
                                positionCountryName        string, -- 所处国家名称
                                friendFoe                  string, -- 敌我代码
                                seaId                      string, -- 海域id
                                seaName                    string, -- 海域名称
                                h3Code                     string, -- 位置h3编码
                                extendInfo                 string, -- 扩展信息 json 串
                                targetType                 string, -- 实体类型 固定值 AIRCRAFT
                                updateTime                 string  -- flink处理时间
) with (
      'connector' = 'kafka',
      'topic' = 'aircraft_source',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'adsbexchange_aircraft_source_idc1',
      'format' = 'json',
      'key.format' = 'json',
      'key.fields' = 'id'
      );


create function getCountry as 'com.jingan.udf.sea.GetCountryFromLngLat';
create function getSeaArea as 'com.jingan.udf.sea.GetSeaArea';


-----------------------

-- 数据处理

-----------------------


-- 字段处理
create view tmp_table01 as
select
    geometry.type                   as type,
    geometry.coordinates[1]         as longitude,
    geometry.coordinates[2]         as latitude,
    properties.altitude * 30.48     as altitude,   -- 单位M
    properties.groundspeed          as speed,
    properties.groundspeed * 1.852  as speed_km,
    properties.direction            as direction,
    if(properties.origin.isUSAirport = true,1,0)                                                    as origin_is_us_airport,
    if(properties.origin.icao in ('---',''),cast(null as varchar),properties.origin.icao)           as origin_icao,
    if(properties.origin.iata in ('---',''),cast(null as varchar),properties.origin.iata)           as origin_iata,
    if(properties.destination.isUSAirport = true,1,0)                                               as dest_is_us_airport,
    if(properties.destination.icao in ('---',''),cast(null as varchar),properties.destination.icao) as dest_icao,
    if(properties.destination.iata in ('---',''),cast(null as varchar),properties.destination.iata) as dest_iata,
    if(properties.destination.TZ in ('---',''),cast(null as varchar),replace(properties.destination.TZ,':',''))     as dest_tz,
    if(properties.altitudeChange in('','-'),cast(null as varchar),properties.altitudeChange)        as altitude_change,
    if(properties.type = '',cast(null as varchar),properties.type) as flight_type,
    properties.flight_id              as flight_id,
    properties.ident                  as flight_no,
    properties.prefix                 as prefix,
    properties.icon                   as icon,
    properties.flightType             as service_category,
    if(properties.ga = true,1,0)      as ga,
    properties.prominence             as prominence,
    properties.projected              as projected,
    if(properties.landingTimes.estimated in ('','0'),cast(null as varchar),from_unixtime(cast(properties.landingTimes.estimated as bigint),'yyyy-MM-dd HH:mm:ss')) as landing_times,
    reponseTime                       as reponse_time,
    -- split_index(properties.flight_id,'-',0) as flight_no,
    cast(split_index(properties.flight_id,'-',1) as bigint)   as acquire_timestamp,
    PROCTIME()  as proctime
from flightaware_list_kafka
where abs(cast(split_index(properties.flight_id,'-',1) as bigint) - UNIX_TIMESTAMP()) <= 864000
  and cast(split_index(properties.flight_id,'-',1) as bigint) is not null
  and properties.ident is not null;



-- 关联对应关系，关联实体表取值字段，计算国家，加速海域
create view tmp_table02 as
select
    t1.*,
    if(longitude is not null and latitude is not null, getCountry(longitude,latitude),cast(null as string)) as country_code3, -- 经纬度位置转换国家
    if(longitude is not null and latitude is not null, getSeaArea(longitude,latitude),cast(null as string)) as sea_id,
    t2.flight_id as new_flight_id
from (
         select * from tmp_table01 where projected = 0   -- 不是预测的数据
     ) as t1
         left join dim_rel_flightaware_no FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.flight_no = t2.flight_no;
-- where t2.flight_id is not null;



-- 关联海域名称，判断区域国家
create view tmp_table03 as
select
    coalesce(t1.new_flight_id,t1.flight_no)as id,
    5                          as srcCode,
    from_unixtime(t1.acquire_timestamp,'yyyy-MM-dd HH:mm:ss') as acquireTime,
    t4.icao_code               as icaoCode,
    t4.registration            as registration,
    t1.flight_no               as flightNo,
    cast(null as varchar)      as callsign,
    coalesce(t4.flight_type,t1.flight_type)  as flightType,
    t4.is_military             as isMilitary,
    if(t1.new_flight_id is not null,'hex','flight_no') as pkType,
    t1.flight_no               as srcPk,
    t4.category_code           as flightCategory,
    t4.category_name           as flightCategoryName,
    t1.longitude               as lng,
    t1.latitude                as lat,
    t1.speed                   as speed,
    t1.speed_km                as speedKm,
    cast(null as double)       as altitudeBaro,
    altitude                   as altitudeBaroM,
    cast(null as double)       as altitudeGeom,
    cast(null as double)       as altitudeGeomM,
    direction                  as heading,
    cast(null as varchar)      as squawkCode,
    cast(null as bigint)       as flightStatus,
    cast(null as int)          as special,
    if(CHAR_LENGTH(origin_icao) < 10,origin_icao,cast(null as varchar))      as originAirport3Code,
    b.e_name                   as originAirportEName,
    b.c_name                   as originAirportCName,
    b.longitude                as originLng,
    b.latitude                 as originLat,
    if(CHAR_LENGTH(dest_icao) <10,dest_icao,cast(null as varchar))           as destAirport3Code,
    d.e_name                   as destAirportEName,
    d.c_name                   as destAirportCName,
    d.longitude                as destLng,
    d.latitude                 as destLat,
    cast(null as varchar)      as flightPhoto,
    cast(null as varchar)      as flightDepartureTime,
    landing_times              as expectedLandingTime,
    cast(null as double)       as toDestinationDistance,
    cast(null as varchar)      as estimatedLandingDuration,
    t4.airlines_icao           as airlinesIcao,
    t4.airlines_e_name         as airlinesEName,
    t4.airlines_c_name         as airlinesCName,
    t4.country_code            as countryCode,
    t4.country_name            as countryName,
    cast(null as varchar)      as dataSource,
    cast(null as varchar)      as source,
    cast(null as varchar)      as positionCountryName,
    t4.friend_foe              as friendFoe,
    t1.sea_id                  as seaId,
    t3.c_name                  as seaName,
    cast(null as varchar)      as h3Code,
    cast(null as varchar)      as extendInfo,
    if(t2.country_code2 is null
           and ((t1.longitude between 107.491636 and 124.806089 and t1.latitude between 20.522241 and 40.799277)
            or
                (t1.longitude between 107.491636 and 121.433286 and t1.latitude between 3.011639 and 20.522241)
           )
        ,'CN', t2.country_code2)    as positionCountryCode2  -- 当前所处的区域

from tmp_table02 as t1
         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 区域3字转2字
                   on t1.country_code3 = t2.country_code3

         left join dim_sea_area FOR SYSTEM_TIME AS OF t1.proctime as t3          -- 海域国家
                   on t1.sea_id = t3.id

         left join dws_et_aircraft_info FOR SYSTEM_TIME AS OF t1.proctime as t4  -- 实体表
                   on t1.new_flight_id = t4.flight_id

         left join dws_et_airport_info FOR SYSTEM_TIME AS OF t1.proctime as b  -- 机场名称
                   on t1.origin_icao = b.id

         left join dws_et_airport_info FOR SYSTEM_TIME AS OF t1.proctime as d  -- 机场名称
                   on t1.dest_icao = d.id;


-----------------------

-- 数据插入

-----------------------

begin statement set;

-- 单独的数据 入库
insert into dwd_bhv_flightaware_all_rt
select
    flight_no    ,
    from_unixtime(acquire_timestamp,'yyyy-MM-dd HH:mm:ss') as acquire_time,
    reponse_time          ,
    longitude             ,
    latitude              ,
    altitude              ,
    speed                 ,
    speed_km              ,
    direction             ,
    origin_icao           ,
    origin_iata           ,
    origin_is_us_airport  ,
    dest_icao             ,
    dest_iata             ,
    dest_tz               ,
    dest_is_us_airport    ,
    altitude_change       ,
    flight_id             ,
    prefix                ,
    flight_type           ,
    icon                  ,
    service_category      ,
    ga                    ,
    prominence            ,
    projected             ,
    landing_times         ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_table01;


-- 数据入融合的中转kafka
insert into aircraft_source
select
    id,
    srcCode,
    acquireTime,
    icaoCode,
    registration,
    flightNo,
    callsign,
    flightType,
    isMilitary,
    pkType,
    srcPk,
    flightCategory,
    flightCategoryName,
    lng,
    lat,
    speed,
    speedKm,
    altitudeBaro,
    altitudeBaroM,
    altitudeGeom,
    altitudeGeomM,
    heading,
    squawkCode,
    flightStatus,
    special,
    originAirport3Code,
    originAirportEName,
    originAirportCName,
    originLng,
    originLat,
    destAirport3Code,
    destAirportEName,
    destAirportCName,
    destLng,
    destLat,
    flightPhoto,
    flightDepartureTime,
    expectedLandingTime,
    toDestinationDistance,
    estimatedLandingDuration,
    airlinesIcao,
    airlinesEName,
    airlinesCName,
    countryCode,
    countryName,
    dataSource,
    source,
    positionCountryCode2,
    positionCountryName,
    friendFoe,
    seaId,
    seaName,
    h3Code,
    extendInfo,
    'AIRCRAFT'                      as targetType,
    from_unixtime(unix_timestamp()) as updateTime -- 数据入库时间
from tmp_table03;


end;




