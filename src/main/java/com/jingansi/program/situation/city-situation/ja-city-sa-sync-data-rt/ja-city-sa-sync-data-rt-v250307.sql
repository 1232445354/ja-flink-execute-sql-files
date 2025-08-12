--********************************************************************--
-- author:      write your name here
-- create time: 2025/3/7 11:11:38
-- description: 城市态势同步数据
-- version: ja-city-sa-sync-data-rt-v250307
--********************************************************************--
set 'pipeline.name' = 'ja-city-sa-sync-data-rt';

-- 以经纬度筛选需城市态势所需的飞机船舶数据

SET 'parallelism.default' = '5';

SET 'execution.checkpointing.interval' = '300000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-city-sa-sync-data-rt';

-- 飞机 来源kafka的源数据
drop table if exists aircraft_source;
create table aircraft_source(
                                id                         string, -- id
                                srcCode                    bigint, --网站标识
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
      'properties.group.id' = 'ja-city-sa-sync-data-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1737986719000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );

-- 船舶kafka源来
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
                                 updateTime                string-- flink处理时间

) with (
      'connector' = 'kafka',
      'topic' = 'vessel-fuse-list',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-city-sa-sync-data-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'properties.auto.offset.reset' = 'earliest',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1741090200000',
      'format' = 'json',
      'key.format' = 'json',
      'key.fields' = 'vessel_id',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );




drop table if exists city_sa_sh_aircraft_source;
create table city_sa_sh_aircraft_source(
                                           id                         string, -- id
                                           srcCode                    bigint, --网站标识
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
      'topic' = 'city-sa-sh-aircraft_source',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-city-sa-sync-data-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1737986719000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );

drop table if exists city_sa_sh_vessel_source;
create table city_sa_sh_vessel_source(
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
                                         updateTime                string-- flink处理时间

) with (
      'connector' = 'kafka',
      'topic' = 'city-sa-sh-vessel_source',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-city-sa-sync-data-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'properties.auto.offset.reset' = 'earliest',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1741090200000',
      'format' = 'json',
      'key.format' = 'json',
      'key.fields' = 'vessel_id',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );





begin statement set;
-- 通过上海的左下角和右上角筛选
insert into city_sa_sh_aircraft_source
select
    if(id='ABC123' and lng between 91 and 124 and lat between 15 and 46,'ABC123-JH',id)  as  id                       , -- id
    srcCode                    , --网站标识
    acquireTime                , -- 采集事件年月日时分秒
    icaoCode                   , -- icao
    registration               , -- 注册号
    flightNo                   , -- 航班号
    callsign                   , -- 呼号
    flightType                 , -- 飞机型号
    isMilitary                 , -- 是否军用
    pkType                     , -- flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex
    srcPk                      , -- 源网站主键
    flightCategory             , -- 飞机类别
    flightCategoryName         , -- 飞机类别名称
    lng                        , -- 经度
    lat                        , -- 纬度
    speed                      , -- 速度节
    speedKm                    , -- 速度 km
    altitudeBaro               , -- 气压高度 海拔 航班当前高度，单位为（ft）
    altitudeBaroM              , -- 气压高度 海拔 单位米
    altitudeGeom               , -- 海拔高度 海拔 航班当前高度，单位为（ft）
    altitudeGeomM              , -- 海拔高度 海拔 单位米
    heading                    , -- 方向
    squawkCode                 , -- 应答器代码
    flightStatus               , -- 飞机状态
    special                    , -- 是否有特殊情况
    originAirport3Code         , -- 起飞机场的iata代码
    originAirportEName         , -- 来源机场英文
    originAirportCName         , -- 来源机场中文
    originLng                  , -- 来源机场经度
    originLat                  , -- 来源机场纬度
    destAirport3Code           , -- 目的机场3字代码
    destAirportEName           , -- 目的机场英文
    destAirportCName           , -- 目的机场中文
    destLng                    , -- 目的地坐标经度
    destLat                    , -- 目的地坐标纬度
    flightPhoto                , -- 飞机的图片
    flightDepartureTime        , -- 飞机起飞时间
    expectedLandingTime        , -- 预计降落时间
    toDestinationDistance      , -- 距离目的地距离
    estimatedLandingDuration   , -- 预计还要多久着陆
    airlinesIcao               , -- 航空公司icao
    airlinesEName              , -- 航空公司英文
    airlinesCName              , -- 航空公司中文
    countryCode                , -- 国家代码
    countryName                , -- 国家名称
    dataSource                 , -- 数据来源的系统
    source                     , -- 来源
    positionCountryCode2       , -- 所处国家
    positionCountryName        , -- 所处国家名称
    friendFoe                  , -- 敌我代码
    seaId                      , -- 海域id
    seaName                    , -- 海域名称
    h3Code                     , -- 位置h3编码
    extendInfo                 , -- 扩展信息 json 串
    targetType                 , -- 实体类型 固定值 AIRCRAFT
    updateTime                   -- flink处理时间
from aircraft_source
where (lng between 120.5 and 122.5
    and lat between 30.1 and 31)
   or (id='ABC123' and lng between 91 and 124 and lat between 15 and 46)
;


insert into city_sa_sh_vessel_source
select
    *
from vessel_fuse_list
where lng between 120.5 and 122.5
  and lat between 30.1 and 31;

end;

