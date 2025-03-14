--********************************************************************--
-- author:      write your name here
-- create time: 2024/7/9 20:03:37
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ja-vessel-merge-source-rt';


set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '6';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-vessel-merge-source-rt';


-----------------------

 -- 数据结构

 -----------------------

-- 船舶数据
drop table if exists vessel_source;
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
                              flag                  string
) with (
      'connector' = 'kafka',
      'topic' = 'vessel_source',
      'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
      'properties.group.id' = 'vessel_source_source_idc',
      --'scan.startup.mode' = 'group-offsets',
      --'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1724580000000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


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
                                        nav_status 							string 		comment '航行状态',
                                        nav_status_name                     string      comment '航行状态中文',
                                        rate_of_turn						double  	comment '转向率',
                                        orientation							double 		comment '方向',
                                        lng 					            double 		comment '经度',
                                        lat 					            double 		comment '纬度',
                                        source								string 		comment	'来源类型',
                                        speed								double 		comment '速度',
                                        speed_km		                    double      comment '速度 单位 km/h ',
                                        draught								double 		comment '吃水深度',
                                        vessel_class						string		comment '船类型',
                                        vessel_class_name                   string      comment '船类型中文',
                                        vessel_type                         string      comment '船小类别',
                                        vessel_type_name                    string      comment '船类型中文名-小类',
                                        cn_iso2								string		comment '国家code',
                                        country_name                        string      comment '国家中文',
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
     'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'sa.dwd_ais_vessel_all_info',
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


-- 创建映射doris的状态数据表(Sink:doris)
drop table if exists dws_ais_vessel_status_info;
create table dws_ais_vessel_status_info (
                                            vessel_id      	 				    bigint		comment '船ID',
                                            acquire_timestamp_format			string      comment '时间戳格式化',
                                            acquire_timestamp					bigint		comment '时间戳',
                                            vessel_name							string      comment '船名称',
                                            c_name                              string      comment '船中文名',
                                            imo                                 string      comment 'imo',
                                            mmsi                                string      comment 'mmsi',
                                            callsign                            string      comment '呼号',
                                            nav_status 							string 		comment '航行状态',
                                            nav_status_name                     string      comment '航行状态中文',
                                            rate_of_turn						double  	comment '转向率',
                                            orientation							double 		comment '方向',
                                            lng 					            double 		comment '经度',
                                            lat 					            double 		comment '纬度',
                                            source								string 		comment	'来源类型',
                                            speed								double 		comment '速度',
                                            speed_km		                    double      comment '速度 单位 km/h ',
                                            draught								double 		comment '吃水深度',
                                            vessel_class						string		comment '船类型',
                                            vessel_class_name                   string      comment '船类型中文',
                                            vessel_type                         string      comment '船小类别',
                                            vessel_type_name                    string      comment '船类型中文名-小类',
                                            cn_iso2								string		comment '国家code',
                                            country_name                        string      comment '国家中文',
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
     'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'sa.dws_ais_vessel_status_info',
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



-----------------------

-- 数据处理

-----------------------
drop view if exists temp_01;
create view temp_01 as
select
    id                                    as vessel_id,
    acquireTime                           as acquire_timestamp_format,
    acquireTimestamp                      as acquire_timestamp,
    coalesce(sourceShipname,vesselName)   as vessel_name,
    cName                                 as c_name,
    coalesce(sourceImo,imo)               as imo,
    coalesce(sourceMmsi,mmsi)             as mmsi,
    coalesce(sourceCallsign,callsign)     as callsign,
    navStatus                             as nav_status,
    navStatusName                         as nav_status_name,
    rateOfTurn                            as rate_of_turn,
    orientation,
    lng,
    lat,
    source,
    speed,
    speedKm                               as speed_km,
    draught,
    coalesce(sourceVesselClass,vesselClass)             as vessel_class,
    coalesce(sourceVesselClassName,vesselClassName)    as vessel_class_name,
    coalesce(sourceVesselType,vesselType)               as vessel_type,
    coalesce(sourceVesselTypeName,vesselTypeName)      as vessel_type_name,
    coalesce(sourceCountryCode,cnIso2)                  as cn_iso2,
    coalesce(sourceCountryName,countryName)             as country_name,
    blockMapIndex                                        as block_map_index,
    blockRangeX                                          as block_range_x,
    blockRangeY                                          as block_range_y,
    positionCountryCode2                                 as position_country_code2,
    friendFoe                                            as friend_foe,
    seaId                                                as sea_id,
    seaName                                              as sea_name
from vessel_source;
-- where flag is not null;



-----------------------

-- 数据入库

-----------------------

begin statement set;

insert into dwd_ais_vessel_all_info
select
    *,
    from_unixtime(unix_timestamp()) as update_time
from temp_01;


insert into dws_ais_vessel_status_info
select
    *,
    from_unixtime(unix_timestamp()) as update_time
from temp_01;


end;





