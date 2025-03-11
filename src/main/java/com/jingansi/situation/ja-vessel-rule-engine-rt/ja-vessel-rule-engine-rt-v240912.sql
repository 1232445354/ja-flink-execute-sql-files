--********************************************************************--
-- author:      write your name here
-- create time: 2024/9/12 13:40:13
-- description: 数据读取给规则引擎
--********************************************************************--
set 'pipeline.name' = 'ja-vessel-rule-engine-rt';


set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '6';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-vessel-rule-engine-rt';

-- 数据来源kafka
drop table if exists vessel_fuse_list;
create table vessel_fuse_list(
                                 vessel_id                      bigint        comment '船舶编号（主键）',
                                 acquire_time                   varchar(200)  comment '采集时间',
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
                                 b_country_code            STRING, -- 国家编码
                                 proctime          as PROCTIME()

) with (
      'connector' = 'kafka',
      'topic' = 'vessel-fuse-list',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-vessel-fuse-rt-rule-engine',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'properties.auto.offset.reset' = 'earliest',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1725336000000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


create table dws_vessel_et_info_rt (
                                       vessel_id                      bigint        comment '船舶编号（主键）',
                                       vessel_name                    string        comment '船舶英文名称',
                                       mmsi                           bigint        comment 'mmsi',
                                       imo                            bigint        comment 'imo',
                                       callsign                       string        comment '呼号',
                                       length                         double        comment '长度 / m',
                                       width                          double        comment '宽度 / m',
                                       height                         double        comment '高度 / m',
                                       country_code                   string        comment '国家编码',
                                       vessel_class_code              int,
                                       vessel_type_code               int,
                                       friend_foe                     string,
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



-- ****************************规则引擎写入数据******************************** --

drop table if exists vessel_source;
create table vessel_source(
                              id                    bigint, -- id
                              acquireTime           string, -- 采集事件年月日时分秒
                              acquireTimestamp      bigint, -- 采集时间戳
                              vesselName            string, -- 船舶名称,写出给规则引擎
                              mmsi                  string, -- mmsi
                              imo                   string, -- imo
                              callsign              string, -- 呼号
                              cnIso2                string, -- 国家代码
                              vesselClass           string, -- 大类型编码
                              vesselType            string, -- 小类型编码
                              friendFoe             string, -- 敌我代码
                              positionCountryCode2  string, -- 所处国家
                              lng                   double, -- 经度
                              lat                   double, -- 纬度
                              orientation           double, -- 方向
                              speed                 double, -- 速度 节
                              speedKm               double, -- 速度km/h
                              rateOfTurn            double, -- 转向率
                              draught               double, -- 吃水
                              length                double, -- 长度
                              width                 double, -- 宽度
                              height                double, -- 高度
                              targetType            string, -- 实体类型 固定值 VESSEL
                              updateTime            string -- flink处理时间
) with (
      'connector' = 'kafka',
      'topic' = 'vessel_source_rule_engine',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'vessel_rule_engine',
      'key.format' = 'json',
      'key.fields' = 'id',
      'format' = 'json'
      );




insert into vessel_source
select
    t1.vessel_id                 as id,
    acquire_time                 as acquireTime,
    unix_timestamp(acquire_time,'yyyy-MM-dd HH:mm:ss')  as acquireTimestamp,
    t1.vessel_name               as vesselName,
    cast(t1.mmsi as string)      as mmsi,
    cast(t1.imo as string)       as imo,
    t1.callsign,
    t1.b_country_code           as cnIso2,
    cast(t2.vessel_class_code as string)    as vesselClass,
    cast(t2.vessel_type_code as string)  as vesselType,
    t2.friend_foe as friendFoe,
    position_country_code2 as positionCountryCode2,
    lng                   ,
    lat                   ,
    orientation           ,
    speed                 ,
    speed_km              as speedKm,
    rate_of_turn          as rateOfTurn,
    draught               ,
    t2.length             ,
    t2.width              ,
    t2.height             ,
    'VESSEL'               as targetType,
    from_unixtime(unix_timestamp()) as updateTime

from vessel_fuse_list as t1 left join dws_vessel_et_info_rt
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                                      on t1.vessel_id = t2.vessel_id;






