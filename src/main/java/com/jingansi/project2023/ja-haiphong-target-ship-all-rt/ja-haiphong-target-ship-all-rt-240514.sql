-- ********************************************************************--
-- author:  yibo@jingan-inc.com
-- create time: 2022/8/25 19:46:09
-- description: ais、radar融合程序
-- ********************************************************************--

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'pipeline.name' = 'ja-haiphong-target-ship-all-rt';
SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '60000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-haiphong-target-ship-all-rt';

-- SET 'table.exec.mini-batch.enabled'='true';
-- SET 'table.exec.mini-batch.allow-latency'='5s';
-- SET 'table.exec.mini-batch.size'='5000';

SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='drop' ;


-- 自定义函数，转换经纬度
-- 计算两个经纬度点位之间的距离
create function distance_udf as 'com.jingan.udf.geohash.DistanceUdf';


-- ais船只数据来源表（Source：kafka）
drop table if exists ja_target_ais_kafka;
create table ja_target_ais_kafka (
                                     imo                bigint            comment '船只唯一标识,可能为空',
                                     aisId              string            comment '所属aisId',
                                     mmsi               string            comment 'mmsi,唯一标识不为空',
                                     shipName           string            comment '船只名称',
                                     longitude          string            comment '经度',
                                     latitude           string            comment '纬度',
                                     courseOverGround   double            comment '对地航向',
                                     speedOverGround    double            comment '对地航速',
                                     trueHeading        bigint            comment '船首向',
                                     navigationStatus   string            comment '航行状态',
                                     cycleSpeed         string            comment '旋回速度',
                                     callSign           string            comment '呼号',
                                     shipType           string            comment '船只类型',
                                     shipDraft          bigint            comment '吃水',
                                     shipLength         double            comment '船长',
                                     shipWidth          double            comment '船宽',
                                     dangerousGoods     string            comment '危险品货物',
                                     destination        string            comment '目的地',
                                     rateOfTurn         double            comment '转向率',
                                     receiveTime        bigint            comment '接收时间',
                                     longitudeAsString  string            comment '带东经西经的经度',
                                     latitudeAsString   string            comment '带南纬北纬的纬度',
                                     gmtCreateBy        string            comment '创建者',
                                     nationality        string            comment '国家',
                                     messageType        string            comment '消息类型',
                                     rowtime as to_timestamp_ltz(receiveTime * 1000,3),
                                     watermark for rowtime as rowtime - interval '3' second
) WITH (
      'connector' = 'kafka',
      'topic' = 'ja_target_ais',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-target-ais-rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 雷达数据来源表（Source：kakfa）
drop table if exists ja_target_radar_kafka;
create table ja_target_radar_kafka (
                                       radarId       string   , -- '雷达id'
                                       targetId      string   , -- '目标id'
                                       distance      double   , -- '距离'
                                       angle         double   , -- '角度'
                                       longitude     string   , -- '经度'
                                       latitude      string   , -- '纬度'
                                       receiveTime   bigint   , -- '接收时间'
                                       gmtCreateBy   string   , -- '创建人'
                                       proctime as PROCTIME() , -- 维表关联的时间函数
                                       rowtime as to_timestamp_ltz(receiveTime,3),
                                       watermark for rowtime as rowtime - interval '3' second
) with (
      'connector' = 'kafka',
      'topic' = 'ja_target_radar',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-target-radar-rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- ais船只全量数据表，存储的是ais的自己单独的轨迹数据
drop table if exists ais_target_ship_all_info;
create table ais_target_ship_all_info (
                                          mmsi                string,
                                          receive_time        timestamp,
                                          imo                 bigint,
                                          ais_id              string,
                                          ship_name           string,
                                          nationality         string,
                                          longitude           double,
                                          latitude            double,
                                          course_over_ground  double,
                                          speed_over_ground   double,
                                          true_heading        bigint,
                                          navigation_status   string,
                                          cycle_speed         string,
                                          call_sign           string,
                                          ship_type           string,
                                          ship_draft          bigint,
                                          ship_length         double,
                                          ship_width          double,
                                          dangerous_goods     string,
                                          destination         string,
                                          rate_of_turn        double,
                                          message_type        string,
                                          longitude_as_string string,
                                          latitude_as_string  string,
                                          site_distance       double,
                                          gmt_create_by       string,
                                          update_time         string
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_chingchi_icos_haiphong.ais_target_ship_all_info',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='2000',
     'sink.batch.interval'='10s'
     );

-- ais船只状态数据表，存储的是每个mmsi最新的一条数据，但是是一个纵表
drop table if exists ais_target_ship_info;
create table ais_target_ship_info (
                                      mmsi 				string       comment '船只唯一标识',
                                      ais_id            string       comment 'ais基站id',
                                      index_name 		string 		 comment '字段名称',
                                      receive_time      timestamp    comment '接收时间',
                                      message_type      string       comment '消息类型',
                                      index_value       string       comment '字段值',
                                      gmt_create_by 	string       comment '创建者',
                                      update_time       string       comment '数据入库时间'
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_chingchi_icos_haiphong.ais_target_ship_info',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='2000',
     'sink.batch.interval'='2s'
     );


-- 雷达的全量数据表，两个雷达的数据全部进入
drop table if exists radar_target_ship_all_info;
create table radar_target_ship_all_info (
                                            radar_id        string,
                                            target_id       string,
                                            receive_time    timestamp,
                                            distance        double,
                                            angle           double,
                                            longitude       double,
                                            latitude        double,
                                            site_distance   double,
                                            gmt_create_by   string,
                                            update_time     string
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_chingchi_icos_haiphong.radar_target_ship_all_info',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='2000',
     'sink.batch.interval'='10s'
     );



-- 雷达的目标状态数据表，两个雷达的数据全部进入
drop table if exists radar_target_ship_info;
create table radar_target_ship_info (
                                        radar_id       string   	 comment '雷达id',
                                        target_id  	 string 	 comment '目标id',
                                        receive_time   timestamp   comment '接收时间',
                                        distance       double      comment '距离',
                                        angle          double      comment '角度',
                                        longitude   	 double 	 comment '经度',
                                        latitude       double      comment '纬度',
                                        gmt_create_by  string      comment '创建者',
                                        update_time    string      comment '数据入库时间'
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_chingchi_icos_haiphong.radar_target_ship_info',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='2000',
     'sink.batch.interval'='2s'
     );


-- 雷达融合目标的状态数据信息表，融合不上就是各自雷达的目标id，融合上就随便取了coalesce(target_id1,target_id2)
drop table if exists merge_radar_target_ship_info;
create table merge_radar_target_ship_info (
                                              target_id  	 string 	 comment '目标id',
                                              radar_id       string   	 comment '雷达id',
                                              receive_time   timestamp   comment '接收时间',
                                              distance       double      comment '距离',
                                              angle          double      comment '角度',
                                              longitude   	 double 	 comment '经度',
                                              latitude       double      comment '纬度',
                                              gmt_create_by  string      comment '创建者',
                                              update_time    string      comment '数据入库时间'
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_chingchi_icos_haiphong.merge_radar_target_ship_info',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='2000',
     'sink.batch.interval'='2s'
     );


-- 雷达融合目标对应关系数据,两个雷达数据的融合对应关系
drop table if exists merge_radar_target_ship_relation_info;
create table merge_radar_target_ship_relation_info (
                                                       target_id      string     comment '目标id',
                                                       target_id1  	 string 	comment '目标id1',
                                                       target_id2  	 string 	comment '目标id2',
                                                       radar_id1      string   	comment '雷达id1',
                                                       radar_id2      string   	comment '雷达id2',
                                                       earliest_time  timestamp  comment '最早时间',
                                                       latest_time    timestamp  comment '最晚时间',
                                                       cnt            bigint     comment '数量',
                                                       gmt_create_by  string     comment '创建者',
                                                       update_time    string     comment '数据入库时间'
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_chingchi_icos_haiphong.merge_radar_target_ship_relation_info',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='2000',
     'sink.batch.interval'='10s'
     );


-- ais、radar融合全量数据表，融合上id就是mmsi，融合不上就是雷达目标id，融合上type是ais，融合不上就是radar,多带了个distance字段，为了再次读取数据处理的
drop table if exists merge_target_ship_all_info;
create table merge_target_ship_all_info (
                                            id 			string       comment '船只唯一标识，mmsi或是target_id',
                                            type 			string	     comment '类型，雷达 or ais',
                                            receive_time  timestamp    comment '接收时间',
                                            longitude     double       comment '经度',
                                            latitude      double       comment '纬度',
                                            distance      double       comment '雷达监测数据的距离',
                                            gmt_create_by string       comment '创建人',
                                            update_time   string       comment '数据入库时间'
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_chingchi_icos_haiphong.merge_target_ship_all_info',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='2000',
     'sink.batch.interval'='10s'
     );



-- ais、radar融合的实时位置状态表，融合目标的状态数据
drop table if exists merge_target_ship_info;
create table merge_target_ship_info (
                                        id 			      string    comment '船只唯一标识，mmsi或是target_id',
                                        type 			      string 	comment '字段类型，雷达 or ais',
                                        receive_time        timestamp comment '消息类型',
                                        longitude           double    comment '经度',
                                        latitude            double    comment '纬度',
                                        ship_name           string    comment '船只名称',
                                        ais_id              string    comment 'ais基站id',
                                        distance            double    comment '雷达数据的距离',
                                        angle               double    comment '雷达角度，度',
                                        gmt_create_by       string    comment '创建人',
                                        update_time         string    comment '数据入库时间'
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_chingchi_icos_haiphong.merge_target_ship_info',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='2000',
     'sink.batch.interval'='2s'
     );


-- 雷达数据进入数据的最小时间（延迟告警）雷达数据进去的时间，为了控制进去不告警，等一会ais数据融合不上在告警，融合上就是ais告警
drop table if exists radar_target_ship_entry_info;
create table radar_target_ship_entry_info (
                                              target_id      string      comment '目标id',
                                              earliest_time  timestamp   comment '雷达数据进入的最小时间',
                                              latest_time    timestamp   comment '雷达数据进入的最大时间',
                                              cnt            bigint      comment '数量',
                                              update_time    string      comment '数据入库时间'
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_chingchi_icos_haiphong.radar_target_ship_entry_info',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='2000',
     'sink.batch.interval'='5s'
     );


-- ais、radar融合对应关系表，两种数据融合的最早时间最晚时间，主键是mmsi
drop table if exists target_ship_relation_ais_info;
create table target_ship_relation_ais_info (
                                               mmsi 			string	   comment '船只唯一标识',
                                               target_id 		string     comment '雷达目标id',
                                               earliest_time   timestamp  comment '最早时间',
                                               latest_time     timestamp  comment '最晚时间',
                                               update_time     string     comment '数据入库时间'
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_chingchi_icos_haiphong.target_ship_relation_ais_info',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='2000',
     'sink.batch.interval'='2s'
     );


--  ais、radar融合对应关系表，还是融合表，主键是雷达目标id
drop table if exists target_ship_relation_info;
create table target_ship_relation_info (
                                           target_id 		string     comment '雷达目标id',
                                           mmsi 			string	   comment '船只唯一标识',
                                           earliest_time   timestamp  comment '最早时间',
                                           latest_time     timestamp  comment '最晚时间',
                                           cnt             bigint     comment '数量',
                                           ship_name       string     comment '船只名称',
                                           ais_id          string     comment 'ais基站的id',
                                           gmt_create_by   string     comment '创建人',
                                           update_time     string     comment '数据入库时间'
)with (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'ja_chingchi_icos_haiphong.target_ship_relation_info',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='2000',
     'sink.batch.interval'='2s'
     );



-- 事件告警表（Sink：kakfa）
drop table if exists event_warn_kafka;
create table event_warn_kafka(
                                 eventId             string     comment '唯一编号',
                                 eventNo             string     comment '事件编号',
                                 eventName           string     comment '事件名称',
                                 deviceId            string     comment '设备id',
                                 deviceName          string     comment '设备名称',
                                 deviceType          string     comment '设备类型',
                                 eventType           string     comment '事件类型,anomaly：异常物,dangerous：危险物 必填',
                                 `level`             string     comment '紧急等级 High、Middle、Low 必填',
                                 eventTime           timestamp  comment '事件发生时间 必填',
                                 longitude           double     comment '经度',
                                 latitude            double     comment '纬度',
                                 targetId            string     comment 'mmsi或者是雷达的target目标id',
                                 radarTargetId       string     comment '雷达的target_id',
    --    source              string     comment '空',
    --    leftTopX            bigint     comment '左上角x坐标',
    --    leftTopY            bigint     comment '左上角Y坐标',
    --    bboxWidth           bigint     comment '目标宽度',
    --    bboxHeight          bigint     comment '目标高度',
    --    sourceFrameWidth    bigint     comment '原始帧宽度',
    -- sourceFrameHeight   bigint     comment '原始帧高度',
    --    positionId          bigint     comment '点位id（无人机、机器狗告警时有）',
    --    confidence          double     comment '置信度',
    --    pointX              double     comment 'x轴',
    --  	pointY              double     comment 'y轴',
    -- pointZ              double     comment 'z轴',
    --    itemName            string     comment '危险物名称,危险物告警时传',
    --    image               string     comment '异常物的小图uri地址',
    --    sourceImage         string     comment '异常物的大图uri地址',
    --    spaceType           string     comment '空间类型',
    --  	spaceId             string     comment '空间Id',
    -- reason              string     comment '事件发生原因',
                                 primary key (targetId,eventTime) NOT ENFORCED
) with (
      'connector' = 'upsert-kafka',
      'topic' = 'event_warn',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'event-warn-kafka-rt',
      'key.format' = 'json',
      'value.format' = 'json'
      );




-- 落水人员基本信息——维表（Source：mysql）
drop table if exists ais_mob;
create table ais_mob (
                         id               bigint       comment '主键',
                         name             string       comment '船员姓名',
                         crew_sn          string       comment '船员编号',
                         device_id        string       comment '对应的预警器编号',
                         device_name      string       comment '预警器设备名称',
                         device_sn        string       comment '预警设备序列号',
                         model            string       comment '预警器设备型号',
                         primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'ais_mob',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '3'
      );


-- 船只黑名单、白名单表（Source：mysql）
drop table if exists black_white_list;
create table black_white_list (
                                  id              bigint  comment '主键',
                                  site_id         bigint  comment '所属站点id',
                                  mmsi            string  comment '船只mmsi',
                                  name            string  comment '船只英文名称',
                                  chinese_name    string  comment '船只中文名称',
                                  type            string  comment '类型，black:黑名单,white:白名单',
                                  primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'black_white_list',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '3'
      );



-- 钻井平台信息表（Source：mysql）
drop table if exists site;
create table site (
                      id                  bigint        comment '主键',
                      sn                  string        comment '点位唯一标识',
                      type                string        comment '点位类型,DRILLING_PLATFORM:钻井平台',
                      name                string        comment '点位名称',
                      image               string        comment '图片uri',
                      longitude           double        comment '经度',
                      latitude            double        comment '纬度',
                      site1_radius        double        comment '一级防护区半径',
                      site2_radius        double        comment '二级防护区半径',
                      site3_radius        double        comment '三级防护区半径',
                      site4_radius        double        comment '四级防护区半径',
                      site5_radius        double        comment '五级防护区半径',
                      warning_area_range  double        comment '警戒区半径',
                      primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'site',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '3'
      );


-- ais、radar融合对应关系表（Source：doris）
drop table if exists target_ship_relation_info_source;
create table target_ship_relation_info_source (
                                                  target_id 		string     comment '雷达目标id',
                                                  mmsi 			string	   comment '船只唯一标识',
                                                  earliest_time   timestamp  comment '最早时间',
                                                  latest_time     timestamp  comment '最晚时间',
                                                  ship_name       string     comment '船只名称',
                                                  ais_id          string     comment 'ais基站的id',
                                                  primary key (target_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:31030/ja_chingchi_icos_haiphong?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'target_ship_relation_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '1000',
      'lookup.cache.ttl' = '30s',
      'lookup.max-retries' = '1'
      );


-- 雷达数据进入数据的最小时间（延迟告警）(Source:doris)
drop table if exists radar_target_ship_entry_info_source;
create table radar_target_ship_entry_info_source (
                                                     target_id      string   	 comment '目标id',
                                                     earliest_time  timestamp     comment '雷达数据进入的最小时间',
                                                     primary key (target_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:31030/ja_chingchi_icos_haiphong?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'radar_target_ship_entry_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '1000',
      'lookup.cache.ttl' = '5s',
      'lookup.max-retries' = '1'
      );


-- ais和雷达的对应关系数据入库，主键为mmsi(Source:doris)
drop table if exists target_ship_relation_ais_info_source;
create table target_ship_relation_ais_info_source (
                                                      mmsi 			string	   comment '船只唯一标识',
                                                      target_id 		string     comment '雷达目标id',
                                                      earliest_time   timestamp  comment '最早时间',
                                                      latest_time     timestamp  comment '最晚时间',
                                                      primary key (mmsi) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:31030/ja_chingchi_icos_haiphong?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'target_ship_relation_ais_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '5s',
      'lookup.max-retries' = '1'
      );




-- -- 告警记录信息表（Sink：doris）
-- drop table if exists event_alarm_info_doris;
-- create table event_alarm_info_doris (
--   id 		        string  	comment 'mmsi或者是雷达的目标id',
--   event_time  		timestamp  	comment '告警时间',
--   device_id   		string 		comment '设备id',
--   event_name  		string 	 	comment '事件名称',
--   device_name   	string 		comment '设备名称',
--   device_type  		string  	comment '设备类型',
--   event_type  		string  	comment '事件类型',
--   `level` 			string  	comment '告警级别',
--   longitude 		double 		comment '经度',
--   latitude 		    double  	comment '纬度',
--   update_time 	    string  	comment '数据入库时间'
-- )with (
-- 'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
-- 'table.identifier' = 'ja_chingchi_icos_haiphong.event_alarm_info',
-- 'username' = 'root',
-- 'password' = 'Jingansi@110',
-- 'doris.request.tablet.size'='1',
-- 'doris.request.read.timeout.ms'='30000',
-- 'sink.batch.size'='2000',
-- 'sink.batch.interval'='10s'
-- );



---------------

-- 数据处理

---------------

-- 对雷达数据的jsonArray数组进行解析、关联钻井平台，计算距离
drop view if exists tmp_ja_target_radar_01;
create view tmp_ja_target_radar_01 as
select
    t1.radarId                   as radar_id,
    t1.targetId                  as target_id,
    -- from_unixtime(t1.receiveTime/1000,'yyyy-MM-dd HH:mm:ss') as receive_time, -- 接收时间string类型
    -- to_timestamp(from_unixtime(t1.receiveTime/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as receive_time, -- 接收时间timestamp类型 来源ms
    to_timestamp(from_unixtime(t1.receiveTime,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as receive_time, -- 接收时间timestamp类型 来源s
    t1.gmtCreateBy               as gmt_create_by,
    t1.distance                  ,
    t1.angle                     ,
    t1.rowtime                   ,
    t1.proctime                  ,
    cast(t1.longitude as double) as longitude,
    cast(t1.latitude as double)  as latitude,
    t2.type                      as site_type,
    t2.longitude                 as site_longitude,
    t2.latitude                  as site_latitude,
    t2.site1_radius                                                 , -- 钻井平台一级防护区半径 1km
    t2.site2_radius                                                 , -- 钻井平台二级防护区半径 2km
    t2.site3_radius                                                 , -- 钻井平台三级防护区半径 3km
    t2.warning_area_range                                           , -- 钻井平台警戒区范围 1km
    t2.site3_radius + t2.warning_area_range as warning_area_radius  , -- 钻井平台警戒区半径 4km
    distance_udf(
            cast(t1.latitude as double),
            cast(t1.longitude as double),
            t2.latitude,
            t2.longitude) * 1000 as site_distance  -- 单位m
from ja_target_radar_kafka as t1
         left join site
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on 'DRILLING_PLATFORM' = t2.type
where t2.longitude is not null
  and t2.latitude is not null
  and t1.radarId is not null
  and t1.radarId <> ''
  and t1.targetId is not null
  and t1.targetId <> ''
  and t1.receiveTime is not null;


-- 对ais中的数据MMSI进行切分取前三位和后三位，关联钻井平台，计算距离
drop view if exists tmp_ja_target_ais_02;
create view tmp_ja_target_ais_02 as
select
    t1.*,
    -- to_timestamp(receiveTime,'yyyy-MM-dd HH:mm:ss') as receiveTimeDateType,
    t2.type as site_type,
    t2.longitude as site_longitude,
    t2.latitude as site_latitude,
    t2.site1_radius                                                 , -- 钻井平台一级防护区半径
    t2.site2_radius                                                 , -- 钻井平台二级防护区半径
    t2.site3_radius                                                 , -- 钻井平台三级防护区半径
    t2.warning_area_range                                           , -- 钻井平台警戒区范围
    t2.site3_radius + t2.warning_area_range as warning_area_radius  , -- 钻井平台警戒区半径
    distance_udf(
            t1.latitude,
            t1.longitude,
            t2.latitude,
            t2.longitude) * 1000 as site_distance  -- m
    -- longitude - cast(pre_10_longitude as double) as diff_longitude,
    -- latitude - cast(pre_10_latitude as double) as diff_latitude
from (
         select
             imo                                           , -- 船只唯一标识,可能为空
             aisId                                         , -- 所属aisId
             mmsi                                          , -- mmsi,唯一标识不为空
             substring(mmsi,1,3) as person_identification  , -- 前三位个人标识
             substring(mmsi,4) as person_no                , -- 后六位个人配置编号
             shipName                                      , -- 船只名称
             cast(longitude as double) as longitude        , -- 经度
             cast(latitude as double) as latitude          , -- 纬度
             courseOverGround                              , -- 对地航向
             speedOverGround                               , -- 对地航速
             trueHeading                                   , -- 船首向
             navigationStatus                              , -- 航行状态
             cycleSpeed                                    , -- 旋回速度
             callSign                                      , -- 呼号
             shipType                                      , -- 船只类型
             shipDraft                                     , -- 吃水
             shipLength                                    , -- 船长
             shipWidth                                     , -- 船宽
             dangerousGoods                                , -- 危险品货物
             destination                                   , -- 目的地
             to_timestamp(from_unixtime(receiveTime,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as receiveTime, -- 接收时间
             rateOfTurn                                    , -- 转向率
             gmtCreateBy                                   , -- 创建者
             longitudeAsString                             , -- 带东经西经的经度
             latitudeAsString                              , -- 带南纬北纬的纬度
             messageType                                   , -- 消息类型
             nationality                                   , -- 国家
             rowtime                                       , -- 水位线
             PROCTIME() as proctime	                     -- 维表关联的时间函数
             -- first_value(longitude) over(partition by mmsi order by rowtime asc) as pre_10_longitude,
             -- first_value(latitude) over(partition by mmsi order by rowtime asc) as pre_10_latitude
         from ja_target_ais_kafka
         where mmsi is not null
           and aisId is not null
           and receiveTime is not null
           and char_length(cast(receiveTime as string)) = 10 -- s级别的时间
     ) as t1
         left join site
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on 'DRILLING_PLATFORM' = t2.type
where t2.longitude is not null
  and t2.latitude is not null;


-- 对雷达的数据目标radar_id进行筛选，两个雷达的数据、并取前10个点位比较
drop view if exists tmp_ja_target_radar_02;
create view tmp_ja_target_radar_02 as
select
    radar_id                      ,
    target_id                     ,
    distance                      ,
    angle                         ,
    rowtime                       ,
    longitude                     ,
    latitude                      ,
    receive_time                  ,
    gmt_create_by                 ,
    site_type                     ,
    site_longitude                ,
    site_latitude                 ,
    site_distance                 ,
    site1_radius                  , -- 钻井平台一级防护区半径
    site2_radius                  , -- 钻井平台二级防护区半径
    site3_radius                  , -- 钻井平台三级防护区半径
    warning_area_range            , -- 钻井平台警戒区范围
    warning_area_radius             -- 钻井平台警戒区半径
    -- first_value(longitude) over(partition by target_id order by rowtime asc) as pre_10_longitude,
    -- first_value(latitude) over(partition by target_id order by rowtime asc) as pre_10_latitude
from tmp_ja_target_radar_01
where radar_id = '34020000006040105800';


drop view if exists tmp_ja_target_radar_03;
create view tmp_ja_target_radar_03 as
select
    radar_id                      ,
    target_id                     ,
    distance                      ,
    rowtime                       ,
    angle                         ,
    longitude                     ,
    latitude                      ,
    receive_time                  ,
    gmt_create_by                 ,
    site_type                     ,
    site_longitude                ,
    site_latitude                 ,
    site_distance                 ,
    site1_radius                  , -- 钻井平台一级防护区半径
    site2_radius                  , -- 钻井平台二级防护区半径
    site3_radius                  , -- 钻井平台三级防护区半径
    warning_area_range            , -- 钻井平台警戒区范围
    warning_area_radius             -- 钻井平台警戒区半径
    -- first_value(longitude) over(partition by target_id order by rowtime asc) as pre_10_longitude,
    -- first_value(latitude) over(partition by target_id order by rowtime asc) as pre_10_latitude
from tmp_ja_target_radar_01
where radar_id = '34020000006040105801';



-- 融合雷达数据---条件为接收时间相等,在计算两个关联数据的距离，筛选距离小于XXX
drop view if exists tmp_ja_target_radar_04;
create view tmp_ja_target_radar_04 as
select
    *,
    PROCTIME() as proctime                		  -- 维表关联的时间函数
    -- latitude - pre_10_latitude    as diff_latitude,
    -- longitude - pre_10_longitude  as diff_longitude
    -- to_timestamp(receive_time,'yyyy-MM-dd HH:mm:ss') as receive_time_date_type,
from (
         select
             t1.radar_id                                             as radar_id1            , -- 雷达1的id
             t2.radar_id                                             as radar_id2            , -- 雷达2的id
             t1.target_id                                            as target_id1           , -- 目标1的id
             t2.target_id                                            as target_id2           , -- 目标2的id
             coalesce(t1.radar_id,t2.radar_id)                       as radar_id             , -- 雷达id
             coalesce(t1.target_id,t2.target_id)                     as target_id            , -- 目标id
             coalesce(t1.longitude,t2.longitude)                     as longitude            , -- 雷达经度
             coalesce(t1.latitude,t2.latitude)                       as latitude             , -- 雷达纬度
             coalesce(t1.receive_time,t2.receive_time)               as receive_time         , -- 接收时间
             coalesce(t1.angle,t2.angle)                             as angle                , -- 角度
             coalesce(t1.distance,t2.distance)                       as distance             , -- 距离
             coalesce(t1.gmt_create_by,t2.gmt_create_by)             as gmt_create_by        , -- 创建人
             coalesce(t1.rowtime,t2.rowtime)                         as rowtime              , -- 水位线时间
             coalesce(t1.site_type,t2.site_type)                     as site_type            , -- 钻井平台类型
             coalesce(t1.site_longitude,t2.site_longitude)           as site_longitude       , -- 钻井平台经度
             coalesce(t1.site_latitude,t2.site_latitude)             as site_latitude        , -- 钻井平台纬度
             coalesce(t1.site_distance,t2.site_distance)             as site_distance        , -- 钻井平台距离
             coalesce(t1.site1_radius,t2.site1_radius)               as site1_radius         , -- 钻井平台一级防护区半径
             coalesce(t1.site2_radius,t2.site2_radius)               as site2_radius         , -- 钻井平台二级防护区半径
             coalesce(t1.site3_radius,t2.site3_radius)               as site3_radius         , -- 钻井平台三级防护区半径
             coalesce(t1.warning_area_range,t2.warning_area_range)   as warning_area_range   , -- 钻井平台警戒区范围
             coalesce(t1.warning_area_radius,t2.warning_area_radius) as warning_area_radius    -- 钻井平台警戒区半径
             -- coalesce(t1.pre_10_longitude,t2.pre_10_longitude) as pre_10_longitude,  -- 前10个的数据经度
             -- coalesce(t1.pre_10_latitude,t2.pre_10_latitude) as pre_10_latitude      -- 前10个的数据纬度
         from tmp_ja_target_radar_02 as t1
                  full join tmp_ja_target_radar_03 as t2
                            on t1.receive_time = t2.receive_time
                                and distance_udf(
                                            t1.latitude,
                                            t1.longitude,
                                            t2.latitude,
                                            t2.longitude) * 1000 <= 100   -- 这里的距离需要小于两个船之间的距离，默认两个船的距离很大
     ) as tt;


-- 将雷达数据和ais的数据进行融合，距离较短，在开窗，取时间最近的，在取距离最近的mmsi
drop view if exists tmp_ja_target_radar_ais_merge_05_00;
create view tmp_ja_target_radar_ais_merge_05_00 as
select
    *
from (
         select
             *,
             row_number() over(partition by target_id,receive_time order by ais_radar_target_distance asc,date_diff_flag asc) as rk -- 开窗找出距离最小的，时间最小的
        -- row_number() over(partition by target_id,receive_time order by ais_radar_target_distance asc,ais_receive_time desc) as rk
         from (
                  select
                      t1.radar_id                                              , -- 雷达id
                      t1.target_id                                             , -- 目标id
                      t1.longitude                                             , -- 经度
                      t1.latitude                                              , -- 纬度
                      t1.rowtime                                               , -- 水位线
                      t1.receive_time                                          , -- 接收时间
                      t1.angle                                                 , -- 角度
                      t1.distance                                              , -- 距离
                      t1.gmt_create_by                                         , -- 创建者
                      t2.mmsi                                                  , -- mmsi
                      t2.shipName  as ship_name                                , -- 船只名称
                      t2.aisId  as ais_id                                      , -- ais基站id
                      t2.receiveTime as ais_receive_time                       , -- ais的接收时间
                      t2.longitude as ais_longitude                            , -- ais的经度
                      t2.latitude as ais_latitude                              , -- ais的纬度
                      abs(timestampdiff(second,t1.receive_time,t2.receiveTime)) as date_diff_flag, -- 两个时间的差值s绝对值
                      -- atan2(t1.diff_latitude,t1.diff_longitude) as radar_atan  , -- 雷达的atan角度值
                      -- atan2(t2.diff_latitude,t2.diff_longitude) as ais_atan    , -- 雷达的atan角度值
                      distance_udf(
                              t1.latitude,
                              t1.longitude,
                              t2.latitude,
                              t2.longitude) * 1000 as ais_radar_target_distance -- 距离
                  from tmp_ja_target_radar_04 t1, tmp_ja_target_ais_02 t2
                  where t1.site_type = t2.site_type
                    and t1.receive_time between t2.receiveTime - interval '20' second and t2.receiveTime + interval '20' second
              ) as t
         where ais_radar_target_distance <= 100
           and mmsi is not null
         -- and (radar_atan is null or ais_atan is null or abs(radar_atan - ais_atan) <= 10)  -- 1.5
     ) as tt
where rk = 1;
-- 处理完之后，可能存在多个target_id对应到一个mmsi上，一个target_id目标也可能对应多个mmsi


-- 以雷达id和mmsi进行分组，取出关联的最大最小时间
drop view if exists tmp_ja_target_radar_ais_merge_05;
create view tmp_ja_target_radar_ais_merge_05 as
select
    target_id,
    mmsi,
    min(receive_time) as earliest_time,
    max(receive_time) as latest_time,
    count(1)          as cnt,
    max(ship_name)    as ship_name,
    max(ais_id)       as ais_id
from tmp_ja_target_radar_ais_merge_05_00
group by target_id,mmsi
having count(*) >= 15;


-- -- -- 将两个雷达融合的数据与ais的实时位置表进行关联,并进行分组
-- -- drop view if exists tmp_ja_target_radar_ais_merge_05;
-- -- create view tmp_ja_target_radar_ais_merge_05 as
-- -- select
-- --   target_id,
-- --   mmsi,
-- --   min(receive_time) as earliestTime,
-- --   max(receive_time) as latestTime,
-- --   count(target_id)  as cnt,
-- --   max(ship_name)    as ship_name,
-- --   max(ais_id)       as ais_id
-- -- from (
-- --     select
-- --       t1.radar_id          , -- 雷达id
-- --       t1.target_id         , -- 目标id
-- --       t1.longitude         , -- 经度
-- --       t1.latitude          , -- 纬度
-- --       t1.rowtime           , -- 水位线
-- --       t1.receive_time      , -- 接收时间
-- --       t1.angle             , -- 角度
-- --       t1.distance          , -- 距离
-- --       t1.gmt_create_by     , -- 创建者
-- --       t1.sub_longitude     , -- 截取的经度
-- --       t1.sub_latitude      , -- 截取的纬度
-- --       coalesce(t2.id,t3.id,t4.id,t5.id,t6.id) as mmsi                                              , -- mmsi
-- --       coalesce(t2.ship_name,t3.ship_name,t4.ship_name,t5.ship_name,t6.ship_name)  as ship_name     , -- 船只名称
-- --       coalesce(t2.ais_id,t3.ais_id,t4.ais_id,t5.ais_id,t6.ais_id) as ais_id                        , -- ais基站id
-- --       coalesce(t2.receive_time,t3.receive_time,t4.receive_time,t5.receive_time,t6.receive_time)  as ais_receive_time       ,
-- --       coalesce(t2.longitude,t3.longitude,t4.longitude,t5.longitude,t6.longitude)  as ais_longitude ,
-- --       coalesce(t2.latitude,t3.latitude,t4.latitude,t5.latitude,t6.latitude)  as ais_latitude       ,
-- --       distance_udf(
-- --                cast(t1.latitude as double),
-- --                cast(t1.longitude as double),
-- --                cast(coalesce(t2.latitude,t3.latitude,t4.latitude,t5.latitude,t6.latitude) as double),
-- --                cast(coalesce(t2.longitude,t3.longitude,t4.longitude,t5.longitude,t6.longitude) as double)) * 1000 as ais_radar_target_distance -- 距离
-- --     from tmp_ja_target_radar_04 as t1
-- --       left join target_ship_position_rt_mysql
-- --       FOR SYSTEM_TIME AS OF t1.proctime as t2
-- --       on t1.sub_longitude =  t2.sub_longitude
-- --       and t1.sub_latitude = t2.sub_latitude
-- --       and t1.receive_time0 = t2.receive_time_minute
-- --       and t1.join_type_flag = t2.type
-- --       left join target_ship_position_rt_mysql
-- --       FOR SYSTEM_TIME AS OF t1.proctime as t3
-- --       on t1.sub_longitude = t3.sub_longitude
-- --       and t1.sub_latitude = t3.sub_latitude
-- --       and t1.receive_time1 = t3.receive_time_minute
-- --       and t1.join_type_flag = t3.type
-- --       left join target_ship_position_rt_mysql
-- --       FOR SYSTEM_TIME AS OF t1.proctime as t4
-- --       on t1.sub_longitude = t4.sub_longitude
-- --       and t1.sub_latitude = t4.sub_latitude
-- --       and t1.receive_time2 = t4.receive_time_minute
-- --       and t1.join_type_flag = t4.type
-- --       left join target_ship_position_rt_mysql
-- --       FOR SYSTEM_TIME AS OF t1.proctime as t5
-- --       on t1.sub_longitude = t5.sub_longitude
-- --       and t1.sub_latitude = t5.sub_latitude
-- --       and t1.receive_time3 = t5.receive_time_minute
-- --       and t1.join_type_flag = t5.type
-- --       left join target_ship_position_rt_mysql
-- --       FOR SYSTEM_TIME AS OF t1.proctime as t6
-- --       on t1.sub_longitude = t6.sub_longitude
-- --       and t1.sub_latitude = t6.sub_latitude
-- --       and t1.receive_time4 = t6.receive_time_minute
-- --       and t1.join_type_flag = t6.type
-- --   ) as tt
-- --   where ais_radar_target_distance <= 100
-- --    and mmsi is not null
-- --   group by target_id,mmsi
-- --   having count(1) >= 3;


-- 对分组之后的数据进行开窗取第一条，得出雷达和ais融合的数据
drop view if exists tmp_ja_target_radar_ais_merge_06;
create view tmp_ja_target_radar_ais_merge_06 as
select
    target_id,
    mmsi,
    earliest_time,
    latest_time,
    cnt,
    ship_name,
    ais_id
from (
         select
             *,
             row_number() over(partition by target_id order by cnt desc) as rkk
         from tmp_ja_target_radar_ais_merge_05
     ) as tt
where tt.rkk = 1;


-- 将雷达融合的数据与（雷达和ais融合的数据——对应关系）关联，判断防护区,关联条件：目标id相等
drop view if exists tmp_ja_target_radar_ais_merge_07;
create view tmp_ja_target_radar_ais_merge_07 as
select
    t1.radar_id                , -- 雷达id
    t1.target_id               , -- 目标id
    t1.longitude               , -- 经度
    t1.latitude                , -- 纬度
    t1.receive_time            , -- 接收时间
    t1.angle                   , -- 角度
    t1.rowtime                 , -- 水位线
    t1.distance                , -- 雷达数据带来的距离
    t1.gmt_create_by           , -- 创建者
    t1.proctime                , -- 维表关联的时间
    t1.site_type               , -- 钻井平台类型
    t1.site_longitude          , -- 钻井平台经度
    t1.site_latitude           , -- 钻井平台纬度
    t1.site_distance           , -- 钻井平台距离
    t1.site1_radius            , -- 钻井平台一级防护区半径
    t1.site2_radius            , -- 钻井平台二级防护区半径
    t1.site3_radius            , -- 钻井平台三级防护区半径
    t1.warning_area_range      , -- 钻井平台警戒区范围
    t1.warning_area_radius     , -- 钻井平台警戒区半径
    t2.mmsi                    , -- 船号
    t2.ship_name               , -- 船只名称
    t2.ais_id                  , -- ais基站id
    t2.latest_time               -- 融合最大时间
from tmp_ja_target_radar_04 as t1     -- tmp_ja_target_radar_04融合的雷达数据
         left join target_ship_relation_info_source  -- ais、雷达对应关系，主键是雷达目标target_id
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.target_id = t2.target_id;


-- 将最终融合数据进行关联黑白名单，钻井信息，判断进行告警
drop view if exists tmp_ja_target_radar_ais_merge_08;
create view tmp_ja_target_radar_ais_merge_08 as
select
    t1.radar_id                                 , -- 雷达id
    t1.target_id                                , -- 目标id
    t1.longitude                                , -- 经度
    t1.latitude                                 , -- 纬度
    t1.receive_time                             , -- 接收时间
    t1.mmsi                                     , -- 船只mmsi
    t1.ship_name                                , -- 船只名称
    t1.ais_id                                   , -- ais基站id
    t1.latest_time                              , -- 融合的最大时间
    t1.site_distance                            , -- 钻井平台距离
    t1.site1_radius                             , -- 钻井平台一级防护区半径
    t1.site2_radius                             , -- 钻井平台二级防护区半径
    t1.site3_radius                             , -- 钻井平台三级防护区半径
    t1.warning_area_range                       , -- 钻井平台警戒区范围
    t1.warning_area_radius                      , -- 钻井平台警戒区半径
    t2.type                                     , -- 黑白名单类型
    t2.chinese_name                             , -- 船只的中文名称
    t2.name                                     , -- 船只的英文名称
    t3.earliest_time                              -- 雷达数据进入时间
from tmp_ja_target_radar_ais_merge_07 as t1
         left join black_white_list                  -- 黑白名单信息表
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.mmsi = t2.mmsi
         left join radar_target_ship_entry_info_source   -- 雷达数据最小时间表
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.target_id = t3.target_id
where t3.earliest_time is not null                   -- 20s延迟告警
  and abs(timestampdiff(second,t1.receive_time,t3.earliest_time)) >= 20;



-- 对雷达数据筛选除了白名单外的需要告警的数据（异常船只靠近，船只闯入防护区）
drop view if exists tmp_ja_target_radar_ais_merge_09;
create view tmp_ja_target_radar_ais_merge_09 as
select
    case when site_distance > site3_radius and site_distance <= warning_area_radius then '异常船舶靠近'
         when site_distance > site2_radius and site_distance <= site3_radius then '船舶闯入三级防护区'
         when site_distance > site1_radius and site_distance <= site2_radius then '船舶闯入二级防护区'
         when site_distance <= site1_radius then '船舶闯入一级防护区'
        end                                                       as eventName,                   -- 事件名称
    radar_id                                                  as deviceId,                    -- 设备id
    cast(null as varchar)                                     as deviceName,                  -- 设备名称
    'RADAR'                                                   as deviceType,                  --  设备类型
    case when site_distance > site3_radius and site_distance <= warning_area_radius then 'unusual_ship_near'   -- 靠近
         when site_distance <= site3_radius then 'ship_breaks_into_protected_area'
        end                                                       as eventType,                   -- 事件类型
    case when site_distance > site3_radius and site_distance <= warning_area_radius then '4'
         when site_distance > site2_radius and site_distance <= site3_radius then '3'
         when site_distance > site1_radius and site_distance <= site2_radius then '2'
         when site_distance <= site1_radius then '1'
        end                                                       as `level`,                     -- 防护区等级
    receive_time                                              as eventTime,                   -- 事件时间
    longitude                                                 ,                               --  经度
    latitude                                                  ,                               --  纬度
    if(abs(timestampdiff(second,receive_time,latest_time)) <= 60,mmsi,target_id) as targetId,
    -- coalesce(mmsi,target_id)                                  as targetId,                    -- mmsi或者是雷达目标的id
    target_id                                                 as radarTargetId
from tmp_ja_target_radar_ais_merge_08
where site_distance <= warning_area_radius
  and (type is null or type <> 'WHITE');



-- 对ais来源裁剪的数据进行将数据转成map拼接
drop view if exists tmp_ja_target_ais_03;
create view tmp_ja_target_ais_03 as
select
    mmsi,
    messageType as message_type,
    receiveTime as receive_time,
    aisId       as ais_id,
    map[
        'imo'                    ,cast(imo as varchar)             ,
    'shipName'               ,shipName                         ,
    'longitude'              ,cast(longitude as string)        ,
    'latitude'               ,cast(latitude as string)         ,
    'courseOverGround'       ,cast(courseOverGround as string) ,
    'speedOverGround'        ,cast(speedOverGround as string)  ,
    'trueHeading'            ,cast(trueHeading as string)      ,
    'navigationStatus'       ,navigationStatus                 ,
    'cycleSpeed'             ,cycleSpeed                       ,
    'callSign'               ,callSign                         ,
    'shipType'               ,shipType                         ,
    'shipDraft'              ,cast(shipDraft as string)        ,
    'shipLength'             ,cast(shipLength as string)       ,
    'shipWidth'              ,cast(shipWidth as string)        ,
    'dangerousGoods'         ,dangerousGoods                   ,
    'destination'            ,destination                      ,
    'nationality'            ,nationality                      ,
    'rateOfTurn'             ,cast(rateOfTurn as string)       ,
    'gmtCreateBy'            ,gmtCreateBy                      ,
    'longitudeAsStrin'       ,longitudeAsString                ,
    'latitudeAsString'       ,latitudeAsString
    ] as index_name_value
  from tmp_ja_target_ais_02;


-- 将map拼接数据进行展开
drop view if exists tmp_ja_target_ais_04;
create view tmp_ja_target_ais_04 as
select
    t1.mmsi         ,
    t1.message_type ,
    t1.receive_time ,
    t1.ais_id,
    t2.index_name    as index_name,
    t2.index_value   as index_value
from tmp_ja_target_ais_03 as t1
         cross join unnest (index_name_value) as t2 (
                                                     index_name,
                                                     index_value
    );


-- 对ais数据判断切分的MMSI是否是：970yyzzzz，代表落水人员
drop view if exists tmp_ja_target_ais_05;
create view tmp_ja_target_ais_05 as
select
    if(t2.name is null,'船员落水',concat(t2.name,'船员落水')) as eventName     , --  事件名称 必填
    aisId                            as deviceId                            , --  设备id  必填
    t2.name                          as deviceName                          , --  设备名称
    'AIS_BASE_STATION'               as deviceType                          , --  设备类型
    'crew_overboard'                 as eventType                           , --  事件类型,anomaly：异常物,dangerous：危险物 必填
    '1'                              as `level`                             , --  紧急等级 1、2、3 必填
    t1.receiveTime                   as eventTime                           , --  事件发生时间 必填
    t1.longitude                                                            , --  经度
    t1.latitude                                                             , --  纬度
    t1.mmsi                          as targetId                              -- mmsi或者是雷达目标id
from (
         select
             *
         from tmp_ja_target_ais_02
         where person_identification = '991'   -- 筛选人员设备落水的
           and messageType = '14'
     ) as t1
         left join ais_mob                   -- 人员基本信息表
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.person_no = t2.device_id;


-- ais的数据与对应关系表、黑白名单表、钻井平台表、进行关联，
-- 当船只为黑名单的时候，在防护区外进行告警，黑名单船只靠近
-- 当船只在雷达没有关联上的时候，进行正常的告警
drop view if exists tmp_ja_target_ais_06;
create view tmp_ja_target_ais_06 as
select
    t1.imo                                      , -- 船只唯一标识,可能为空
    t1.aisId                                    , -- 所属aisId
    t1.mmsi                                     , -- mmsi,唯一标识不为空
    t1.shipName                                 , -- 船只名称
    t1.longitude                                , -- 经度
    t1.latitude                                 , -- 纬度
    t1.receiveTime                              , -- 接收时间
    t1.site_distance                            , -- 钻井平台距离
    t1.site1_radius                             , -- 钻井平台一级防护区半径
    t1.site2_radius                             , -- 钻井平台二级防护区半径
    t1.site3_radius                             , -- 钻井平台三级防护区半径
    t1.warning_area_range                       , -- 钻井平台警戒区范围
    t1.warning_area_radius                      , -- 钻井平台警戒区半径
    t2.target_id                                , -- 雷达id
    t2.mmsi as mmsi_relation                    , -- 对应关系的mmsi
    t2.latest_time                              , -- 最晚时间
    t3.type                                     , -- 黑白名单类型
    t3.name                                     , -- 船只的英文名称
    t3.chinese_name                               -- 船只的中文名称
from (
         select
             *
         from tmp_ja_target_ais_02
         where messageType <> '14'
           and longitude is not null
           and latitude is not null
     ) as t1
         left join target_ship_relation_ais_info_source     -- 雷达ais对应关系表，主键mmsi的
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.mmsi = t2.mmsi
-- and abs(timestampdiff(day,t1.receiveTime,t2.latest_time)) = 0
         left join black_white_list                          -- 黑白名单信息表
    FOR SYSTEM_TIME AS OF t1.proctime as t3             -- 没有和雷达关联上的数据由ais进行告警
                   on t1.mmsi = t3.mmsi;

-- where (t1.receiveTime > timestampadd(minute,1,t2.latest_time) and t2.target_id is not null)
--   or t2.target_id is null;
-- where abs(timestampdiff(minute,t1.receiveTime,t2.latest_time)) >= 1


-- 筛选出除了黑白名单数据外的没有和雷达关联上的数据进行告警
drop view if exists tmp_ja_target_ais_07;
create view tmp_ja_target_ais_07 as
select
    case when site_distance > site3_radius and site_distance <= warning_area_radius then '异常船舶靠近'
         when site_distance > site2_radius and site_distance <= site3_radius then '船舶闯入三级防护区'
         when site_distance > site1_radius and site_distance <= site2_radius then '船舶闯入二级防护区'
         when site_distance <= site1_radius then '船舶闯入一级防护区'
        end                                                       as eventName,
    aisId                                                     as deviceId, -- 唯一标识不为空
    cast(null as varchar)                                     as deviceName,
    'AIS_BASE_STATION'                                        as deviceType, --  设备类型
    case when site_distance > site3_radius and site_distance <= warning_area_radius then 'unusual_ship_near'
         when site_distance <= site3_radius then 'ship_breaks_into_protected_area'
        end                                                       as eventType, -- 事件类型
    case when site_distance > site3_radius and site_distance <= warning_area_radius then '4'
         when site_distance > site2_radius and site_distance <= site3_radius then '3'
         when site_distance > site1_radius and site_distance <= site2_radius then '2'
         when site_distance <= site1_radius then '1'
        end                                                       as `level`, -- 防护区等级
    receiveTime                                               as eventTime, -- 事件时间
    longitude                                                 , --  经度
    latitude                                                  , --  纬度
    mmsi                                                      as targetId,
    cast(null as varchar)                                     as radarTargetId
from tmp_ja_target_ais_06
where site_distance <= warning_area_radius
  and (target_id is null or receiveTime > timestampadd(minute,1,latest_time))
  and (type is null or type <> 'WHITE');


-----------------------

-- 数据插入

-----------------------

begin statement set;

-- ais船只全量数据表，存储的是ais的自己单独的轨迹数据
insert into ais_target_ship_all_info
select
    mmsi                              ,
    receiveTime                       as receive_time    ,
    imo                               ,
    aisId                             as ais_id   ,
    shipName                          as ship_name  ,
    nationality                       ,
    longitude                         ,
    latitude                          ,
    courseOverGround                  as course_over_ground  ,
    speedOverGround                   as speed_over_ground  ,
    trueHeading                       as true_heading   ,
    navigationStatus                  as navigation_status,
    cycleSpeed                        as cycle_speed   ,
    callSign                          as call_sign,
    shipType                          as ship_type,
    shipDraft                         as ship_draft  ,
    shipLength                        as ship_length   ,
    shipWidth                         as ship_width     ,
    dangerousGoods                    as dangerous_goods ,
    destination,
    rateOfTurn                        as rate_of_turn  ,
    messageType                       as message_type,
    longitudeAsString                 as longitude_as_string,
    latitudeAsString                  as latitude_as_string,
    site_distance                     as site_distance,
    gmtCreateBy                       as gmt_create_by,
    from_unixtime(unix_timestamp())   as update_time
from tmp_ja_target_ais_02;



-- ais船只状态数据表，存储的是每个mmsi最新的一条数据，但是是一个纵表
insert into ais_target_ship_info
select
    mmsi               ,
    ais_id             ,
    index_name         ,
    receive_time       ,
    message_type       ,
    index_value        ,
    'ja-flink' as gmt_create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_ja_target_ais_04
where (index_name not in('longitude','latitude')
    and index_value is not null
    and index_value <> '')
   or
    (index_name in('latitude','longitude')
        and index_value >'0');


-- 雷达的全量数据表，两个雷达的数据全部进入
insert into radar_target_ship_all_info
select
    radar_id        ,
    target_id       ,
    receive_time    ,
    distance        ,
    angle           ,
    longitude       ,
    latitude        ,
    site_distance   ,
    gmt_create_by   ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_ja_target_radar_01;



-- radar雷达全部未融合状态数据信息入库doris
insert into radar_target_ship_info
select
    radar_id,
    target_id,
    receive_time,
    distance,
    angle,
    longitude,
    latitude,
    gmt_create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_ja_target_radar_01;



-- 雷达融合目标的状态数据信息表，融合不上就是各自雷达的目标id，融合上就随便取了coalesce(target_id1,target_id2)
insert into merge_radar_target_ship_info
select
    target_id,
    radar_id,
    receive_time as receive_time,
    distance,
    angle,
    longitude,
    latitude,
    gmt_create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_ja_target_radar_04;



-- 雷达融合目标对应关系数据,两个雷达数据的融合对应关系
insert into merge_radar_target_ship_relation_info
select
    max(target_id) as target_id,
    target_id1,
    target_id2,
    min(radar_id1) as radar_id1,
    min(radar_id2) as radar_id2,
    min(receive_time) as earliest_time,
    max(receive_time) as latest_time,
    count(1) as cnt   ,
    max(gmt_create_by) as gmt_create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_ja_target_radar_04
where target_id1 is not null
  and target_id2 is not null
group by target_id1,target_id2;


-- ais、radar融合全量数据表（融合轨迹表）
-- 对于雷达：融合上id就是mmsi，融合不上就是雷达目标id，类型都是radar,多带了个distance字段，为了再次读取数据处理的
-- 对于ais，全部数据入库，类型是ais
insert into merge_target_ship_all_info
select
    coalesce(mmsi,target_id) as id                , -- 船只唯一标识，mmsi或是target_id
    'radar' as type                               , -- 字段类型，雷达 or ais
    receive_time as receive_time                  , -- 接收时间
    longitude                                     , -- 雷达数据的经度
    latitude                                      , -- 雷达数据的纬度
    distance                                      , -- 距离
    gmt_create_by                                 , -- 创建人
    from_unixtime(unix_timestamp()) as update_time
from tmp_ja_target_radar_ais_merge_07;


-- ais的全数据入库doris
insert into merge_target_ship_all_info
select
    mmsi                              , -- 船只mmsi
    'ais' as type                     , -- 类型
    receiveTime as receive_time       , -- 接收时间
    longitude                         , -- 经度
    latitude                          , -- 纬度
    cast(null as double) as distance  , -- 距离
    gmtCreateBy as gmt_create_by      , -- 创建人
    from_unixtime(unix_timestamp()) as update_time
from tmp_ja_target_ais_02;



-- ais、radar融合的实时位置状态表，融合目标的状态数据（融合状态表）
insert into merge_target_ship_info
select
    coalesce(mmsi,target_id) as id    , -- 船只唯一标识，mmsi或是target_id
    'radar' as type                   , -- 字段类型，雷达 or ais
    receive_time as receive_time      , -- 接收时间
    longitude                         , -- 雷达数据的经度
    latitude                          , -- 雷达数据的纬度
    ship_name                         , -- 船名称
    ais_id                            , -- ais基站id
    distance                          , -- 距离
    angle                             , -- 角度
    gmt_create_by                     , -- 创建人
    from_unixtime(unix_timestamp()) as update_time
from tmp_ja_target_radar_ais_merge_07;


-- ais的全量实时位置数据入库doris（融合状态表）
insert into merge_target_ship_info
select
    mmsi                 as id   ,
    'ais'                as type,
    receiveTime          as receive_time,
    longitude,
    latitude,
    shipName             as ship_name,
    aisId                as ais_id,
    cast(null as double) as distance,
    cast(null as double) as angle,
    gmtCreateBy as gmt_create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_ja_target_ais_02
where longitude is not null
  and latitude is not null;



-- 雷达数据进入数据的最小时间（延迟告警）雷达数据进去的时间，为了控制进去不告警，等一会ais数据融合不上在告警，融合上就是ais告警
insert into radar_target_ship_entry_info
select
    target_id,
    min(receive_time) as earliest_time,
    max(receive_time) as latest_time,
    count(1) as cnt,
    from_unixtime(unix_timestamp()) as update_time
    -- to_timestamp(convert_tz(min(receive_time),'UTC','Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss') as earliest_time,
    -- to_timestamp(convert_tz(max(receive_time),'UTC','Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss') as latest_time,
    -- to_timestamp(min(receive_time),'yyyy-MM-dd HH:mm:ss') as earliest_time,
    -- to_timestamp(max(receive_time),'yyyy-MM-dd HH:mm:ss') as latest_time,
from tmp_ja_target_radar_04
     -- where site_distance <= 4000
group by target_id;


-- ais、radar融合对应关系表，两种数据融合的最早时间最晚时间，主键是mmsi
insert into target_ship_relation_ais_info
select
    mmsi 			               , -- 船只唯一标识
    target_id 		               , -- 雷达目标id
    earliest_time as earliest_time , -- 最早融合时间
    latest_time as latest_time     , -- 最晚融合时间
    from_unixtime(unix_timestamp()) as update_time
    -- to_timestamp(convert_tz(earliest_time,'UTC','Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss') as earliest_time,
    -- to_timestamp(convert_tz(latest_time,'UTC','Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss') as latest_time,
from tmp_ja_target_radar_ais_merge_06
where mmsi is not null;


-- ais、radar融合对应关系表，还是融合表，主键是雷达目标id
insert into target_ship_relation_info
select
    target_id 		               , -- 雷达目标id
    mmsi 			               , -- 船只唯一标识
    earliest_time as earliest_time , -- 最早融合时间
    latest_time as latest_time     , -- 最晚融合时间
    cnt                            , -- 数量
    ship_name                      , -- 船只名称
    ais_id                         , -- ais基站id
    'ja-flink' as gmt_create_by,
    from_unixtime(unix_timestamp()) as update_time
    -- to_timestamp(convert_tz(earliest_time,'UTC','Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss') as earliest_time,
    -- to_timestamp(convert_tz(latest_time,'UTC','Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss') as latest_time,
from tmp_ja_target_radar_ais_merge_06
where mmsi is not null;



-- 告警4步骤
--  1. ais落水人员告警
--  2. 船舶进入防护区雷达告警为主（延迟）-  未知船舶、和黑名单船舶
--  3. ais的在防护区内没有和雷达融合上的单独进行ais告警
--  4. 黑名单船舶在防护区外告警（ais范围比雷达大，黑名单船舶是检测到就告警）


-- ais落水人员告警入kafka
insert into event_warn_kafka
select
    uuid()                         as eventId               , -- 唯一编号 必填
    uuid()                         as eventNo               , -- 事件编号 必填
    eventName                                               , -- 事件名称 必填
    deviceId                                                , -- 设备id  必填
    deviceName                                              , -- 设备名称
    deviceType                                              , -- 设备类型
    eventType                                               , -- 事件类型,anomaly：异常物,dangerous：危险物 必填
    `level`                                                 , -- 紧急等级 1、2、3 必填
    eventTime                                               , -- 事件发生时间 必填
    longitude                                               , -- 经度
    latitude                                                , -- 纬度
    targetId                                                , -- mmsi或者是雷达目标的id
    cast(null as varchar)            as radarTargetId        -- 雷达的target_id
    --    cast(null as varchar)            as source              , -- 来源
    --    cast(null as bigint)             as leftTopX            , -- 左上角x坐标
    --    cast(null as bigint)             as leftTopY            , -- 左上角Y坐标
    --    cast(null as bigint)             as bboxWidth           , -- 目标宽度
    --    cast(null as bigint)             as bboxHeight          , -- 目标高度
    --    cast(null as bigint)             as sourceFrameWidth    , -- 原始帧宽度
    -- cast(null as bigint)             as sourceFrameHeight   , -- 原始帧高度
    --    cast(null as bigint)             as positionId          , -- 点位id（无人机、机器狗告警时有）
    --    cast(null as double)             as confidence          , -- 置信度
    --    cast(null as double)             as pointX              , -- x轴
    --  	cast(null as double)             as pointY              , -- y轴
    -- cast(null as double)             as pointZ              , -- z轴
    --    cast(null as varchar)            as itemName            , -- 危险物名称,危险物告警时传
    --    cast(null as varchar)            as image               , -- 异常物的小图uri地址
    --    cast(null as varchar)            as sourceImage         , -- 异常物的大图uri地址
    --    cast(null as varchar)            as spaceType           , -- 空间类型
    --  	cast(null as varchar)            as spaceId             , -- 空间Id
    -- cast(null as varchar)            as reason                -- 事件发生原因
from tmp_ja_target_ais_05;



-- 雷达监测船只闯入防护区和在防护区外告警数据入kafka
insert into event_warn_kafka
select
    uuid()                           as eventId                   , -- 唯一编号 必填
    uuid()                           as eventNo                   , -- 事件编号 必填
    eventName                                                     , -- 事件名称
    deviceId                                                      , -- 设备id  必填
    deviceName                                                    , -- 设备名称
    deviceType                                                    , -- 设备类型
    eventType                                                     , -- 事件类型
    `level`                                                       , -- 防护区等级
    eventTime                                                     , -- 事件时间
    longitude                        as longitude                 , -- 经度
    latitude                         as latitude                  , -- 纬度
    targetId                                                      , -- mmsi或者是雷达目标id
    radarTargetId                                                  -- 雷达的target_id
    --    cast(null as varchar)            as source                    , -- 空
    --    cast(null as bigint)             as leftTopX                  , --  左上角x坐标
    --    cast(null as bigint)             as leftTopY                  , --  左上角Y坐标
    --    cast(null as bigint)             as bboxWidth                 , --  目标宽度
    --    cast(null as bigint)             as bboxHeight                , --  目标高度
    --    cast(null as bigint)             as sourceFrameWidth          , --  原始帧宽度
    -- cast(null as bigint)             as sourceFrameHeight         , --  原始帧高度
    --    cast(null as bigint)             as positionId                , --  点位id（无人机、机器狗告警时有）
    --    cast(null as double)             as confidence                , --  置信度
    --    cast(null as double)             as pointX                    , --  x轴
    --  	cast(null as double)             as pointY                    , --  y轴
    -- cast(null as double)             as pointZ                    , --  z轴
    --    cast(null as varchar)            as itemName                  , --  危险物名称,危险物告警时传
    --    cast(null as varchar)            as image                     , --  异常物的小图uri地址
    --    cast(null as varchar)            as sourceImage               , --  异常物的大图uri地址
    --    cast(null as varchar)            as spaceType                 , --  空间类型
    --  	cast(null as varchar)            as spaceId                   , --  空间Id
    -- cast(null as varchar)            as reason                      --  事件发生原因
from tmp_ja_target_radar_ais_merge_09;




-- ais监测数据没有和雷达关联上的数据入kafka
insert into event_warn_kafka
select
    uuid()                           as eventId                   , -- 唯一编号 必填
    uuid()                           as eventNo                   , -- 事件编号 必填
    eventName                                                     , -- 事件名称
    deviceId                                                      , -- 设备id  必填
    deviceName                                                    , -- 设备名称
    deviceType                                                    , -- 设备类型
    eventType                                                     , -- 事件类型
    `level`                                                       , -- 防护区等级
    eventTime                                                     , -- 事件时间
    longitude                        as longitude                 , -- 经度
    latitude                         as latitude                  , -- 纬度
    targetId                                                      , -- ais的基站id
    radarTargetId                                                  -- 雷达的target——id
    --    cast(null as varchar)            as source                    , -- 空
    --    cast(null as bigint)             as leftTopX                  , --  左上角x坐标
    --    cast(null as bigint)             as leftTopY                  , --  左上角Y坐标
    --    cast(null as bigint)             as bboxWidth                 , --  目标宽度
    --    cast(null as bigint)             as bboxHeight                , --  目标高度
    --    cast(null as bigint)             as sourceFrameWidth          , --  原始帧宽度
    -- cast(null as bigint)             as sourceFrameHeight         , --  原始帧高度
    --    cast(null as bigint)             as positionId                , --  点位id（无人机、机器狗告警时有）
    --    cast(null as double)             as confidence                , --  置信度
    --    cast(null as double)             as pointX                    , --  x轴
    --  	cast(null as double)             as pointY                    , --  y轴
    -- cast(null as double)             as pointZ                    , --  z轴
    --    cast(null as varchar)            as itemName                  , --  危险物名称,危险物告警时传
    --    cast(null as varchar)            as image                     , --  异常物的小图uri地址
    --    cast(null as varchar)            as sourceImage               , --  异常物的大图uri地址
    --    cast(null as varchar)            as spaceType                 , --  空间类型
    --  	cast(null as varchar)            as spaceId                   , --  空间Id
    -- cast(null as varchar)            as reason                      --  事件发生原因
from tmp_ja_target_ais_07;



-- ais监测黑名单船只在防护区外告警数据入kafka
insert into event_warn_kafka
select
    uuid()                           as eventId                  , -- 唯一编号 必填
    uuid()                           as eventNo                  , -- 事件编号 必填
    '黑名单船舶靠近'                   as eventName                 , -- 事件名称 必填
    aisId                            as deviceId                  , -- 设备id  必填
    cast(null as varchar)            as deviceName  , -- 设备名称
    'AIS_BASE_STATION'               as deviceType                , -- 设备类型
    'blacklist_ship_stalled'         as  eventType                , -- 事件类型,anomaly：异常物,dangerous：危险物 必填
    '4'                              as `level`                   , -- 紧急等级 1、2、3 必填
    receiveTime                      as eventTime                 , -- 事件发生时间 必填
    longitude                                                     , -- 经度
    latitude                                                      , -- 纬度
    mmsi                             as targetId                  , -- 来源，ais的id
    cast(null as varchar)            as radarTargetId              -- 雷达的target_id
    --    cast(null as varchar)            as source                    , -- 空
    --    cast(null as bigint)             as leftTopX                  , -- 左上角x坐标
    --    cast(null as bigint)             as leftTopY                  , -- 左上角Y坐标
    --    cast(null as bigint)             as bboxWidth                 , -- 目标宽度
    --    cast(null as bigint)             as bboxHeight                , -- 目标高度
    --    cast(null as bigint)             as sourceFrameWidth          , -- 原始帧宽度
    -- cast(null as bigint)             as sourceFrameHeight         , -- 原始帧高度
    --    cast(null as bigint)             as positionId                , -- 点位id（无人机、机器狗告警时有）
    --    cast(null as double)             as confidence                , -- 置信度
    --    cast(null as double)             as pointX                    , -- x轴
    --  	cast(null as double)             as pointY                    , -- y轴
    -- cast(null as double)             as pointZ                    , -- z轴
    --    cast(null as varchar)            as itemName                  , -- 危险物名称,危险物告警时传
    --    cast(null as varchar)            as image                     , -- 异常物的小图uri地址
    --    cast(null as varchar)            as sourceImage               , -- 异常物的大图uri地址
    --    cast(null as varchar)            as spaceType                 , -- 空间类型
    --  	cast(null as varchar)            as spaceId                   , -- 空间Id
    -- cast(null as varchar)            as reason                      -- 事件发生原因
from tmp_ja_target_ais_06
where type = 'BLACK'
  and site_distance > warning_area_radius;




-- -- ais监测人员落水数据入doris - 可以不入doris表
-- insert into event_alarm_info_doris
-- select
--     targetId         as id                  , -- 来源，ais的id
--     eventTime        as event_time          , -- 事件发生时间 必填
--     deviceId         as device_id           , -- 设备id  必填
--     eventName        as event_name          , -- 事件名称 必填
--     deviceName       as device_name         , -- 设备名称
--     deviceType       as device_type         , -- 设备类型
--     eventType        as event_type          , -- 事件类型,anomaly：异常物,dangerous：危险物 必填
--     `level`                                 , -- 紧急等级 1、2、3 必填
--     longitude                               , -- 经度
--     latitude                                , -- 纬度
--     from_unixtime(unix_timestamp()) as update_time
--   from tmp_ja_target_ais_05;



-- -- 雷达监测船只闯入防护区和在防护区外告警数据入doris
-- insert into event_alarm_info_doris
-- select
--     targetId    as id                        , -- mmsi或者雷达目标id
--     eventTime   as event_time                , -- 事件时间
--     deviceId    as device_id                 , -- 设备id  必填
--     eventName   as event_name                , -- 事件名称
--     deviceName  as device_name               , -- 设备名称
--     deviceType  as device_type               , -- 设备类型
--     eventType   as event_type                , -- 事件类型
--     `level`                                  , -- 防护区等级
--     longitude                                , -- 经度
--     latitude                                 , -- 纬度
--     from_unixtime(unix_timestamp()) as update_time
--   from tmp_ja_target_radar_ais_merge_09;


-- -- ais监测数据没有和雷达关联上的数据如doris
-- insert into event_alarm_info_doris
-- select
--     targetId    as id                         , -- mmsi或者雷达目标id
--     eventTime   as event_time                 , -- 事件时间
--     deviceId    as device_id                  , -- 设备id  必填
--     eventName   as event_name                 , -- 事件名称
--     deviceName  as device_name                , -- 设备名称
--     deviceType  as device_type                , -- 设备类型
--     eventType   as event_type                 , -- 事件类型
--     `level`                                   , -- 防护区等级
--     longitude                                 , -- 经度
--     latitude                                  , -- 纬度
--     from_unixtime(unix_timestamp()) as update_time
--   from tmp_ja_target_ais_07;



-- -- ais监测黑名单船只闯入防护区和在防护区外告警数据入doris
-- insert into event_alarm_info_doris
-- select
--     mmsi                             as id                         , -- mmsi或则雷达的目标id
--     receiveTime                      as event_time                 , -- 事件发生时间 必填
--     aisId                            as device_id                  , -- 设备id  必填
--     '黑名单船舶靠近'                   as event_name                 , -- 事件名称 必填
--     cast(null as varchar)            as device_name  , -- 设备名称
--     'AIS_BASE_STATION'               as device_type                , -- 设备类型
--     'blacklist_ship_stalled'         as  event_type                , -- 事件类型,anomaly：异常物,dangerous：危险物 必填
--     '4'                              as `level`                    , -- 紧急等级 1、2、3 必填
--     longitude                                                      , -- 经度
--     latitude                                                       , -- 纬度
--     from_unixtime(unix_timestamp()) as update_time                  -- 数据入库时间
--   from tmp_ja_target_ais_06
--   where type = 'BLACK'
--     and site_distance > warning_area_radius;


end;
