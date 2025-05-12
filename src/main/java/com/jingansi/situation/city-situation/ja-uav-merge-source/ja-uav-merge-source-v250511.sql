--********************************************************************--
-- author:      write your name here
-- create time: 2025/5/11 17:38:47
-- description: rid、aoa、雷达融合数据入库
-- version: ja-uav-merge-source-v250511
--********************************************************************--
set 'pipeline.name' = 'ja-uav-merge-source';


SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '60000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '3';
set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';

SET 'execution.checkpointing.interval' = '120000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-uav-merge-source';


 -----------------------

 -- 数据结构来源

 -----------------------

-- 设备检测数据上报 （Source：kafka）
create table uav_merge_target_kafka (
                                        id                            string  comment 'id',
                                        device_id                     string  comment '数据来源的设备id',
                                        acquire_time          		string  comment '采集时间',
                                        src_code              		string  comment '自己本身数据类型 RID、AOA、RADAR',
                                        src_pk                        string  comment '自己网站的目标id',
                                        device_name                   string  comment '设备名称',
                                        rid_devid             		string  comment 'rid设备的id-飞机上报的',
                                        msgtype               		bigint  comment '消息类型',
                                        recvtype              		string  comment '无人机数据类型,示例:2.4G',
                                        recvmac                       string  comment '无人机的mac地址',
                                        mac                   		string  comment 'rid设备MAC地址',
                                        rssi                  		bigint  comment '信号强度',
                                        longitude             		double  comment '探测到的无人机经度',
                                        latitude              		double  comment '探测到的无人机纬度',
                                        location_alit         		double  comment '气压高度',
                                        ew                    		double  comment 'rid航迹角,aoa监测站识别的目标方向角',
                                        speed_h               		double  comment '水平速度',
                                        speed_v               		double  comment '垂直速度',
                                        height                		double  comment '距地高度',
                                        height_type           		double  comment '高度类型',
                                        control_station_longitude  	double  comment '控制无人机人员经度',
                                        control_station_latitude   	double  comment '控制无人机人员纬度',
                                        control_station_height 	   	double 	comment '控制站高度',
                                        target_name             		string  comment 'aoa-目标名称',
                                        altitude                		double  comment 'aoa-无人机所在海拔高度',
                                        distance_from_station   		double  comment 'aoa-无人机距离监测站的距离',
                                        speed_ms                  	double  comment 'aoa-无人机飞行速度 (m/s)',
                                        target_frequency_khz    		double  comment 'aoa-目标使用频率 (k_hz)',
                                        target_bandwidth_khz		    double  comment 'aoa-目标带宽 (k_hz)',
                                        target_signal_strength_db		double  comment 'aoa-目标信号强度 (d_b)'
) WITH (
      'connector' = 'kafka',
      'topic' = 'uav_merge_target',
      -- 'properties.bootstrap.servers' = '135.100.11.110:30090',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.group.id' = 'uav_merge_target2',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',  -- 1745564415000
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- ********************************* doris写入的表 ********************************************

-- 融合全量表
create table dwd_bhv_merge_target_rt (
                                         uav_id                         string  comment '无人机的id-sn号',
                                         control_station_id             string  comment '控制站的id',
                                         device_id                      string  comment 'RID、AOA设备id',
                                         acquire_time                   string  comment '采集时间',
                                         src_code                       string  comment '自己本身数据类型 RID、AOA RADAR',
                                         src_pk                	     string  comment '数据自身的id',
                                         merge_type                     string  comment '融合的设备类型,逗号分隔,示例：RID,AOA,RADAR',
                                         merge_cnt                      bigint  comment '融合的设备类型数量,示例：1',
                                         merge_target_cnt               bigint  comment '融合的目标id数量,示例，100个RID目标融合为100',
                                         device_name                    string  comment '设备名称',
                                         rid_devid                      string  comment 'rid设备的id-飞机上报的',
                                         msgtype                        bigint  comment '消息类型',
                                         recvtype                       string  comment '无人机数据类型,示例:2.4G',
                                         recvmac                        string  comment '无人机的mac地址',
                                         mac                            string  comment 'rid设备MAC地址',
                                         rssi                           bigint  comment '信号强度',
                                         longitude                      double  comment 'rid-探测到的无人机经度',
                                         latitude                       double  comment 'rid-探测到的无人机纬度',
                                         location_alit                  double  comment '气压高度',
                                         ew                             double  comment '航迹角',
                                         speed_h                        double  comment '无人机地速-水平速度',
                                         speed_v                        double  comment '垂直速度',
                                         height                         double  comment '无人机距地高度',
                                         height_type                    double  comment '高度类型',
                                         control_station_longitude      double  comment '控制无人机人员经度',
                                         control_station_latitude       double  comment '控制无人机人员纬度',
                                         control_station_height 	     double  comment '控制站高度',
                                         target_name             	     string  comment 'aoa-目标名称',
                                         altitude                	     double  comment 'aoa-无人机所在海拔高度',
                                         distance_from_station   	     double  comment 'aoa-无人机距离监测站的距离',
                                         speed_ms                       double  comment 'aoa-无人机飞行速度 (m/s)',
                                         target_frequency_khz    	     double  comment 'aoa-目标使用频率 (k_hz)',
                                         target_bandwidth_khz		     double  comment 'aoa-目标带宽 (k_hz)',
                                         target_signal_strength_db	     double  comment 'aoa-目标信号强度 (d_b)',
                                         filter_col                     string  comment '动态筛选字段拼接',
                                         update_time                    string  comment '更新时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dwd_bhv_merge_target_rt',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 融合状态表
create table dws_bhv_merge_target_last_location_rt (
                                                       uav_id                         string  comment '无人机的id-sn号',
                                                       control_station_id             string  comment '控制站的id',
                                                       device_id                      string  comment 'RID、AOA设备id',
                                                       acquire_time                   string  comment '采集时间',
                                                       src_code                       string  comment '自己本身数据类型 RID、AOA RADAR',
                                                       src_pk                	     string  comment '数据自身的id',
                                                       merge_type                     string  comment '融合的设备类型,逗号分隔,示例：RID,AOA,RADAR',
                                                       merge_cnt                      bigint  comment '融合的设备类型数量,示例：1',
                                                       merge_target_cnt               bigint  comment '融合的目标id数量,示例，100个RID目标融合为100',
                                                       device_name                    string  comment '设备名称',
                                                       rid_devid                      string  comment 'rid设备的id-飞机上报的',
                                                       msgtype                        bigint  comment '消息类型',
                                                       recvtype                       string  comment '无人机数据类型,示例:2.4G',
                                                       recvmac                        string  comment '无人机的mac地址',
                                                       mac                            string  comment 'rid设备MAC地址',
                                                       rssi                           bigint  comment '信号强度',
                                                       longitude                      double  comment 'rid-探测到的无人机经度',
                                                       latitude                       double  comment 'rid-探测到的无人机纬度',
                                                       location_alit                  double  comment '气压高度',
                                                       ew                             double  comment '航迹角',
                                                       speed_h                        double  comment '无人机地速-水平速度',
                                                       speed_v                        double  comment '垂直速度',
                                                       height                         double  comment '无人机距地高度',
                                                       height_type                    double  comment '高度类型',
                                                       control_station_longitude      double  comment '控制无人机人员经度',
                                                       control_station_latitude       double  comment '控制无人机人员纬度',
                                                       control_station_height 	     double  comment '控制站高度',
                                                       target_name             	     string  comment 'aoa-目标名称',
                                                       altitude                	     double  comment 'aoa-无人机所在海拔高度',
                                                       distance_from_station   	     double  comment 'aoa-无人机距离监测站的距离',
                                                       speed_ms                       double  comment 'aoa-无人机飞行速度 (m/s)',
                                                       target_frequency_khz    	     double  comment 'aoa-目标使用频率 (k_hz)',
                                                       target_bandwidth_khz		     double  comment 'aoa-目标带宽 (k_hz)',
                                                       target_signal_strength_db	     double  comment 'aoa-目标信号强度 (d_b)',
                                                       filter_col                     string  comment '动态筛选字段拼接',
                                                       update_time                    string  comment '更新时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_bhv_merge_target_last_location_rt',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 控制站实体表
create table `dws_et_control_station_info` (
                                               id              string  comment '控制站id',
                                               acquire_time    string  comment '采集时间',
                                               name            string  comment '控制站名称',
                                               register_uav    string  comment '登记无人机-序列号（产品型号）',
                                               source          string  comment '数据来源',
                                               search_content  string  comment '倒排索引数据',
                                               update_time     string  comment '数据入库时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_et_control_station_info',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='5s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 无人机实体表
create table `dws_et_uav_info` (
                                   id                      string  comment '无人机id-sn号',
                                   sn                      string  comment '序列号',
                                   name                    string  comment '无人机名称',
                                   device_id               string  comment '无人机的设备id-牍术介入的',
                                   recvmac                 string  comment 'MAC地址',
                                   manufacturer            string  comment '厂商',
                                   model                   string  comment '型号',
                                   owner                   string  comment '所有者',
                                   type                    string  comment '类型',
                                   source                  string  comment '数据来源',
    -- category                string  comment '类别',
    -- phone                   string  comment '电话',
    -- empty_weight            string  comment '空机重量',
    -- maximum_takeoff_weight  string  comment '最大起飞重量',
    -- purpose                 string  comment '用途',
                                   search_content          string  comment '倒排索引数据',
                                   update_time             string  comment '数据入库时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_et_uav_info',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='5s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- rid、控制站、无人机关系表
create table `dws_rl_rid_uav_rt` (
                                     uav_id               string  comment '无人机的id-sn号',
                                     control_station_id   string  comment '控制站id',
                                     device_id   	       string  comment 'RID的设备id-牍术接入的',
                                     rid_devid       	   string  comment 'rid设备的id-飞机上报的',
                                     acquire_time         string  comment '采集时间',
                                     update_time          string  comment '更新时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_rl_rid_uav_rt',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='5s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- ********************************** doris数据表读取 ***********************************

-- 无人机实体表来源
create table `dws_et_uav_info_source` (
                                          id                      string  comment '无人机id-sn号',
                                          sn                      string  comment '序列号',
                                          name                    string  comment '无人机名称',
                                          recvmac                 string  comment 'MAC地址',
                                          manufacturer            string  comment '厂商',
                                          model                   string  comment '型号',
                                          owner                   string  comment '所有者',
                                          type                    string  comment '类型',
                                          source                  string  comment '数据来源',
                                          category                string  comment '类别',
                                          phone                   string  comment '电话',
                                          empty_weight            string  comment '空机重量',
                                          maximum_takeoff_weight  string  comment '最大起飞重量',
                                          purpose                 string  comment '用途',
                                          PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_uav_info',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- 无人机设备表（Sink：mysql）
create table device (
                        id                  int,
                        device_id	          string, -- 设备id
                        name	              string, -- 设备名称
                        manufacturer	      string, -- 设备厂商
                        model	              string, -- 设备型号
                        type	              string, -- 设备类型
                        owner	              string, -- 所属人
                        sn	              string, -- 唯一序列号
                        status              string, -- 在线状态
                        longitude           decimal(12,8), -- 经度
                        latitude            decimal(12,8), -- 纬度
                        PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://135.100.11.110:31306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      -- 'url' = 'jdbc:mysql://172.21.30.105:31306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- ****************************规则引擎写入数据******************************** --
create table uav_source(
                           id                    string, -- id
                           name                  string, -- 名称
                           type                  string, -- 类型
                           manufacturer          string, -- 厂商
                           model                 string, -- 型号
    -- category              string, -- 类别
    -- purpose               string, -- 用途
    -- emptyWeight           double, -- 空机重量
    -- maximumTakeoffWeight  double, -- 最大起飞重量

                           ew                    double, -- 航迹角
                           height                double, -- 无人机距地高度
                           locationAlit          double, -- 气压高度
                           speedV                double, -- 垂直速度
                           speedH                double, -- 无人机地速,水平速度
                           recvtype              string, -- 无人机数据类型
                           rssi                  double, -- 信号强度
                           lng                   double, -- 经度
                           lat                   double, -- 维度
                           acquireTime           string, -- 采集时间
                           targetType            string, -- 实体类型 固定值 UAV
                           updateTime            string -- flink处理时间
) with (
      'connector' = 'kafka',
      'topic' = 'uav_source',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'uav_source1',
      'key.format' = 'json',
      'key.fields' = 'id',
      'format' = 'json'
      );


-----------------------

-- 数据处理

-----------------------

-- 数据字段处理
create view temp01 as
select
    *,
    split_index(id,';',0) as uav_id,
    split_index(id,';',1) as merge_type,
    split_index(id,';',2) as merge_cnt,
    split_index(id,';',3) as merge_target_cnt,
    PROCTIME() as proctime
from uav_merge_target_kafka;



-- 数据union 整合字段，入库融合表
create view temp02 as
select
    t1.uav_id,
    concat('cs',t1.uav_id) as control_station_id,
    t1.device_id,
    acquire_time,
    src_code,
    src_pk,
    merge_type,
    cast(merge_cnt as bigint) as merge_cnt,
    cast(merge_target_cnt as bigint) as merge_target_cnt,
    device_name,
    rid_devid,
    msgtype,
    recvtype,
    t1.recvmac,
    mac,
    rssi,
    t1.longitude,
    t1.latitude,
    location_alit,
    ew,
    speed_h,
    speed_v,
    height,
    height_type,
    control_station_longitude,
    control_station_latitude,
    control_station_height,
    target_name,
    altitude,
    distance_from_station,
    speed_ms,
    target_frequency_khz,
    target_bandwidth_khz,
    target_signal_strength_db,

    t2.device_id              as join_dushu_uav_device_id,
    t2.name                   as join_dushu_uav_name,
    t2.manufacturer           as join_dushu_uav_manufacturer,
    t2.model                  as join_dushu_uav_model,
    t2.owner                  as join_dushu_uav_owner,
    t2.type                   as join_dushu_uav_type,
    t2.status                 as join_dushu_uav_status,

    t3.id                     as doris_uav_join_id,
    t3.name                   as doris_uav_join_name,
    t3.recvmac                as doris_uav_join_recvmac,
    t3.manufacturer           as doris_uav_join_manufacturer,
    t3.model                  as doris_uav_join_model,
    t3.owner                  as doris_uav_join_owner,
    t3.type                   as doris_uav_join_type,
    t3.category               as doris_uav_join_category,
    t3.phone                  as doris_uav_join_phone,
    t3.empty_weight           as doris_uav_join_empty_weight,
    t3.maximum_takeoff_weight as doris_uav_join_maximum_takeoff_weightn,
    t3.purpose                as doris_uav_join_purpose,
    concat(
            ifnull(cast(t1.longitude as varchar),''),'¥',
            ifnull(cast(t1.latitude as varchar),''),'¥',
            ifnull(cast(location_alit as varchar),''),'¥',
            ifnull(cast(ew as varchar),''),'¥',
            ifnull(cast(height as varchar),''),'¥',
            ifnull(cast(speed_v as varchar),''),'¥',
            ifnull(cast(speed_h as varchar),''),'¥',
            ifnull(recvtype,''),'¥',
            ifnull(cast(rssi as varchar),''),'¥',
            ifnull(t2.`status`,''),'¥',
            ifnull(cast(control_station_longitude as varchar),''),'¥',
            ifnull(cast(control_station_latitude as varchar),''),'¥',
            ifnull(rid_devid,''),'¥',
            ifnull(src_code,''),'¥',
            ifnull(merge_type,''),'¥',
            ifnull(merge_cnt,''),'¥',
            ifnull(merge_target_cnt,''),'¥',
            ifnull(target_name,''),'¥',
            ifnull(cast(altitude as varchar),''),'¥',
            ifnull(cast(distance_from_station as varchar),''),'¥',
            ifnull(cast(speed_ms as varchar),''),'¥',
            ifnull(cast(target_frequency_khz as varchar),''),'¥',
            ifnull(cast(target_bandwidth_khz as varchar),''),'¥',
            ifnull(cast(target_signal_strength_db as varchar),'')
        ) as filter_col

from temp01 as t1
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 设备表 关联无人机
                   on t1.uav_id = t2.sn
                       and 'UAV' = t2.type
         left join dws_et_uav_info_source FOR SYSTEM_TIME AS OF t1.proctime as t3   -- 设备表 关联无人机
                   on t1.uav_id = t3.id;


-----------------------

-- 数据写入

-----------------------


begin statement set;


-- rid、控制站、无人机关系表
insert into dws_rl_rid_uav_rt
select
    uav_id,
    control_station_id,
    device_id,
    rid_devid,
    acquire_time,
    from_unixtime(unix_timestamp()) as update_time
from temp02;


-- 控制站实体表
insert into dws_et_control_station_info
select
    control_station_id                as id,
    acquire_time,
    if(src_code in ('RID','AOA'),coalesce(target_name,uav_id),cast(null as varchar))     as name, -- 无人机id-sn号
    uav_id                            as register_uav,
    src_code                          as source,
    uav_id                            as search_content,
    from_unixtime(unix_timestamp())   as update_time
from temp02
where doris_uav_join_id is null
  and control_station_longitude is not null;


-- 无人机实体表
insert into dws_et_uav_info
select
    uav_id                       as id,
    if(src_code in ('RID','AOA'),uav_id,cast(null as varchar))   as sn,
    coalesce(join_dushu_uav_name,target_name,uav_id)    as name,
    join_dushu_uav_device_id as device_id,
    recvmac,
    join_dushu_uav_manufacturer  as manufacturer,
    join_dushu_uav_model         as model,
    join_dushu_uav_owner         as owner,
    join_dushu_uav_type          as type,
    src_code as source,
    -- cast(null as varchar)  as category,
    -- cast(null as varchar)  as phone,
    -- cast(null as varchar)  as type
    -- cast(null as varchar)  as empty_weight,
    -- cast(null as varchar)  as maximum_takeoff_weight,
    -- cast(null as varchar)  as purpose,
    concat(
            ifnull(coalesce(join_dushu_uav_name,target_name),''),' ',
            ifnull(uav_id,'')
        )         as search_content,
    from_unixtime(unix_timestamp()) as update_time
from temp02
where doris_uav_join_id is null;




-- 融合数据入库
insert into dwd_bhv_merge_target_rt
select
    uav_id                         ,
    control_station_id             ,
    device_id,
    acquire_time                   ,
    src_code                       ,
    src_pk                	     ,
    merge_type,
    merge_cnt,
    merge_target_cnt,
    device_name,
    rid_devid                      ,
    msgtype                        ,
    recvtype                       ,
    recvmac                        ,
    mac                            ,
    rssi                           ,
    longitude                      ,
    latitude                       ,
    location_alit                  ,
    ew                             ,
    speed_h                        ,
    speed_v                        ,
    height                         ,
    height_type                    ,
    control_station_longitude      ,
    control_station_latitude       ,
    control_station_height 	     ,
    target_name             	     ,
    altitude                	     ,
    distance_from_station   	     ,
    speed_ms                       ,
    target_frequency_khz    	     ,
    target_bandwidth_khz		     ,
    target_signal_strength_db	     ,
    filter_col                     ,
    from_unixtime(unix_timestamp()) as update_time
from temp02;


-- 融合状态数据入库
insert into dws_bhv_merge_target_last_location_rt
select
    uav_id                         ,
    control_station_id             ,
    device_id,
    acquire_time                   ,
    src_code                       ,
    src_pk                	     ,
    merge_type,
    merge_cnt,
    merge_target_cnt,
    device_name,
    rid_devid                      ,
    msgtype                        ,
    recvtype                       ,
    recvmac                        ,
    mac                            ,
    rssi                           ,
    longitude                      ,
    latitude                       ,
    location_alit                  ,
    ew                             ,
    speed_h                        ,
    speed_v                        ,
    height                         ,
    height_type                    ,
    control_station_longitude      ,
    control_station_latitude       ,
    control_station_height 	     ,
    target_name             	     ,
    altitude                	     ,
    distance_from_station   	     ,
    speed_ms                       ,
    target_frequency_khz    	     ,
    target_bandwidth_khz		     ,
    target_signal_strength_db	     ,
    filter_col                     ,
    from_unixtime(unix_timestamp()) as update_time
from temp02;



-- *********** 规则引擎数据 ***********
insert into uav_source
select
    uav_id                      as  id,
    join_dushu_uav_name         as name,
    join_dushu_uav_type         as type,
    join_dushu_uav_model        as model,
    join_dushu_uav_manufacturer as manufacturer,
    ew,
    height,
    location_alit   as locationAlit,
    speed_v         as speedV,
    speed_h         as speedH,
    recvtype,
    rssi,
    longitude       as lng,
    latitude        as lat,
    acquire_time    as acquireTime,
    'UAV'           as targetType,
    from_unixtime(unix_timestamp())  as updateTime
from temp02;


end;



