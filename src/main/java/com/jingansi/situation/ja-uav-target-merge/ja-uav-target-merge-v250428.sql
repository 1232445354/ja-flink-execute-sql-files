--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2025/04/23 16:48:50
-- description: rid设备数据采集数据,aoa采集数据，rid和aoa数据融合
-- version: ja-uav-target-merge-v250428
--********************************************************************--

set 'pipeline.name' = 'ja-uav-target-merge';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '60000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'parallelism.default' = '3';
set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';

SET 'execution.checkpointing.interval' = '120000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-uav-target-merge';


-- 计算距离
create function distance_udf as 'com.jingan.udf.geohash.DistanceUdf';

-- 获取无人机随机id
create function get_random_id as 'com.jingan.udf.random.RandomId';

-- udtf使用函数调用一次
create function PassThroughUdtf as 'com.jingan.udtf.PassThroughUdtf';

-----------------------

-- 数据结构来源

-----------------------

-- 设备检测数据上报 （Source：kafka）
create table iot_device_message_kafka_01 (
                                             type                 string,    -- 物模型的类型
                                             deviceId             string,    -- 设备id，rid设备id
                                             productKey           string,    -- 产品key
                                             message row<
                                                 bid                  string,
                                             tid                  string,
                                             version              string,
                                             `timestamp`          bigint,
                                             `method`             string,

                                             `data` row<
                                                 -- RID的数据字段
                                                 devid                string,  -- RID设备编号 设备的唯一标识
                                             msgtype              bigint,  -- 消息类型
                                             recvmac              string,  -- 采集到的无人机MAC地址
                                             recvtype             string,  -- 无人机数据类型
                                             mac                  string,  -- 设备MAC地址
                                             rssi                 bigint,  -- 信号强度
                                             latitude             double,  -- rid设备纬度
                                             longitude            double,  -- rid设备经度
                                             openAlarm            bigint,  -- 是否告警、开箱 1:true，0:false
                                             outpowerAlarm        bigint,  -- 是否有电量 1:true，0:false
                                             basic_info row<
                                                 basic_uaid           string, -- UAVID 无人机序列号
                                             basic_idtype         bigint,
                                             basic_uatype         bigint -- UA 类型     1：设备序列号 2：由 CAA 提供的 UAS 实名登记号 3：UTM 任务 ID （3-0 位，UA 类型）
                                                 >,
                                             location_info row<
                                                 location_speed_v     double, -- 垂直速度
                                             location_hori_acc    double, -- 水平精度
                                             location_vert_acc    double, -- 垂直精度
                                             location_speed_multi double, -- 保留字
                                             location_height_type double, -- 高度类型
                                             location_ew          double, -- 航迹角
                                             location_height      double, -- 无人机距地高度
                                             location_status      double, -- 保留字
                                             location_speed_acc   double, -- 速度精度
                                             location_speed_h     double, -- 无人机地速（以实际为准）
                                             location_alit        double, -- 气压高度
                                             location_lat         double, -- 探测到的无人机纬度（精确到小数点后6位）
                                             location_lon         double,-- 探测到的无人机经度（精确到小数点后6位）
                                             location_direc       double -- 保留字
                                                 >,
                                             system_info row<
                                                 alit                 double, -- 控制站高度
                                             sys_classification   double, -- 保留字
                                             sys_area_count       double, -- 运行区域计数
                                             sys_category         double, -- 保留字
                                             sys_lat              double, -- 控制无人机人员纬度
                                             sys_location_type    double, -- 保留字
                                             sys_area_rad         double, -- 运行区域半径
                                             sys_lon              double, -- 控制无人机人员经度
                                             sys_coordinate_type  double, -- 保留字
                                             sys_area_ceil        double, -- 运行区域高度上限
                                             sys_area_floor       double, -- 运行区域高度下限
                                             sys_class            double, -- 保留字
                                             sys_timestamp        string,
                                             operator_type        bigint, -- 描述类型 保留字
                                             operator_id          string -- 可为空
                                                 >,
                                             operator_info row<>,

    -- AOA的数据字段
                                             droneUniqueId   					string, -- 无人机唯一ID
                                             droneTargetNumber					double, -- 无人机目标编号
                                             droneTargetName						string, -- 无人机目标名称
                                             droneLongitude						double, -- 无人机所在经度
                                             droneLatitude						double, -- 无人机所在纬度
                                             droneAltitude						double, -- 无人机所在海拔高度
                                             dronePressureAltitude				double, -- 无人机所在气压高度
                                             monitoringStationDirectionAngle	    double, -- 监测站识别的目标方向角
                                             distanceFromMonitoringStation		double, -- 无人机距离监测站的距离
                                             controllerLongitude					double, -- 遥控器所在经度
                                             controllerLatitude					double, -- 遥控器所在纬度
                                             targetFrequencyKHz					double, -- 目标使用频率 (kHz)
                                             targetBandwidthKHz					double, -- 目标带宽 (kHz)
                                             targetSignalStrengthDb				double, -- 目标信号强度 (dB)
                                             confidence							double, -- 置信度
                                             droneTimestamp						bigint, -- 无人机识别时间戳
                                             droneVelocityMs						double -- 无人机飞行速度 (m/s)
                                                 >
                                                 >

) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = '135.100.11.110:30090',
      -- 'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.group.id' = 'iot-rid-data6',
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
create table dwd_bhv_target_rt (
                                   device_id                      string  comment 'RID、AOA设备id',
                                   uav_id                         string  comment '无人机的id-sn号',
                                   control_station_id             string  comment '控制站的id',
                                   acquire_time                   string  comment '采集时间',
                                   max_acquire_time               string  comment 'aoa-rid最大时间',
                                   src_code                       string  comment '数据类型 RID、AOA Merge',
                                   pk_type					     string  comment '数据本身的来源是RID还是AOA',
                                   src_pk                	     string  comment '数据自身的id',
                                   rid_device_id                  string  comment 'RID的设备id-牍术接入的',
                                   aoa_device_id                  string  comment 'AOA的设备id-牍术接入的',
                                   uav_device_id                  string  comment '无人机设备id-牍术接入的',
                                   rid_devid                      string  comment 'rid设备的id-飞机上报的',
                                   msgtype                        bigint  comment '消息类型',
                                   recvtype                       string  comment '无人机数据类型,示例:2.4G',
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
                                   aoa_longitude                  double  comment 'AOA数据的无人机经度',
                                   aoa_latitude                   double  comment 'AOA数据的无人机纬度',
                                   aoa_control_station_longitude  double  comment 'AOA数据的控制站纬度',
                                   aoa_control_station_latitude   double  comment 'AOA数据的控制站纬度',
                                   target_name             	     string  comment 'aoa-目标名称',
                                   altitude                	     double  comment 'aoa-无人机所在海拔高度',
                                   pressure_altitude		         double  comment 'aoa-无人机所在气压高度',
                                   direction_angle         	     double  comment 'aoa-监测站识别的目标方向角',
                                   distance_from_station   	     double  comment 'aoa-无人机距离监测站的距离',
                                   speed_ms                       double  comment 'aoa-无人机飞行速度 (m/s)',
                                   target_frequency_khz    	     double  comment 'aoa-目标使用频率 (k_hz)',
                                   target_bandwidth_khz		     double  comment 'aoa-目标带宽 (k_hz)',
                                   target_signal_strength_db	     double  comment 'aoa-目标信号强度 (d_b)',
                                   filter_col                     string  comment '动态筛选字段拼接',
                                   update_time                    string  comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      -- 'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dwd_bhv_target_rt',
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
create table dws_bhv_target_last_location_rt (
                                                 device_id                      string  comment 'RID、AOA设备id',
                                                 uav_id                         string  comment '无人机的id-sn号',
                                                 control_station_id             string  comment '控制站的id',
                                                 acquire_time                   string  comment '采集时间',
                                                 max_acquire_time               string  comment 'aoa-rid最大时间',
                                                 src_code                       string  comment '数据类型 RID、AOA Merge',
                                                 pk_type					     string  comment '数据本身的来源是RID还是AOA',
                                                 src_pk                	     string  comment '数据自身的id',
                                                 rid_device_id                  string  comment 'RID的设备id-牍术接入的',
                                                 aoa_device_id                  string  comment 'AOA的设备id-牍术接入的',
                                                 uav_device_id                  string  comment '无人机设备id-牍术接入的',
                                                 rid_devid                      string  comment 'rid设备的id-飞机上报的',
                                                 msgtype                        bigint  comment '消息类型',
                                                 recvtype                       string  comment '无人机数据类型,示例:2.4G',
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
                                                 aoa_longitude                  double  comment 'AOA数据的无人机经度',
                                                 aoa_latitude                   double  comment 'AOA数据的无人机纬度',
                                                 aoa_control_station_longitude  double  comment 'AOA数据的控制站纬度',
                                                 aoa_control_station_latitude   double  comment 'AOA数据的控制站纬度',
                                                 target_name             	     string  comment 'aoa-目标名称',
                                                 altitude                	     double  comment 'aoa-无人机所在海拔高度',
                                                 pressure_altitude		         double  comment 'aoa-无人机所在气压高度',
                                                 direction_angle         	     double  comment 'aoa-监测站识别的目标方向角',
                                                 distance_from_station   	     double  comment 'aoa-无人机距离监测站的距离',
                                                 speed_ms                       double  comment 'aoa-无人机飞行速度 (m/s)',
                                                 target_frequency_khz    	     double  comment 'aoa-目标使用频率 (k_hz)',
                                                 target_bandwidth_khz		     double  comment 'aoa-目标带宽 (k_hz)',
                                                 target_signal_strength_db	     double  comment 'aoa-目标信号强度 (d_b)',
                                                 filter_col                     string  comment '动态筛选字段拼接',
                                                 update_time                    string  comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      -- 'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_bhv_target_last_location_rt',
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
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      -- 'fenodes' = '172.21.30.245:8030',
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
                                   device_id               string  comment '无人机设备id-牍术接入的',
                                   sn                      string  comment '序列号',
                                   name                    string  comment '无人机名称',
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
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      -- 'fenodes' = '172.21.30.245:8030',
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
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      -- 'fenodes' = '172.21.30.245:8030',
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


-- 融合关系表
create table dws_rl_merge_id_rt (
                                    rid_uav_id            string, -- rid的无人机id
                                    aoa_uav_id            string, -- aoa的无人机id
                                    max_rid_acquire_time  string, -- rid的入库时间
                                    max_aoa_acquire_time  string, -- aoa的入库时间
                                    update_time           string -- 更新时间
) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      -- 'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_rl_merge_id_rt',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='1s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- ********************************** doris数据表读取 ***********************************

-- 无人机实体表来源
create table `dws_et_uav_info_source` (
                                          id                      string  comment '无人机id-sn号',
                                          device_id               string  comment '无人机设备id-牍术接入的',
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
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      -- 'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
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
      'url' = 'jdbc:mysql://135.100.11.110:31306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
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
                           updateTime            string, -- flink处理时间
                           primary key (id,acquireTime) NOT ENFORCED
) with (
      'connector' = 'upsert-kafka',
      'topic' = 'uav_source',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'uav_source2',
      'key.format' = 'json',
      'value.format' = 'json'
      );


-----------------------

-- 数据处理

-----------------------

-- 数据字段处理
create view temp01 as
select
    productKey                                          as product_key,
    deviceId                                            as device_id,
    from_unixtime(message.`timestamp`/1000,'yyyy-MM-dd HH:mm:ss') acquire_time,
    to_timestamp(from_unixtime(message.`timestamp`/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')  as acquire_time_timestamp_type, -- 接受时间的时间类型
    message.`timestamp`                                 as acquire_timestamp,
    message.`method`                                    as `method`,

    message.`data`.longitude                            as longitude, -- 属性上报的
    message.`data`.latitude                             as latitude,  -- 属性上报的
    message.`data`.openAlarm                            as open_alarm, -- 属性上报的
    message.`data`.outpowerAlarm                        as outpower_alarm, -- 属性上报的

    message.`data`.devid                                as rid_devid,
    message.`data`.msgtype                              as msgtype,
    message.`data`.recvtype                             as recvtype,
    message.`data`.recvmac                              as recvmac,
    message.`data`.mac                                  as mac,
    message.`data`.rssi                                 as rssi,

    REPLACE(REPLACE(message.`data`.basic_info.basic_uaid,'\t',''),'\n','')    as uav_id,
    message.`data`.basic_info.basic_idtype              as basic_idtype,
    message.`data`.basic_info.basic_uatype              as basic_uatype,

    message.`data`.location_info.location_speed_v       as speed_v,              -- 垂直速度
    message.`data`.location_info.location_hori_acc      as hori_acc,             -- 水平精度
    message.`data`.location_info.location_direc         as location_direc,       -- 保留字
    message.`data`.location_info.location_vert_acc      as vert_acc,             -- 垂直精度
    message.`data`.location_info.location_speed_multi   as location_speed_multi, -- 保留字
    message.`data`.location_info.location_height_type   as height_type,          -- 高度类型
    message.`data`.location_info.location_ew            as ew,                   -- 航迹角
    message.`data`.location_info.location_height        as height,               -- 无人机距地高度
    message.`data`.location_info.location_status        as location_status,      -- 保留字
    message.`data`.location_info.location_speed_acc     as speed_acc,            -- 速度精度
    message.`data`.location_info.location_speed_h       as speed_h,              -- 无人机地速（以实际为准）
    message.`data`.location_info.location_alit          as location_alit,        -- 气压高度

    message.`data`.system_info.alit                     as control_station_height, -- 控制站高度
    message.`data`.system_info.sys_classification       as sys_classification,   -- 保留字
    message.`data`.system_info.sys_area_count           as sys_area_count,       -- 运行区域计数
    message.`data`.system_info.sys_category             as sys_category,         -- 保留字
    message.`data`.system_info.sys_location_type        as sys_location_type,     -- 保留字
    message.`data`.system_info.sys_area_rad             as sys_area_rad,          -- 运行区域半径

    message.`data`.system_info.sys_coordinate_type      as sys_coordinate_type,
    message.`data`.system_info.sys_area_ceil            as sys_area_ceil,         -- 运行区域高度上限
    message.`data`.system_info.sys_area_floor           as sys_area_floor,        -- 运行区域高度下限
    message.`data`.system_info.sys_class                as sys_class,             -- 保留字
    message.`data`.system_info.sys_timestamp            as sys_timestamp,         -- 探测到无人机时间戳
    message.`data`.system_info.operator_type            as operator_type,         -- 描述类型 保留字
    message.`data`.system_info.operator_id              as operator_id,            -- 无用-可为空
    coalesce(message.`data`.location_info.location_lon,message.`data`.droneLongitude) as uav_longitude, -- 无人机纬度
    coalesce(message.`data`.location_info.location_lat,message.`data`.droneLatitude)  as uav_latitude, -- 无人机纬度
    coalesce(message.`data`.system_info.sys_lon,message.`data`.controllerLongitude) as control_station_longitude, -- 控制站经度
    coalesce(message.`data`.system_info.sys_lat,message.`data`.controllerLatitude)  as control_station_latitude, -- 控制站纬度

    -- aoa设备数据
    message.`data`.droneUniqueId                        as aoa_uav_id,                    -- 无人机唯一ID
    message.`data`.droneTargetNumber                    as drone_target_no,               -- 无人机目标编号
    message.`data`.droneTargetName                      as target_name,                   -- 目标名称
    message.`data`.droneAltitude                        as altitude,                      -- 无人机所在海拔高度
    message.`data`.dronePressureAltitude                as pressure_altitude,             -- 无人机所在气压高度
    message.`data`.monitoringStationDirectionAngle      as direction_angle,               -- 监测站识别的目标方向角
    message.`data`.distanceFromMonitoringStation        as distance_from_station,         -- 无人机距离监测站的距离
    message.`data`.targetFrequencyKHz                   as target_frequency_khz,          -- 目标使用频率 (k_hz)
    message.`data`.targetBandwidthKHz                   as target_bandwidth_khz,          -- 目标带宽 (k_hz)
    message.`data`.targetSignalStrengthDb               as target_signal_strength_db,     -- 目标信号强度 (d_b)
    message.`data`.confidence                           as confidence,                    -- 置信度
    message.`data`.droneTimestamp                       as drone_timestamp,               -- 时间戳
    message.`data`.droneVelocityMs                      as speed_ms,                      -- 无人机飞行速度 (m/s)
    PROCTIME() as proctime
from iot_device_message_kafka_01
where message.`timestamp` is not null
  and productKey in('9sMhTTcOrbv','iiCC6Y7Qhmz');


-- 关联设备位置，筛选距离范围内数据，较远数据丢弃 15km的距离
create view temp02 as
select
    t1.*,
    'JOIN' as join_column
from (
         select
             *
         from temp01
         where ((`method` = 'event.ridMessage.info' and uav_id is not null and uav_id <> '') or (`method` = 'event.aoaMessage.info'))
           and uav_longitude between -180 and 180
           and uav_latitude between -90 and 90
           and uav_longitude <> 0
           and uav_latitude <> 0
           and control_station_longitude between -180 and 180
           and control_station_latitude between -90 and 90
           and control_station_longitude <>0
           and control_station_latitude <> 0
     ) as t1
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 设备表 关联设备位置
                   on t1.device_id = t2.device_id
where distance_udf(t1.uav_latitude,t1.uav_longitude,t2.latitude,t2.longitude) <= 15;


-- -- aoa的来源数据筛选处理 , 并融合关联无人机id
-- create view temp02_aoa as
-- select
--   tt.*
-- from (
-- select
--   t1.*,
--   abs(timestampdiff(second,t1.acquire_time_timestamp_type,t2.acquire_time)) as date_diff_s_flag,
--   distance_udf(
--                 t1.uav_latitude,
--                 t1.uav_longitude,
--                 t2.latitude,
--                 t2.longitude) as distance_pre_curr,
--   t2.uav_id
-- from (
--     select
--       drone_target_no,
--       target_name,
--       altitude,
--       pressure_altitude,
--       direction_angle,
--       distance_from_station,
--       control_station_longitude,
--       control_station_latitude,
--       target_frequency_khz,
--       target_bandwidth_khz,
--       target_signal_strength_db,
--       confidence,
--       drone_timestamp,
--       speed_ms,
--       uav_longitude,
--       uav_latitude,
--       product_key,
--       device_id,
--       acquire_time,
--       acquire_time_timestamp_type,
--       proctime
--     from temp02
--     where product_key = '9sMhTTcOrbv' and (aoa_uav_id is null or trim(aoa_uav_id) is null or trim(aoa_uav_id) = '')
--   ) as t1 left join dws_bhv_aoa_last_location_rt_source FOR SYSTEM_TIME AS OF t1.proctime as t2
--   on t1.device_id = t2.device_id
--   and t1.acquire_time_timestamp_type between t2.acquire_time - interval '30' second and t2.acquire_time + interval '30' second
-- ) as tt
--   where (uav_id is not null and distance_pre_curr < 100) or uav_id is null;



-- -- aoa开窗取时间最接近的,没有关联上取random id
-- create view temp03_aoa as
-- select
--   drone_target_no,
--   target_name,
--   altitude,
--   pressure_altitude,
--   direction_angle,
--   distance_from_station,
--   control_station_longitude,
--   control_station_latitude,
--   target_frequency_khz,
--   target_bandwidth_khz,
--   target_signal_strength_db,
--   confidence,
--   drone_timestamp,
--   speed_ms,
--   uav_longitude as longitude,
--   uav_latitude as latitude,
--   product_key,
--   device_id,
--   acquire_time,
--   acquire_time_timestamp_type,
--   proctime,
--   if(uav_id is not null,uav_id,b.uav_random_id) as uav_id,
--   'JOIN' as join_column
-- from (
-- select
--   *
-- from (
--   select
--     *,
--     row_number()over(partition by target_name,acquire_time order by distance_pre_curr asc,date_diff_s_flag asc) as rk
--   from temp02_aoa
-- ) as t1
--   where rk = 1
-- ) as tt1,lateral table(PassThroughUdtf(get_random_id())) as b(uav_random_id);


-- aoa数据ID整合
create view temp03_aoa as
select
    device_id,     -- AOA的设备ID
    coalesce(aoa_uav_id_trim,uav_random_id) as uav_id,
    'AOA'                               as src_code,
    acquire_time,
    acquire_time                        as max_acquire_time,
    'AOA'                               as pk_type,
    coalesce(aoa_uav_id_trim,uav_random_id)  as src_pk,
    cast(null as varchar)               as rid_device_id,
    device_id                           as aoa_device_id,
    cast(null as varchar)               as rid_devid,
    cast(null as bigint)                as msgtype,
    cast(null as varchar)               as recvtype,
    cast(null as varchar)               as recvmac,
    cast(null as varchar)               as mac,
    cast(null as bigint)                as rssi,
    uav_longitude                       as longitude, -- rid的无人机经度
    uav_latitude                        as latitude,  -- rid的无人机纬度
    cast(null as double)                as location_alit,
    cast(null as double)                as ew,      -- RID航迹角
    cast(null as double)                as speed_h, -- RID水平速度
    cast(null as double)                as speed_v, -- RID垂直速度
    cast(null as double)                as height,  -- RID距地高度
    cast(null as double)                as height_type, -- RID高度类型
    control_station_longitude,         -- RID控制站经度
    control_station_latitude,          -- RID控制站纬度
    cast(null as double)                as control_station_height, -- RID控制站高度
    uav_longitude                       as aoa_longitude,
    uav_latitude                        as aoa_latitude,
    control_station_longitude           as aoa_control_station_longitude,
    control_station_latitude            as aoa_control_station_latitude,
    target_name,
    altitude,
    pressure_altitude,
    direction_angle,
    distance_from_station,
    speed_ms,
    target_frequency_khz,
    target_bandwidth_khz,
    target_signal_strength_db,
    drone_target_no,
    acquire_time_timestamp_type as aoa_acquire_time_timestamp_type,
    drone_timestamp,
    confidence,
    cast(null as varchar) as aoa_uav_id,
    join_column,
    proctime
from (
         select
             *,
             if(aoa_uav_id = '' or aoa_uav_id is null or trim(aoa_uav_id) = '' or trim(aoa_uav_id) is null,cast(null as varchar),trim(aoa_uav_id)) as aoa_uav_id_trim
         from temp02
         where product_key = '9sMhTTcOrbv'   -- AOA的key
     ) as tt1 ,lateral table(PassThroughUdtf(get_random_id())) as b(uav_random_id);



-- 筛选rid数据
create view temp01_rid as
select
    *
from temp02
where product_key = 'iiCC6Y7Qhmz';


-- 筛选rid数据,与aoa数据关联融合
create view temp01_rid_merge as
select
    *
from (
         select
             t1.product_key,
             t1.device_id,
             t1.acquire_time,
             t1.acquire_time_timestamp_type,
             t1.acquire_timestamp,
             t1.rid_devid,
             t1.msgtype,
             t1.recvtype,
             t1.recvmac,
             t1.mac,
             t1.rssi,
             t1.uav_id,
             t1.speed_v,
             t1.height_type,
             t1.ew,
             t1.height,
             t1.speed_h,
             t1.location_alit,
             t1.control_station_height,
             t1.control_station_longitude,
             t1.control_station_latitude,
             t1.uav_longitude  as longitude,
             t1.uav_latitude   as latitude,
             t1.join_column,
             t1.proctime,

             t2.drone_target_no,
             t2.acquire_time as aoa_acquire_time,
             t2.aoa_acquire_time_timestamp_type,
             t2.target_name,
             t2.altitude,
             t2.pressure_altitude,
             t2.direction_angle,
             t2.distance_from_station,
             t2.aoa_control_station_longitude,
             t2.aoa_control_station_latitude,
             t2.target_frequency_khz,
             t2.target_bandwidth_khz,
             t2.target_signal_strength_db,
             t2.confidence,
             t2.speed_ms,
             t2.drone_timestamp,
             t2.aoa_longitude,
             t2.aoa_latitude,
             t2.uav_id        as aoa_uav_id,
             t2.device_id     as aoa_device_id,
             abs(timestampdiff(second,t1.acquire_time_timestamp_type,t2.aoa_acquire_time_timestamp_type)) as date_diff_s_flag,
             distance_udf(
                     t1.uav_longitude,
                     t1.uav_latitude,
                     t2.longitude,
                     t2.latitude) * 1000 as distance_pre_curr        -- udf返回为km
         from temp01_rid as t1
                  left join temp03_aoa as t2
                            on t1.join_column = t2.join_column
                                and t1.acquire_time_timestamp_type between t2.aoa_acquire_time_timestamp_type - interval '30' second and t2.aoa_acquire_time_timestamp_type + interval '30' second
     ) as tt
where (aoa_longitude is not null and distance_pre_curr < 100)
   or aoa_longitude is null;


-- RID数据融合结束
create view temp02_rid_merge as
select
    device_id,
    uav_id,
    if(aoa_uav_id is not null,'RID,AOA','RID') as src_code,
    acquire_time,
    case when aoa_acquire_time_timestamp_type is null then acquire_time
         when timestampdiff(second,acquire_time_timestamp_type,aoa_acquire_time_timestamp_type) >0 then acquire_time
         else aoa_acquire_time
        end as max_acquire_time,
    'RID'                 as pk_type,
    uav_id                as src_pk,
    device_id             as rid_device_id,
    aoa_device_id,
    rid_devid,
    msgtype,
    recvtype,
    recvmac,
    mac,
    rssi,
    longitude,
    latitude,
    location_alit,
    ew,
    speed_h,
    speed_v,
    height,
    height_type,
    control_station_longitude,
    control_station_latitude,
    control_station_height,

    aoa_longitude,
    aoa_latitude,
    aoa_control_station_longitude,
    aoa_control_station_latitude,
    target_name,
    altitude,
    pressure_altitude,
    direction_angle,
    distance_from_station,
    speed_ms,
    target_frequency_khz,
    target_bandwidth_khz,
    target_signal_strength_db,
    drone_target_no,
    acquire_time_timestamp_type,
    drone_timestamp,
    confidence,
    aoa_uav_id,
    join_column,
    proctime
from(
        select
            *,
            row_number()over(partition by device_id,uav_id,acquire_time_timestamp_type order by date_diff_s_flag asc,distance_pre_curr asc) as rk
        from temp01_rid_merge
    ) as ttt
where rk = 1;



-- 数据union 整合字段，入库融合表
create view temp_04 as
select
    t1.device_id,
    t1.uav_id,
    src_code,
    acquire_time,
    max_acquire_time,
    pk_type,
    src_pk,
    rid_device_id,
    aoa_device_id,
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
    aoa_longitude,
    aoa_latitude,
    aoa_control_station_longitude,
    aoa_control_station_latitude,
    target_name,
    altitude,
    pressure_altitude,
    direction_angle,
    distance_from_station,
    speed_ms,
    target_frequency_khz,
    target_bandwidth_khz,
    target_signal_strength_db,
    concat('cs',uav_id)    as control_station_id,

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
            ifnull(cast(aoa_longitude as varchar),''),'¥',
            ifnull(cast(aoa_latitude as varchar),''),'¥',
            ifnull(cast(aoa_control_station_longitude as varchar),''),'¥',
            ifnull(cast(aoa_control_station_latitude as varchar),''),'¥',
            ifnull(target_name,''),'¥',
            ifnull(cast(altitude as varchar),''),'¥',
            ifnull(cast(pressure_altitude as varchar),''),'¥',
            ifnull(cast(direction_angle as varchar),''),'¥',
            ifnull(cast(distance_from_station as varchar),''),'¥',
            ifnull(cast(speed_ms as varchar),''),'¥',
            ifnull(cast(target_frequency_khz as varchar),''),'¥',
            ifnull(cast(target_bandwidth_khz as varchar),''),'¥',
            ifnull(cast(target_signal_strength_db as varchar),'')
        ) as filter_col

from (
         select * from temp02_rid_merge

         union all

         select * from temp03_aoa
     ) as t1
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 设备表 关联无人机
                   on t1.uav_id = t2.sn and 'UAV' = t2.type
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
    coalesce(rid_device_id,aoa_device_id) as device_id,
    rid_devid,
    max_acquire_time as acquire_time,
    from_unixtime(unix_timestamp()) as update_time
from temp_04;


-- 控制站实体表
insert into dws_et_control_station_info
select
    control_station_id     as id,
    acquire_time,
    coalesce(target_name,uav_id)     as name, -- 无人机id-sn号
    uav_id                            as register_uav,
    src_code as source,
    uav_id                 as search_content,
    from_unixtime(unix_timestamp()) as update_time
from temp_04
where doris_uav_join_id is null;


-- 无人机实体表
insert into dws_et_uav_info
select
    uav_id                       as id,
    join_dushu_uav_device_id     as device_id,
    uav_id                       as sn,
    coalesce(join_dushu_uav_name,target_name,uav_id)    as name,
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
from temp_04
where doris_uav_join_id is null;




-- rid、aoa的融合数据入库
insert into dwd_bhv_target_rt
select
    device_id,
    uav_id                         ,
    control_station_id             ,
    acquire_time                   ,
    max_acquire_time               ,
    src_code                       ,
    pk_type					     ,
    src_pk                	     ,
    rid_device_id                  ,
    aoa_device_id                  ,
    join_dushu_uav_device_id as uav_device_id,
    rid_devid                      ,
    msgtype                        ,
    recvtype                       ,
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
    aoa_longitude                  ,
    aoa_latitude                   ,
    aoa_control_station_longitude  ,
    aoa_control_station_latitude   ,
    target_name             	     ,
    altitude                	     ,
    pressure_altitude		         ,
    direction_angle         	     ,
    distance_from_station   	     ,
    speed_ms                       ,
    target_frequency_khz    	     ,
    target_bandwidth_khz		     ,
    target_signal_strength_db	     ,
    filter_col                     ,
    from_unixtime(unix_timestamp()) as update_time
from temp_04;


-- rid、aoa的融合状态数据入库
insert into dws_bhv_target_last_location_rt
select
    device_id,
    uav_id                         ,
    control_station_id             ,
    acquire_time                   ,
    max_acquire_time               ,
    src_code                       ,
    pk_type					     ,
    src_pk                	     ,
    rid_device_id                  ,
    aoa_device_id                  ,
    join_dushu_uav_device_id as uav_device_id,
    rid_devid                      ,
    msgtype                        ,
    recvtype                       ,
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
    aoa_longitude                  ,
    aoa_latitude                   ,
    aoa_control_station_longitude  ,
    aoa_control_station_latitude   ,
    target_name             	     ,
    altitude                	     ,
    pressure_altitude		         ,
    direction_angle         	     ,
    distance_from_station   	     ,
    speed_ms                       ,
    target_frequency_khz    	     ,
    target_bandwidth_khz		     ,
    target_signal_strength_db	     ,
    filter_col                     ,
    from_unixtime(unix_timestamp()) as update_time
from temp_04;


-- 融合对应数据入库
insert into dws_rl_merge_id_rt
select
    uav_id as rid_uav_id,
    aoa_uav_id,
    acquire_time as max_rid_acquire_time,
    cast(null as varchar) as max_aoa_acquire_time,
    from_unixtime(unix_timestamp()) as update_time
from temp02_rid_merge
where uav_id is not null
  and aoa_uav_id is not null;



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
from temp_04;


end;



