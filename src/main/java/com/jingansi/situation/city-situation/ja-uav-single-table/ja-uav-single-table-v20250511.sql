--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2025/04/23 16:48:50
-- description: rid设备数据采集数据,aoa采集数据 单独直接入库
-- version: ja-uav-single-table-v250428
--********************************************************************--

set 'pipeline.name' = 'ja-uav-single-table';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
-- SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '3';
-- set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';

SET 'execution.checkpointing.interval' = '300000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-uav-single-table';


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
      -- 'properties.bootstrap.servers' = '135.100.11.110:30090',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.group.id' = 'iot-rid-data7',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',  -- 1745564415000
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- ********************************* doris写入的表 ********************************************

-- rid数据全量单独入库doris
create table dwd_bhv_rid_rt (
                                device_id                  string  comment 'RID的设备id-牍术接入的',
                                uav_id                     string  comment 'uav无人机id,sn号',
                                control_station_id         string  comment '控制站的id',
                                acquire_time               string  comment '采集时间',
                                uav_device_id              string  comment '无人机设备id-牍术接入的',
                                rid_devid                  string  comment 'rid设备的id-飞机上报的',
                                msgtype                    bigint  comment '消息类型',
                                recvtype                   string  comment '无人机数据类型，示例：2.4G',
                                mac                        string  comment 'rid设备MAC地址',
                                rssi                       bigint  comment '信号强度',
                                basic_uatype               bigint  comment '无人机UA 类型  1：设备序列号 2：由 CAA 提供的 UAS 实名登记号 3：UTM 任务 ID （3-0 位，UA 类型）',
                                basic_idtype               bigint,
                                longitude                  double  comment '探测到的无人机经度（精确到小数点后6位）',
                                latitude                   double  comment '探测到的无人机纬度（精确到小数点后6位）',
                                location_alit              double  comment '气压高度',
                                ew                         double  comment '航迹角',
                                speed_h                    double  comment '无人机地速-水平速度',
                                speed_v                    double  comment '垂直速度',
                                height                     double  comment '无人机距地高度',
                                height_type                double  comment '高度类型',
                                hori_acc                   double  comment '水平精度',
                                vert_acc                   double  comment '垂直精度',
                                speed_acc                  double  comment '速度精度',
                                control_station_longitude  double  comment 'system_info-控制无人机人员经度',
                                control_station_latitude   double  comment 'system_info-控制无人机人员纬度',
                                control_station_height     double  comment '控制站高度',
                                sys_area_count             double  comment '运行区域计数',
                                sys_area_rad               double  comment '运行区域半径',
                                sys_area_ceil              double  comment '运行区域高度上限',
                                sys_area_floor             double  comment '运行区域高度下限',
                                sys_classification         double  comment '保留字',
                                sys_category               double  comment '保留字',
                                sys_class                  double  comment '保留字',
                                sys_location_type          double  comment '保留字',
                                sys_coordinate_type        double  comment '保留字',
                                location_direc             double  comment '保留字',
                                location_status            double  comment '保留字',
                                location_speed_multi       double  comment '保留字',
                                operator_type              bigint  comment '描述类型 保留字',
                                operator_id                string  comment '无用-可为空',
                                sys_timestamp              string  comment '无人机自己上报的自己的时间-可能不对的',
                                update_time                string  comment '更新时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'fenodes' = '172.21.30.105:30030',
      'table.identifier' = 'sa.dwd_bhv_rid_rt',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- rid数据状态单独入库doris
create table dws_bhv_rid_last_location_rt (
                                              device_id                  string  comment 'RID的设备id-牍术接入的',
                                              uav_id                     string  comment 'uav无人机id,sn号',
                                              control_station_id         string  comment '控制站的id',
                                              acquire_time               string  comment '采集时间',
                                              uav_device_id              string  comment '无人机设备id-牍术接入的',
                                              rid_devid                  string  comment 'rid设备的id-飞机上报的',
                                              msgtype                    bigint  comment '消息类型',
                                              recvtype                   string  comment '无人机数据类型，示例：2.4G',
                                              mac                        string  comment 'rid设备MAC地址',
                                              rssi                       bigint  comment '信号强度',
                                              basic_uatype               bigint  comment '无人机UA 类型  1：设备序列号 2：由 CAA 提供的 UAS 实名登记号 3：UTM 任务 ID （3-0 位，UA 类型）',
                                              basic_idtype               bigint,
                                              longitude                  double  comment '探测到的无人机经度（精确到小数点后6位）',
                                              latitude                   double  comment '探测到的无人机纬度（精确到小数点后6位）',
                                              location_alit              double  comment '气压高度',
                                              ew                         double  comment '航迹角',
                                              speed_h                    double  comment '无人机地速-水平速度',
                                              speed_v                    double  comment '垂直速度',
                                              height                     double  comment '无人机距地高度',
                                              height_type                double  comment '高度类型',
                                              hori_acc                   double  comment '水平精度',
                                              vert_acc                   double  comment '垂直精度',
                                              speed_acc                  double  comment '速度精度',
                                              control_station_longitude  double  comment 'system_info-控制无人机人员经度',
                                              control_station_latitude   double  comment 'system_info-控制无人机人员纬度',
                                              control_station_height     double  comment '控制站高度',
                                              sys_area_count             double  comment '运行区域计数',
                                              sys_area_rad               double  comment '运行区域半径',
                                              sys_area_ceil              double  comment '运行区域高度上限',
                                              sys_area_floor             double  comment '运行区域高度下限',
                                              sys_classification         double  comment '保留字',
                                              sys_category               double  comment '保留字',
                                              sys_class                  double  comment '保留字',
                                              sys_location_type          double  comment '保留字',
                                              sys_coordinate_type        double  comment '保留字',
                                              location_direc             double  comment '保留字',
                                              location_status            double  comment '保留字',
                                              location_speed_multi       double  comment '保留字',
                                              operator_type              bigint  comment '描述类型 保留字',
                                              operator_id                string  comment '无用-可为空',
                                              sys_timestamp              string  comment '无人机自己上报的自己的时间-可能不对的',
                                              update_time                string  comment '更新时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'fenodes' = '172.21.30.105:30030',
      'table.identifier' = 'sa.dws_bhv_rid_last_location_rt',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- aoa的数据入库全量表doris
create table dwd_bhv_aoa_rt (
                                device_id         		          string  comment 'AOA的设备id-牍术接入的',
                                uav_id                              string  comment '无人机唯一ID',
                                acquire_time                        string  comment '采集时间',
                                drone_target_no                     double  comment '无人机目标编号',
                                target_name   	                  string  comment '无人机目标名称',
                                longitude   	                      double  comment '无人机所在经度',
                                latitude				              double  comment '无人机所在纬度',
                                altitude			                  double  comment '无人机所在海拔高度',
                                pressure_altitude			          double  comment '无人机所在气压高度',
                                direction_angle                     double  comment '监测站识别的目标方向角',
                                distance_from_station	              double  comment '无人机距离监测站的距离',
                                control_station_longitude			  double  comment '遥控器所在经度',
                                control_station_latitude			  double  comment '遥控器所在纬度',
                                target_frequency_khz			      double  comment '目标使用频率 (k_hz)',
                                target_bandwidth_khz			      double  comment '目标带宽 (k_hz)',
                                target_signal_strength_db	          double  comment '目标信号强度 (d_b)',
                                confidence						  double  comment '置信度',
                                drone_timestamp					  bigint  comment '无人机识别时间戳',
                                speed_ms					          double  comment '无人机飞行速度 (m/s)',
                                update_time           		      string  comment '更新时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'fenodes' = '172.21.30.105:30030',
      'table.identifier' = 'sa.dwd_bhv_aoa_rt',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- aoa的数据入库状态表doris
create table dws_bhv_aoa_last_location_rt (
                                              device_id         		          string  comment 'AOA的设备id-牍术接入的',
                                              uav_id                              string  comment '无人机唯一ID',
                                              acquire_time                        string  comment '采集时间',
                                              drone_target_no                     double  comment '无人机目标编号',
                                              target_name   	                  string  comment '无人机目标名称',
                                              longitude   	                      double  comment '无人机所在经度',
                                              latitude				              double  comment '无人机所在纬度',
                                              altitude			                  double  comment '无人机所在海拔高度',
                                              pressure_altitude			          double  comment '无人机所在气压高度',
                                              direction_angle                     double  comment '监测站识别的目标方向角',
                                              distance_from_station	              double  comment '无人机距离监测站的距离',
                                              control_station_longitude			  double  comment '遥控器所在经度',
                                              control_station_latitude			  double  comment '遥控器所在纬度',
                                              target_frequency_khz			      double  comment '目标使用频率 (k_hz)',
                                              target_bandwidth_khz			      double  comment '目标带宽 (k_hz)',
                                              target_signal_strength_db	          double  comment '目标信号强度 (d_b)',
                                              confidence						  double  comment '置信度',
                                              drone_timestamp					  bigint  comment '无人机识别时间戳',
                                              speed_ms					          double  comment '无人机飞行速度 (m/s)',
                                              update_time           		      string  comment '更新时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'fenodes' = '172.21.30.105:30030',
      'table.identifier' = 'sa.dws_bhv_aoa_last_location_rt',
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

-----------------------

-- 数据处理

-----------------------

-- 数据字段处理
create view temp01 as
select
    productKey                                          as product_key,
    deviceId                                            as device_id,
    from_unixtime(message.`timestamp`/1000,'yyyy-MM-dd HH:mm:ss') acquire_time,
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



-- 筛选aoa数据 整合字段
create view temp01_aoa as
select
    device_id,     -- AOA的设备ID
    coalesce(aoa_uav_id_trim,uav_random_id) as uav_id,
    acquire_time,
    drone_target_no,
    uav_longitude                   as longitude, -- rid的无人机经度
    uav_latitude                    as latitude,  -- rid的无人机纬度
    control_station_longitude,      -- RID控制站经度
    control_station_latitude,       -- RID控制站纬度
    target_name,
    altitude,
    pressure_altitude,
    direction_angle,
    distance_from_station,
    speed_ms,
    target_frequency_khz,
    target_bandwidth_khz,
    target_signal_strength_db,
    drone_timestamp,
    confidence
from (
         select
             *,
             if(aoa_uav_id = '' or aoa_uav_id is null or trim(aoa_uav_id) = '' or trim(aoa_uav_id) is null,cast(null as varchar),trim(aoa_uav_id)) as aoa_uav_id_trim
         from temp01
         where `method` = 'event.aoaMessage.info'
           and uav_longitude between -180 and 180
           and uav_latitude between -90 and 90
           and uav_longitude <> 0
           and uav_latitude <> 0
           and control_station_longitude between -180 and 180
           and control_station_latitude between -90 and 90
           and control_station_longitude <>0
           and control_station_latitude <> 0
     ) as tt1 ,lateral table(PassThroughUdtf(get_random_id())) as b(uav_random_id);


-- 筛选rid数据 整合字段
create view temp01_rid as
select
    *
from temp01
where `method` = 'event.ridMessage.info'
  and uav_longitude between -180 and 180
  and uav_latitude between -90 and 90
  and uav_longitude <> 0
  and uav_latitude <> 0
  and control_station_longitude between -180 and 180
  and control_station_latitude between -90 and 90
  and control_station_longitude <>0
  and control_station_latitude <> 0;

-----------------------

-- 数据写入

-----------------------


begin statement set;

-- aoa入库全量表
insert into dwd_bhv_aoa_rt
select
    device_id         		     ,
    uav_id                         ,
    acquire_time                   ,
    drone_target_no                ,
    target_name   	             ,
    longitude   	 ,
    latitude		 ,
    altitude			             ,
    pressure_altitude			     ,
    direction_angle                ,
    distance_from_station	         ,
    control_station_longitude      ,
    control_station_latitude       ,
    target_frequency_khz			 ,
    target_bandwidth_khz			 ,
    target_signal_strength_db	     ,
    confidence			         ,
    drone_timestamp		         ,
    speed_ms					     ,
    from_unixtime(unix_timestamp()) as update_time
from temp01_aoa;


-- aoa入库状态最后位置表
insert into dws_bhv_aoa_last_location_rt
select
    device_id         		     ,
    uav_id                         ,
    acquire_time                   ,
    drone_target_no                ,
    target_name   	             ,
    longitude   	                 ,
    latitude		                 ,
    altitude			             ,
    pressure_altitude			     ,
    direction_angle                ,
    distance_from_station	         ,
    control_station_longitude      ,
    control_station_latitude       ,
    target_frequency_khz			 ,
    target_bandwidth_khz			 ,
    target_signal_strength_db	     ,
    confidence			         ,
    drone_timestamp		         ,
    speed_ms					     ,
    from_unixtime(unix_timestamp()) as update_time
from temp01_aoa;



-- rid入库全量表
insert into dwd_bhv_rid_rt
select
    device_id                  ,
    uav_id                     ,
    concat('cs',uav_id) as control_station_id         ,
    acquire_time               ,
    cast(null as varchar) as uav_device_id,
    rid_devid                  ,
    msgtype                    ,
    recvtype                   ,
    mac                        ,
    rssi                       ,
    basic_uatype               ,
    basic_idtype               ,
    uav_longitude as longitude ,
    uav_latitude as latitude   ,
    location_alit              ,
    ew                         ,
    speed_h                    ,
    speed_v                    ,
    height                     ,
    height_type                ,
    hori_acc                   ,
    vert_acc                   ,
    speed_acc                  ,
    control_station_longitude  ,
    control_station_latitude   ,
    control_station_height     ,
    sys_area_count             ,
    sys_area_rad               ,
    sys_area_ceil              ,
    sys_area_floor             ,
    sys_classification         ,
    sys_category               ,
    sys_class                  ,
    sys_location_type          ,
    sys_coordinate_type        ,
    location_direc             ,
    location_status            ,
    location_speed_multi       ,
    operator_type              ,
    operator_id                ,
    sys_timestamp              ,
    from_unixtime(unix_timestamp()) as update_time
from temp01_rid;


-- rid入库状态最后位置表
insert into dws_bhv_rid_last_location_rt
select
    device_id                  ,
    uav_id                     ,
    concat('cs',uav_id) as control_station_id,
    acquire_time               ,
    cast(null as varchar) as uav_device_id,
    rid_devid                  ,
    msgtype                    ,
    recvtype                   ,
    mac                        ,
    rssi                       ,
    basic_uatype               ,
    basic_idtype               ,
    uav_longitude as longitude ,
    uav_latitude as latitude   ,
    location_alit              ,
    ew                         ,
    speed_h                    ,
    speed_v                    ,
    height                     ,
    height_type                ,
    hori_acc                   ,
    vert_acc                   ,
    speed_acc                  ,
    control_station_longitude  ,
    control_station_latitude   ,
    control_station_height     ,
    sys_area_count             ,
    sys_area_rad               ,
    sys_area_ceil              ,
    sys_area_floor             ,
    sys_classification         ,
    sys_category               ,
    sys_class                  ,
    sys_location_type          ,
    sys_coordinate_type        ,
    location_direc             ,
    location_status            ,
    location_speed_multi       ,
    operator_type              ,
    operator_id                ,
    sys_timestamp              ,
    from_unixtime(unix_timestamp()) as update_time
from temp01_rid;

end;



