--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2025/04/23 16:48:50
-- description: rid、aoa、警航版本
-- version: ja-uav-target-merge-v20251022 警航阵地感知三吉数据接入
--********************************************************************--

set 'pipeline.name' = 'ja-uav-target-merge';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '60000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '4';
set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';

SET 'execution.checkpointing.interval' = '120000';

SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-uav-target-merge';


-- 计算距离
create function distance_udf as 'com.jingan.udf.geohash.DistanceUdf';

-- -- 融合获取id
-- create function get_id as 'com.jingan.udf.merge.TargetTimeAndSpaceMergeDcc';


-- SET 'pipeline.global-job-parameters' = '
--   mysqlUrl:"jdbc:mysql://172.21.30.105:31030/sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8",
--   mysqlPassword:Jingansi@110,
--   temporalThreshold:10000,
--   speedThreshold:20,
--   wTime:0.2,
--   wDistance:0.4,
--   wVelocity:0,
--   wSpeedDiff:0.4,
--   syncInterval:600000';

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
                                             droneVelocityMs						double, -- 无人机飞行速度 (m/s)

    -- 雷达、检测数据
                                             targets array<
              row(
                targetId         string   ,-- -目标id
                xDistance        double   ,-- x距离
                yDistance        double   ,-- y距离
                speed            double   ,-- 速度
                targetPitch      double   ,-- 俯仰角
                targetAltitude   double   ,-- 目标海拔高度
                status           string   ,-- 0 目标跟踪 1 目标丢失 2 跟踪终止
                targetLongitude  double   ,-- 目标经度
                targetLatitude   double   , -- 目标纬度
                distance         double   , -- 上报距离
                targetYaw        double   , -- 水平角度
                utc_time         bigint   , -- 雷达检测数据上报时间戳
                tracked_times    double   , -- 已跟踪次数
                loss_times       double   , -- 连续丢失次数
                targetType       bigint   , -- 目标类型,天朗项目-也有，目标识别类型

                -- 天朗设备
                RCS               bigint, -- 目标RCS
                radialDistance    double, -- 目标位置信息
                targetState       bigint, -- 目标状态，
                `timestamp`       bigint, -- 录取时间信息（基日）
                timestampBase     bigint, -- 录取时间信息（时间）
                uploadMode        string
            )
          >
      >
  >

) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = '135.100.11.110:30090',
      'properties.group.id' = 'iot-rid-data6',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1747126853000', -- 1747102253000
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 整合rid、aoa、雷达的数据 写入
-- 在进行读取做数据融合生成id
create table rid_m30_aoa(
                            id                        string  comment 'id',
                            acquire_timestamp         bigint,
                            product_key               string,
                            device_id                 string,
                            device_name               string,
                            acquire_time              string,
                            `method`                  string,
                            src_code                  string,
                            src_pk                    string,
                            rid_devid                 string,
                            msgtype                   bigint,
                            recvtype                  string,
                            mac                       string,
                            rssi                      bigint,
                            longitude                 double,
                            latitude                  double,
                            location_alit             double,
                            ew                        double,
                            speed_h                   double,
                            speed_v                   double,
                            height                    double,
                            height_type               double,
                            control_station_longitude double,
                            control_station_latitude  double,
                            control_station_height    double,
                            target_name               string,
                            altitude                  double,
                            distance_from_station     double,
                            speed_ms                  double,
                            target_frequency_khz      double,
                            target_bandwidth_khz      double,
                            target_signal_strength_db double,
                            target_type               bigint,

                            target_list_status        int,          --  名单状态1是白名单 2是黑名单 4是民众注册无人机
                            target_list_type          int,          --  名单类型3是政务无人机 2是低空经济无人机 5是重点关注
                            user_company_name         string,       --  持有单位
                            user_full_name            string,       --  持有者姓名
                            target_direction          double,       --  无人机方位（对于发现设备）
                            target_area_code          string,       --  无人机飞行地区行政编码
                            target_registered         int,          --  无人机是否报备1是已报备
                            target_fly_report_status  int,          --  无人机报备状态1是未报备 2 是已报备 3 超出报备区域范围 4 是不符合报备飞行时间5 是超出报备区域范围 6是 飞行海拔高度超过120米
                            home_longitude            double,       --  无人机返航点经度
                            home_latitude             double,       --  无人机返航点纬度
                            no_fly_zone_id            int,          --  无人机是否飞入禁飞区0是未飞入
                            user_phone                string        --  持有者手机号
) with (
      'connector' = 'kafka',
      'topic' = 'uav_merge_target',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'uav_merge_target1',
      -- 'key.format' = 'json',
      -- 'key.fields' = 'id',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );

-- ********************************** doris数据表读取 ***********************************


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
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- -- 写入中转的kafka-topic , 先写入在融合生成id，但是现在不用融合，直接使用自己的id，所以这里可以不用
-- create table uav_merge_target(
--   id                            string  comment 'id',
--   device_id                     string  comment '数据来源的设备id',
--   acquire_time          		string  comment '采集时间',
--   src_code              		string  comment '数据类型 RID、AOA、RADAR',
--   src_pk                        string  comment '自己网站的目标id',
--   device_name                   string  comment '设备名称',
--   rid_devid             		string  comment 'rid设备的id-飞机上报的',
--   msgtype               		bigint  comment '消息类型',
--   recvtype              		string  comment '无人机数据类型,示例:2.4G',
--   mac                   		string  comment 'rid设备MAC地址',
--   rssi                  		bigint  comment '信号强度',
--   longitude             		double  comment '探测到的无人机经度',
--   latitude              		double  comment '探测到的无人机纬度',
--   location_alit         		double  comment '气压高度',
--   ew                    		double  comment 'rid航迹角,aoa监测站识别的目标方向角',
--   speed_h               		double  comment '水平速度',
--   speed_v               		double  comment '垂直速度',
--   height                		double  comment '距地高度',
--   height_type           		double  comment '高度类型',
--   control_station_longitude  	double  comment '控制无人机人员经度',
--   control_station_latitude   	double  comment '控制无人机人员纬度',
--   control_station_height 	   	double 	comment '控制站高度',
--   target_name             		string  comment 'aoa-目标名称',
--   altitude                		double  comment 'aoa-无人机所在海拔高度',
--   distance_from_station   		double  comment 'aoa-无人机距离监测站的距离',
--   speed_ms                  	double  comment 'aoa-无人机飞行速度 (m/s)',
--   target_frequency_khz    		double  comment 'aoa-目标使用频率 (k_hz)',
--   target_bandwidth_khz		    double  comment 'aoa-目标带宽 (k_hz)',
--   target_signal_strength_db		double  comment 'aoa-目标信号强度 (d_b)',
--   target_type                   bigint,
--   target_list_status            int,          --  名单状态1是白名单 2是黑名单 4是民众注册无人机
--   target_list_type              int,          --  名单类型3是政务无人机 2是低空经济无人机 5是重点关注
--   user_company_name             string,       --  持有单位
--   user_full_name                string,       --  持有者姓名
--   target_direction              double,       --  无人机方位（对于发现设备）
--   target_area_code              string,       --  无人机飞行地区行政编码
--   target_registered             int,          --  无人机是否报备1是已报备
--   target_fly_report_status      int,          --  无人机报备状态1是未报备 2 是已报备 3 超出报备区域范围 4 是不符合报备飞行时间5 是超出报备区域范围 6是 飞行海拔高度超过120米
--   home_longitude                double,       --  无人机返航点经度
--   home_latitude                 double,       --  无人机返航点纬度
--   no_fly_zone_id                int,          --  无人机是否飞入禁飞区0是未飞入
--   user_phone                    string,        --  持有者手机号
--   update_time                	string  comment '更新时间'
-- ) with (
--     'connector' = 'kafka',
--     'topic' = 'uav_merge_target',
--     'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
--     'properties.group.id' = 'uav_merge_target1',
--     'key.format' = 'json',
--     'key.fields' = 'id',
--     'format' = 'json'
--   );


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

    message.`data`.basic_info.basic_uaid                as uav_id,
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
    message.`data`.system_info.sys_classification       as sys_classification,     -- 保留字
    message.`data`.system_info.sys_area_count           as sys_area_count,         -- 运行区域计数
    message.`data`.system_info.sys_category             as sys_category,           -- 保留字
    message.`data`.system_info.sys_location_type        as sys_location_type,      -- 保留字
    message.`data`.system_info.sys_area_rad             as sys_area_rad,           -- 运行区域半径

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
    message.`data`.targets                              as targets,
    PROCTIME() as proctime
from iot_device_message_kafka_01
where message.`timestamp` is not null
  -- 9sMhTTcOrbv(AOA)  iiCC6Y7Qhmz(RID)    xjWO7NdIOYs(天朗雷达) aby554pqic0(雷莫雷达)
  and productKey in('9sMhTTcOrbv','iiCC6Y7Qhmz','xjWO7NdIOYs','aby554pqic0')
  and message.`method` in('event.ridMessage.info','event.aoaMessage.info','event.targetInfo.info');


-- 关联设备的经纬度位置和设备名称
create view temp02 as
select
    t1.*,
    t2.longitude as device_longitude,
    t2.latitude  as device_latitude,
    t2.name as device_name
from temp01 as t1
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 设备表 关联设备位置
                   on t1.device_id = t2.device_id;


-- 筛选雷达数据并整合字段
create view temp03 as
select
    t1.acquire_timestamp,
    t1.product_key,
    t1.device_id,
    t1.device_name,
    t1.acquire_time,
    t1.`method`,
    'RADAR'               as src_code,
    t2.targetId           as src_pk,
    cast(null as varchar) as rid_devid,
    cast(null as bigint)  as msgtype,
    cast(null as varchar) as recvtype,
    cast(null as varchar) as mac,
    cast(null as bigint)  as rssi,
    t2.targetLongitude    as longitude,
    t2.targetLatitude     as latitude,

    cast(null as double)  as location_alit,
    cast(null as double)  as ew,
    cast(null as double)  as speed_h,
    cast(null as double)  as speed_v,
    cast(null as double)  as height,
    cast(null as double)  as height_type,
    cast(null as double)  as control_station_longitude,
    cast(null as double)  as control_station_latitude,
    cast(null as double)  as control_station_height,
    cast(null as varchar) as target_name,
    t2.targetAltitude     as altitude,
    cast(null as double)  as distance_from_station,
    t2.speed              as speed_ms,
    cast(null as double)  as target_frequency_khz,
    cast(null as double)  as target_bandwidth_khz,
    cast(null as double)  as target_signal_strength_db,
    targetType as target_type,
    cast(null as int)               as target_list_status,
    cast(null as int)               as target_list_type,
    cast(null as varchar)           as user_company_name,
    cast(null as varchar)           as user_full_name,
    cast(null as double)            as target_direction,
    cast(null as varchar)           as target_area_code,
    cast(null as int)               as target_registered,
    cast(null as int)               as target_fly_report_status,
    cast(null as double)            as home_longitude,
    cast(null as double)            as home_latitude,
    cast(null as int)               as no_fly_zone_id,
    cast(null as varchar)           as user_phone
from (
         select
             *
         from temp02
         where `method` = 'event.targetInfo.info'
     ) as t1
         cross join unnest (targets) as t2 (
                                            targetId         ,
                                            xDistance        ,
                                            yDistance        ,
                                            speed            ,
                                            targetPitch      ,
                                            targetAltitude   ,
                                            status           ,
                                            targetLongitude  ,
                                            targetLatitude   ,
                                            distance         ,
                                            targetYaw        ,
                                            utc_time         ,
                                            tracked_times    ,
                                            loss_times       ,
                                            targetType       ,

    -- 天朗雷达
                                            RCS              ,
                                            radialDistance   ,
                                            targetState      ,
                                            `timestamp`      ,
                                            timestampBase    ,
                                            uploadMode
    )
where
  -- t2.targetType = 9
    t2.targetLongitude is not null
  and t2.targetLatitude is not null
  and t2.targetId is not null;


-- 筛选rid、aoa数据并整合字段
create view temp04 as
select
    acquire_timestamp,
    product_key,
    device_id,
    device_name,
    acquire_time,
    `method`,
    if(product_key = '9sMhTTcOrbv','AOA','RID') as src_code,
    coalesce(uav_id,aoa_uav_id) as src_pk,
    rid_devid,
    msgtype,
    recvtype,
    mac,
    rssi,
    uav_longitude as longitude,
    uav_latitude as latitude,
    coalesce(location_alit,pressure_altitude) as location_alit, -- location_alit(RID气压高度)  pressure_altitude(AOA气压高度)
    coalesce(ew,direction_angle) as ew,                         -- ew(RID航迹角) direction_angle(AOA监测站识别的目标方向角)
    coalesce(speed_h,speed_ms) as speed_h,                      -- speed_h(RID水平速度) speed_ms（AOA无人机飞行速度）
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
    cast(null as int) as target_type,
    cast(null as int)               as target_list_status,
    cast(null as int)               as target_list_type,
    cast(null as varchar)           as user_company_name,
    cast(null as varchar)           as user_full_name,
    cast(null as double)            as target_direction,
    cast(null as varchar)           as target_area_code,
    cast(null as int)               as target_registered,
    cast(null as int)               as target_fly_report_status,
    cast(null as double)            as home_longitude,
    cast(null as double)            as home_latitude,
    cast(null as int)               as no_fly_zone_id,
    cast(null as varchar)           as user_phone
from temp02
where `method` in('event.ridMessage.info','event.aoaMessage.info')
  and uav_longitude between -180 and 180
  and uav_latitude between -90 and 90
  and uav_longitude <> 0
  and uav_latitude <> 0
  and control_station_longitude between -180 and 180
  and control_station_latitude between -90 and 90
  -- and control_station_longitude <>0
  -- and control_station_latitude <> 0
  and distance_udf(uav_latitude,uav_longitude,device_latitude,device_longitude) <= 15;



-- -- get_id( 原网站的id、经度、纬度、时间戳毫秒,数据来源类型) as res_str,
-- -- 雷达、RID、AOA数据union一起,并且调用函数获取融合的id
-- create view temp05 as
-- select
--   *,
--   concat(src_pk,';',src_code,';1;1;',cast(longitude as varchar),';',cast(latitude as varchar)) as id
--   -- get_id(src_pk,longitude,latitude,acquire_timestamp,src_code) as id
-- from rid_m30_aoa as t1;



-----------------------

-- 数据写入

-----------------------



begin statement set;

-- temp03（雷达数据）， temp04（RID，AOA数据）
insert into rid_m30_aoa

select
    concat(src_pk,';',src_code,';1;1;',cast(longitude as varchar),';',cast(latitude as varchar)) as id,
    *
from (
         select * from temp03
         union all
         select * from temp04
     ) as t1;




-- -- 融合数据写入中转的kafka-topic
-- insert into uav_merge_target
-- select
--   id,
--   device_id,
--   acquire_time                   ,
--   src_code                       ,
--   src_pk                	     ,
--   device_name,
--   rid_devid                      ,
--   msgtype                        ,
--   recvtype                       ,
--   mac                            ,
--   rssi                           ,
--   longitude                      ,
--   latitude                       ,
--   location_alit                  ,
--   ew                             ,
--   speed_h                        ,
--   speed_v                        ,
--   height                         ,
--   height_type                    ,
--   control_station_longitude      ,
--   control_station_latitude       ,
--   control_station_height 	     ,
--   target_name             	     ,
--   altitude                	     ,
--   distance_from_station   	     ,
--   speed_ms                       ,
--   target_frequency_khz    	     ,
--   target_bandwidth_khz		     ,
--   target_signal_strength_db	     ,
--   target_type,

--   target_list_status,
--   target_list_type,
--   user_company_name,
--   user_full_name,
--   target_direction,
--   target_area_code,
--   target_registered,
--   target_fly_report_status,
--   home_longitude,
--   home_latitude,
--   no_fly_zone_id,
--   user_phone,
--   from_unixtime(unix_timestamp()) as update_time
-- from temp05;


end;



