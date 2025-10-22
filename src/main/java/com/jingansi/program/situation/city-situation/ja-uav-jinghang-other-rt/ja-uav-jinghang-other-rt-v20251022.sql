--********************************************************************--
-- author:      write your name here
-- create time: 2025/10/22 15:49:44
-- description: write your description here
-- version: ja-uav-jinghang-other-rt-v20251022 警航的阵地感知 & 三吉 无人机数据接入
--********************************************************************--
set 'pipeline.name' = 'ja-uav-jinghang-other-rt';


SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '60000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '4';
set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';

SET 'execution.checkpointing.interval' = '120000';

SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-uav-jinghang-other-rt';



 -----------------------

 -- 数据结构来源

 -----------------------

-- 设备检测数据上报 （Source：kafka）
create table jh_uav_data_kafka (
    `data`   array<
        row (
        id                       string,
        deviceId                 double,
        device   row(
        actionScope          double,
        agentId              string,
        areaCode             string,
        disable              int,
        disposalScope        int,
        id                   double,
        longitude            string,
        latitude             string,
        lightScope           int,
        name                 string,
        online               int,
        radarScope           int,
        sentinelScope        int,
        spectrumScope        int,
        supportHeartbeat     int,
        type                 int
        ),
        subDeviceId                int,
        subDevice   row (
        calibratedDirection  int,
        `delete`             int,
        deviceId             double,
        disable              int,
        frequencies          string,
        gmtCreate            string,
        height               double,
        id                   double,
        model                string,
        name                 string,
        online               int,
        remark               string,
        `running`            int,
        sn                   string,
        spec                 int,
        supportHeartbeat     int,
        type                 int,
        uniqueId             string
        ),
        targetSn                     string,
        target    row(
        listStatus             int,
        listType               int,
        sn                     string,
        uavUser row(
        companyName        string,
        fullName           string,
        phone              string
        )
        ),
        deviceDetectType           int,
        deviceLongitude            string,
        deviceLatitude             string,
        targetLongitude            string,
        targetLatitude             string,
        targetFrequency            string,
        targetName                 string,
        targetHeight               double,
        targetDistance             double,
        targetSpeed                double,
        targetAltitude             double,
        targetDirection            double,
        targetRollAngle            double,
        targetPitchAngle           double,
        targetYawAngle             double,
        targetElectricQuantity     double,
        targetAreaCode             string,
        targetAddress              string,
        targetFlyTime              double,
        targetRegistered           int,
        targetFlyReported          double,
        targetFlyReportStatus      int,
        targetIllegalFly           double,
        operatorLongitude          string,
        operatorLatitude           string,
        operatorAddress            string,
        homeLongitude              string,
        homeLatitude               string,
        homeAddress                string,
        noFlyZoneId                int,
        gmtDetect                  string
        )
        >

) WITH (
      'connector' = 'kafka',
      'topic' = 'jh-uav-data',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'jh-uav-data1',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0', -- 1747102253000
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );




-- 三吉数据上报 （Source：kafka）
create table sanji_uav_data_kafka (
                                      uavId        string, -- 无人机sn
                                      uavLat       double, -- 无人机纬度
                                      uavLon       double, -- 无人机经度
                                      velocity     double, -- 速度 m/s
                                      yaw          double, -- 水平角度(正北为0度，顺时针360度)
                                      uavAlt       double, -- 无人机海拔
                                      pilotLat     double, -- 飞手纬度
                                      pilotLon     double, -- 飞手经度
                                      type         int,    -- 2代表通航数据、4代表无人机数据
                                      createTime   bigint  -- 最新时间 毫秒
) WITH (
      'connector' = 'kafka',
      'topic' = 'sanji-uav-data',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'sanji-uav-data1',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0', -- 1747102253000
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 整合rid、aoa、雷达的数据 写入
-- 在进行读取做数据融合生成id
create table rid_m30_aoa(
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
      'topic' = 'rid-m30-aoa',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'uav_merge_target22',
      -- 'key.format' = 'json',
      -- 'key.fields' = 'id',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-----------------------

-- 数据处理

-----------------------


-- 展开数组筛选数据，整理字段
create view temp_01 as
select
        UNIX_TIMESTAMP(gmtDetect,'yyyy-MM-dd HH:mm:ss') * 1000 as  acquire_timestamp,
        cast(null as varchar)              as product_key,
        id                                 as device_id,
        cast(null as varchar)              as device_name,
        gmtDetect                          as acquire_time,
        cast(null as varchar)              as `method`,
        '阵地'                              as src_code,
        targetSn                           as src_pk,
        cast(null as varchar)              as rid_devid,
        cast(null as bigint)               as msgtype,
        cast(null as varchar)              as recvtype,
        cast(null as varchar)              as mac,
        cast(null as bigint)               as rssi,
        cast(targetLongitude as double)    as longitude,
        cast(targetLatitude as double)     as latitude,
        cast(null as double)               as location_alit,
        targetYawAngle                     as ew,
        targetSpeed                        as speed_h,
        cast(null as double)               as speed_v,
        targetHeight                       as height,
        cast(null as double)               as height_type,
        cast(operatorLongitude as double)  as control_station_longitude,
        cast(operatorLatitude as double)   as control_station_latitude,
        cast(null as double)               as control_station_height,
        targetName                         as target_name,
        targetAltitude                     as altitude,
        targetDistance                     as distance_from_station,
        targetSpeed                        as speed_ms,
        if(targetFrequency = '',cast(null as double),cast(targetFrequency as double)) as target_frequency_khz,
        cast(null as double)               as target_bandwidth_khz,
        cast(null as double)               as target_signal_strength_db,
        cast(null as int)                  as target_type,
        target.listStatus                  as target_list_status,          --  名单状态1是白名单 2是黑名单 4是民众注册无人机
        target.listType                    as target_list_type,            --  名单类型3是政务无人机 2是低空经济无人机 5是重点关注
        target.uavUser.companyName         as user_company_name,           --  持有单位
        target.uavUser.fullName            as user_full_name,              --  持有者姓名
        target.uavUser.phone               as user_phone,                  -- 持有者手机号
        targetDirection                    as target_direction,            --  无人机方位（对于发现设备）
        targetAreaCode                     as target_area_code,            --  无人机飞行地区行政编码
        targetRegistered                   as target_registered,           --  无人机是否报备1是已报备
        targetFlyReportStatus              as target_fly_report_status,    --  无人机报备状态1是未报备 2 是已报备 3 超出报备区域范围 4 是不符合报备飞行时间5 是超出报备区域范围 6是 飞行海拔高度超过120米
        cast(homeLongitude as double)      as home_longitude,              --  无人机返航点经度
        cast(homeLatitude as double)       as home_latitude,               --  无人机返航点纬度
        noFlyZoneId                        as no_fly_zone_id              --  无人机是否飞入禁飞区0是未飞入
from jh_uav_data_kafka as t1
         cross join unnest (`data`) as t2 (
                                           id,
                                           deviceId,
                                           device,
                                           subDeviceId,
                                           subDevice,
                                           targetSn,
                                           target,
                                           deviceDetectType           ,
                                           deviceLongitude            ,
                                           deviceLatitude             ,
                                           targetLongitude            ,
                                           targetLatitude             ,
                                           targetFrequency            ,
                                           targetName                 ,
                                           targetHeight               ,
                                           targetDistance             ,
                                           targetSpeed                ,
                                           targetAltitude             ,
                                           targetDirection            ,
                                           targetRollAngle            ,
                                           targetPitchAngle           ,
                                           targetYawAngle             ,
                                           targetElectricQuantity     ,
                                           targetAreaCode             ,
                                           targetAddress              ,
                                           targetFlyTime              ,
                                           targetRegistered           ,
                                           targetFlyReported          ,
                                           targetFlyReportStatus      ,
                                           targetIllegalFly           ,
                                           operatorLongitude          ,
                                           operatorLatitude           ,
                                           operatorAddress            ,
                                           homeLongitude              ,
                                           homeLatitude               ,
                                           homeAddress                ,
                                           noFlyZoneId                ,
                                           gmtDetect
    )
where t2.targetSn is not null
  and t2.targetSn <> ''
  and t2.targetLongitude is not null
  and t2.targetLongitude <> ''
  and t2.targetLatitude is not null
  and t2.targetLatitude <> ''
  and t2.gmtDetect is not null;



-- 三吉无人机数据接入
create view sanji_temp_01 as
select
    createTime                      as acquire_timestamp,
    cast(null as varchar)           as product_key,
    cast(null as varchar)           as device_id,
    cast(null as varchar)           as device_name,
    from_unixtime(createTime/1000,'yyyy-MM-dd HH:mm:ss') as acquire_time,
    cast(null as varchar)           as `method`,
    '1'                             as src_code,
    uavId                           as src_pk,
    cast(null as varchar)           as rid_devid,
    cast(null as bigint)            as msgtype,
    cast(null as varchar)           as recvtype,
    cast(null as varchar)           as mac,
    cast(null as bigint)            as rssi,
    uavLon                          as longitude,
    uavLat                          as latitude,
    cast(null as double)            as location_alit,
    yaw                             as ew,
    velocity                        as speed_h,
    cast(null as double)            as speed_v,
    cast(null as double)            as height,
    cast(null as double)            as height_type,
    pilotLon                        as control_station_longitude,
    pilotLat                        as control_station_latitude,
    cast(null as double)            as control_station_height,
    cast(null as varchar)           as target_name,
    uavAlt                          as altitude,
    cast(null as double)            as distance_from_station,
    velocity                        as speed_ms,
    cast(null as double)            as target_frequency_khz,
    cast(null as double)            as target_bandwidth_khz,
    cast(null as double)            as target_signal_strength_db,
    cast(null as int)               as target_type,
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
from sanji_uav_data_kafka
where uavId is not null
  and uavId <> ''
  and uavLon is not null
  and uavLat is not null
  and createTime is not null;



-----------------------

-- 数据写入

-----------------------

begin statement set;


insert into rid_m30_aoa
select
    acquire_timestamp         ,
    product_key               ,
    device_id                 ,
    device_name               ,
    acquire_time              ,
    `method`                  ,
    src_code                  ,
    src_pk                    ,
    rid_devid                 ,
    msgtype                   ,
    recvtype                  ,
    mac                       ,
    rssi                      ,
    longitude                 ,
    latitude                  ,
    location_alit             ,
    ew                        ,
    speed_h                   ,
    speed_v                   ,
    height                    ,
    height_type               ,
    control_station_longitude ,
    control_station_latitude  ,
    control_station_height    ,
    target_name               ,
    altitude                  ,
    distance_from_station     ,
    speed_ms                  ,
    target_frequency_khz      ,
    target_bandwidth_khz      ,
    target_signal_strength_db ,
    target_type               ,
    target_list_status        ,
    target_list_type          ,
    user_company_name         ,
    user_full_name            ,
    target_direction          ,
    target_area_code          ,
    target_registered         ,
    target_fly_report_status  ,
    home_longitude            ,
    home_latitude             ,
    no_fly_zone_id            ,
    user_phone
from temp_01;



insert into rid_m30_aoa
select
    acquire_timestamp         ,
    product_key               ,
    device_id                 ,
    device_name               ,
    acquire_time              ,
    `method`                  ,
    src_code                  ,
    src_pk                    ,
    rid_devid                 ,
    msgtype                   ,
    recvtype                  ,
    mac                       ,
    rssi                      ,
    longitude                 ,
    latitude                  ,
    location_alit             ,
    ew                        ,
    speed_h                   ,
    speed_v                   ,
    height                    ,
    height_type               ,
    control_station_longitude ,
    control_station_latitude  ,
    control_station_height    ,
    target_name               ,
    altitude                  ,
    distance_from_station     ,
    speed_ms                  ,
    target_frequency_khz      ,
    target_bandwidth_khz      ,
    target_signal_strength_db ,
    target_type               ,
    target_list_status        ,
    target_list_type          ,
    user_company_name         ,
    user_full_name            ,
    target_direction          ,
    target_area_code          ,
    target_registered         ,
    target_fly_report_status  ,
    home_longitude            ,
    home_latitude             ,
    no_fly_zone_id            ,
    user_phone
from sanji_temp_01;


end;





