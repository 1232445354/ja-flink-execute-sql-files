-- ********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2023/3/21 14:06:19
-- description: 旌旗3.0设备接入日志、设备检测数据状态、轨迹等
-- version: 3.0.3.240411
-- fix:新增望楼3.0
-- ********************************************************************--


set 'pipeline.name' = 'ja-chingchi-icos3.0-rt-new-xt';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '300000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-chingchi-icos3.0-rt-new-xt' ;


-- 设备检测数据上报（Source：kafka）
drop table if exists iot_device_message_kafka_01;
create table iot_device_message_kafka_01 (
                                             productKey    string     comment '产品编码',
                                             deviceId      string     comment '设备id',
                                             type          string     comment '类型',
                                             version       string     comment '版本',
                                             `timestamp`   bigint     comment '时间戳毫秒',
                                             tid           string     comment '当前请求的事务唯一ID',
                                             bid           string     comment '长连接整个业务的ID',
                                             `method`      string     comment '服务&事件标识',

    -- 拍照数据
                                             `data`  row(
                                                 pictureUrl       string   , -- 拍照数据上报-图片url
                                                 width            int      , -- 拍照数据上报-宽度
                                                 height           int        -- 拍照数据上报-高度
                                                 ),

                                             message  row(
                                                 tid                     string    , -- 当前请求的事务唯一ID
                                                 bid                     string    , -- 长连接整个业务的ID
                                                 version                 string    , -- 版本
                                                 `timestamp`             bigint    , -- 时间戳
                                                 `method`                string    , -- 服务&事件标识
                                                 productKey              string    , -- 产品编码
                                                 deviceId                string    , -- 设备编码
                                                 `data` row(
                                                 -- 设备日志和轨迹
                                                 photoUrl            string    , -- 媒体上报的拍照图片url
                                                 longitude           double    , -- 机库子设备-无人机,经度
                                                 latitude            double    , -- 机库子设备-无人机，纬度
                                                 height              double    , -- 机库子设备-无人机，高度
                                                 attitudeHead        double    , -- 机库子设备-无人机，水平朝向
                                                 verticalSpeed       double    , -- 机库子设备-无人机，垂直速度
                                                 horizontalSpeed     double    , -- 机库子设备-无人机，水平速度
                                                 electricity         double    , -- 无人机，电量
                                                 gimbalHead          double    , -- 无人机，云台水平朝向
                                                 battery row (
                                                 capacityPercent  double  -- 机库子设备-无人机，电量
                                                 ),
                                                 deviceName         string,  -- 执法仪名称

                                                 -- 雷达检测数据
                                                 targets array<
                                                 row(
                                                 targetId         bigint   ,-- -目标id
                                                 targetAltitude   double   ,-- 目标海拔高度
                                                 status           string   ,-- 0 目标跟踪 1 目标丢失 2 跟踪终止
                                                 targetLongitude  double   ,-- 目标经度
                                                 targetLatitude   double   , -- 目标纬度
                                                 distance         double   , -- 上报距离
                                                 sourceType       string   ,  -- 数据来源类型RADAR
                                                 targetType       string      -- 目标类型
                                                 )
                                                 >
                                                 )
                                                 )
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'iot-device-message-rt-2',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1719925824000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 媒体拍照数据入库（Sink：mysql）
drop table if exists device_media_datasource;
create table device_media_datasource (
                                         device_id                      string        comment '设备编码',
                                         source_id                      string        comment '来源id/应用id',
                                         source_name                    string        comment '来源名称/应用名称',
                                         type                           string        comment 'PICTURE/HISTORY_VIDEO',
                                         start_time                     string        comment '开始时间',
                                         end_time                       string        comment '结束时间',
                                         url                            string        comment '原图/视频 url',
                                         width                          int           comment '图片宽度',
                                         height                         int           comment '图片高度',
                                         bid                            string        comment '业务id',
                                         tid                            string        comment '',
                                         b_type                         string        comment '业务类型',
                                         extends                        string        comment 'json字符串 {}',
                                         gmt_create                     string        comment '',
                                         gmt_create_by                  string        comment '',
                                         gmt_modified_by                string        comment '',
                                         PRIMARY KEY (device_id,start_time,url) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8', -- ECS环境
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',  -- 105环境
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device_media_datasource',
      'sink.buffer-flush.interval' = '1s',
      'sink.buffer-flush.max-rows' = '20'
      );



-- 设备(雷达)检测目标全量数据入库（Sink：doris）
drop table  if exists dwd_radar_target_all_rt;
create table dwd_radar_target_all_rt(
                                        device_id                  string     , -- '雷达设备id',
                                        target_id                  bigint     , -- '目标id',
                                        parent_id                  string     , -- 父设备的id,也就是望楼id
                                        acquire_timestamp_format   string     , -- '上游程序上报时间戳-时间戳格式化',
                                        acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                        source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                        device_name                string     , -- 设备名称
                                        source                     string     , -- 数据检测的来源[{deviceName,targetId,type}]
                                        object_label               string     , -- 目标类型
                                        status                     string     , -- '0 目标跟踪 1 目标丢失 2 跟踪终止',
                                        target_altitude            double     , -- '目标海拔高度',
                                        longitude                  double     , -- '目标经度',
                                        latitude                   double     , -- '目标维度',
                                        distance                   double     , -- 距离，新雷达的距离，没有了x距离和y距离
                                        tid                        string     , -- 当前请求的事务唯一ID
                                        bid                        string     , -- 长连接整个业务的ID
                                        `method`                   string     , -- 服务&事件标识
                                        product_key                string     , -- 产品编码
                                        version                    string     , -- 版本
                                        create_by                  string     , -- 创建人
                                        update_time                string      -- 数据入库时间
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dwd_radar_target_all_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='10s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-- 设备（雷达）检测目标状态数据入库（Sink：doris）
drop table  if exists dws_radar_target_status_rt;
create table dws_radar_target_status_rt(
                                           device_id                  string     , -- '设备id',
                                           target_id                  bigint     , -- '目标id',
                                           parent_id                  string     , -- 父设备的id,也就是望楼id
                                           acquire_timestamp_format   string     , -- '上游程序上报时间戳-时间戳格式化',
                                           acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                           source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                           device_name                string     , -- 设备名称
                                           source                     string     , -- 数据检测的来源[{deviceName,targetId,type}]
                                           object_label               string     , -- 目标类型
                                           status                     string     , -- '0 目标跟踪 1 目标丢失 2 跟踪终止',
                                           target_altitude            double     , -- '目标海拔高度',
                                           longitude                  double     , -- '目标经度',
                                           latitude                   double     , -- '目标维度',
                                           distance                   double     , -- 距离，新雷达的距离，没有了x距离和y距离
                                           tid                        string     , -- 当前请求的事务唯一ID
                                           bid                        string     , -- 长连接整个业务的ID
                                           `method`                   string     , -- 服务&事件标识
                                           product_key                string     , -- 产品编码
                                           version                    string     , -- 版本
                                           create_by                  string     , -- 创建人
                                           update_time                string      -- 数据入库时间
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dws_radar_target_status_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='2s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 设备执法仪、无人机轨迹数据（Sink：doris）
drop table if exists dwd_device_track_rt;
create table dwd_device_track_rt (
                                     device_id                 string              comment '上报的设备id',
                                     acquire_timestamp_format  string              comment '格式化时间',
                                     acquire_timestamp         bigint              comment '上报时间戳',
                                     lng_02                    DECIMAL(30,18)      comment '经度—高德坐标系、火星坐标系',
                                     lat_02                    DECIMAL(30,18)      comment '纬度—高德坐标系、火星坐标系',
                                     username                  string              comment '设备用户',
                                     group_id                  string              comment '组织id',
                                     product_key               string              comment '产品key',
                                     tid                       string              comment 'tid',
                                     bid                       string              comment 'bid',
                                     device_name               string              comment '设备名称',
                                     create_by                 string              comment '创建人',
                                     update_time               string              comment '更新插入时间（数据入库时间'
    -- device_type               string      comment '上报的设备类型',
    -- lng_84                    string      comment '经度—84坐标系',
    -- lat_84                    string      comment '纬度—84坐标系',
    -- rectify_lng_lat           string      comment '高德坐标算法纠偏后经纬度',
    -- start_time                string      comment '开始时间（开窗）',
    -- end_time                  string      comment '结束时间（开窗）',
    -- type                      string      comment 'POSITION 位置',
    -- location_type             string      comment '位置类型',
    -- record_id                 string      comment '行为id',
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dwd_device_track_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='5s'
     );



-- 建立映射mysql的表（为了查询用户名称）
drop table if exists iot_device;
create table iot_device (
                            id	             int,    -- 自增id
                            parent_id        string, -- 父设备的id,也就是望楼id
                            device_id	     string, -- 设备id
                            device_name      string, -- 设备名称
                            gmt_create_by	 string, -- 创建用户名
                            primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'iot_device',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '3'
     );


-- 建立映射mysql的表（为了查询组织id）
drop table if exists users;
create table users (
                       user_id	int,
                       username	string,
                       password	string,
                       name	    string,
                       group_id	string,
                       primary key (user_id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja-4a?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'users',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '3'
     );



-----------------------

-- 数据处理

-----------------------


-- kafka来源的所有数据解析
drop view if exists tmp_source_kafka_01;
create view tmp_source_kafka_01 as
select
    type,
    coalesce(productKey,message.productKey)   as product_key, -- message_product_key
    coalesce(deviceId,message.deviceId)       as device_id,   -- message_device_id
    coalesce(version,message.version)         as version, -- message_version
    coalesce(`timestamp`,message.`timestamp`) as acquire_timestamp, -- message_acquire_timestamp
    coalesce(tid,message.tid)                 as tid, -- message_tid
    coalesce(bid,message.bid)                 as bid, -- message_bid
    coalesce(`method`,message.`method`)       as `method`, -- message_method

    -- 截图数据
    `data`.pictureUrl as picture_url,
    `data`.width  as width,
    `data`.height as height,

    -- 执法仪轨迹
    message.`data`.longitude   as longitude,
    message.`data`.latitude    as latitude,
    message.`data`.deviceName  as device_name,

    -- 雷达检测目标数据
    message.`data`.targets     as targets,

    -- 手动拍照数据
    message.`data`.photoUrl    as photo_url,

    PROCTIME()  as proctime
from iot_device_message_kafka_01;



-- 关联设备表取出设备名称,取出父设备望楼id，数据来源都是子设备id
drop view if exists tmp_source_kafka_02;
create view tmp_source_kafka_02 as
select
    t1.*,
    t2.gmt_create_by as username,
    t2.device_name as device_name_join,
    t2.parent_id,
    t3.group_id
from tmp_source_kafka_01 as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.device_id = t2.device_id

         left join users FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t2.gmt_create_by = t3.username
where t1.device_id is not null
  and t1.acquire_timestamp > 1704096000000;



-- 设备(雷达)检测数据筛选处理
drop view if exists tmp_source_kafka_03;
create view tmp_source_kafka_03 as
select
    tid,
    bid,
    `method`,
    product_key,
    device_id,
    version,
    acquire_timestamp,
    targets,
    device_name_join,
    parent_id,
    proctime
from tmp_source_kafka_02
where `method` = 'event.targetInfo.info';



-- 设备检测数据（雷达）数据进一步解析数组
drop view if exists tmp_source_kafka_04;
create view tmp_source_kafka_04 as
select
    t1.tid,
    t1.bid,
    t1.`method`,
    t1.product_key,
    t1.device_id,
    t1.version,
    -- TO_TIMESTAMP_LTZ(t1.acquire_timestamp,3) as acquire_timestamp_format,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    t1.acquire_timestamp as acquire_timestamp,
    t1.device_name_join as device_name,
    if(t1.parent_id is null or t1.parent_id = '',t1.device_id, t1.parent_id) as parent_id,
    t2.targetId          as target_id,
    t2.targetAltitude    as target_altitude,
    t2.status            as status,
    t2.targetLongitude   as longitude,
    t2.targetLatitude    as latitude,
    t2.distance,
    t2.sourceType as source_type,
    t2.targetType as object_label,
    concat('[{',
           concat('"deviceName":"',if(t1.device_name_join is not null,t1.device_name_join,''),'",'),
           concat('"targetId":"',coalesce(cast(t2.targetId as varchar),''),'",'),
           concat('"type":"',t2.sourceType,'"}]')
        ) as source

    -- t2.xDistance         as x_distance,
    -- t2.yDistance         as y_distance,
    -- t2.speed             as speed,
    -- t2.targetPitch       as target_pitch,
    -- t2.utc_time,
    -- t2.tracked_times,
    -- t2.targetYaw         as target_yaw,
    -- t2.loss_times,
from tmp_source_kafka_03 as t1
         cross join unnest (targets) as t2 (
                                            targetId         ,
                                            targetAltitude   ,
                                            status           ,
                                            targetLongitude  ,
                                            targetLatitude   ,
                                            distance         ,
                                            sourceType       ,
                                            targetType
    );




-- 执法仪、无人机轨迹数据处理
drop view if exists tmp_track_01;
create view tmp_track_01 as
select
    device_id,
    product_key,
    type,
    tid,
    bid,
    acquire_timestamp,
    device_name,
    longitude,
    latitude,
    username,
    group_id
from tmp_source_kafka_02      -- QptZJHOd1KD :执法仪，zyrFih3kept,00000000002:无人机
where product_key in('QptZJHOd1KD','zyrFih3kept','00000000002');


-----------------------

-- 数据插入

-----------------------


begin statement set;

-- 执法仪轨迹数据入库
insert into dwd_device_track_rt
select
    device_id               ,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format  ,
    acquire_timestamp         ,
    longitude   as lng_02     ,
    latitude    as lat_02     ,
    username                  ,
    group_id                  ,
    product_key               ,
    tid                       ,
    bid                       ,
    device_name               ,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_track_01;


-- 数据手动截图数据入库
insert into device_media_datasource
select
    device_id            as device_id,
    'SCREENSHOT'         as source_id,
    cast(null as string) as source_name,
    'PICTURE' as type,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as start_time,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as end_time,
    concat('/',picture_url)    as url,
    width,
    height,
    bid,
    tid,
    cast(null as varchar) as b_type,
    '{}'                  as extends,
    from_unixtime(unix_timestamp()) as gmt_create,
    'ja-flink' as gmt_create_by,
    'ja-flink' as gmt_modified_by
from tmp_source_kafka_01
where acquire_timestamp is not null
  and picture_url is not null
  and `method` = 'platform.capture.post';



-- 驾驶舱遥控器拍照
insert into device_media_datasource
select
    device_id,
    'PHOTOGRAPH'                 as source_id,
    cast(null as string)         as source_name,
    'PICTURE'  as type,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as start_time,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as end_time,
    photo_url                    as url,
    0 as width,
    0 as height,
    bid,
    tid,
    'WAYLINE_TASK' as b_type,
    '{}'           as extends,
    from_unixtime(unix_timestamp()) as gmt_create,
    'ja-flink' as gmt_create_by,
    'ja-flink' as gmt_modified_by
from tmp_source_kafka_01
where acquire_timestamp is not null
  and `method` = 'event.mediaFileUpload.info'
  and photo_url is not null;



-- 设备(雷达)检测全量数据入库doris
insert into dwd_radar_target_all_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    source_type as source_type,
    device_name,
    source,
    object_label,
    status,
    target_altitude,
    longitude,
    latitude,
    distance,
    tid,
    bid,
    `method`,
    product_key,
    version,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_04;


-- 设备(雷达)检测状态数据入库doris
insert into dws_radar_target_status_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    source_type as source_type,
    device_name,
    source,
    object_label,
    status,
    target_altitude,
    longitude,
    latitude,
    distance,
    tid,
    bid,
    `method`,
    product_key,
    version,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_04;

end;


