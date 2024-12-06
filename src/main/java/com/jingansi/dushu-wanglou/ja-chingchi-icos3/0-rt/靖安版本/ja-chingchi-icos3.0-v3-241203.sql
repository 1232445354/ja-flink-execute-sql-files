--********************************************************************--
-- author:      write your name here
-- create time: 2024/12/2 19:42:10
-- description: 截图拍照、属性、轨迹、雷达、振动仪
--version:ja-chingchi-icos3.0-v3-241203
--********************************************************************--

set 'pipeline.name' = 'ja-chingchi-icos3.0-v3-241203';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '4';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-chingchi-icos3.0-v3-241203' ;



-- 设备检测数据上报 （Source：kafka）
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

    -- 截图数据
                                             `data`  row(
                                                 pictureUrl       string, -- 拍照数据上报-图片url
                                                 width            int   , -- 拍照数据上报-宽度
                                                 height           int    -- 拍照数据上报-高度
                                                 ),

                                             message  row(
                                                 tid                     string, -- 当前请求的事务唯一ID
                                                 bid                     string, -- 长连接整个业务的ID
                                                 version                 string, -- 版本
                                                 `timestamp`             bigint, -- 时间戳
                                                 `method`                string, -- 服务&事件标识
                                                 productKey              string, -- 产品编码
                                                 deviceId                string, -- 设备编码
                                                 `data` row(
                                                 -- 媒体拍照数据
                                                 photoUrl            string, -- 媒体上报的拍照图片url
                                                 height              double,
                                                 isCapture           bigint, -- 是否截图 筛选过滤字段

                                                 -- 轨迹
                                                 longitude           double    , -- 机库子设备-无人机,经度
                                                 latitude            double    , -- 机库子设备-无人机，纬度
                                                 deviceName          string,  -- 执法仪名称
                                                 attitudeHead        double, -- 无人机机头朝向
                                                 gimbalHead          double, -- 无人机云台朝向

                                                 -- 雷达检测数据
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
                                                 sourceType       string   , -- 数据来源类型：M300、RADAR
                                                 targetType       string   , -- 目标类型

                                                 -- 振动仪数据
                                                 objectLabel          string, -- 大类型
                                                 targetCredibility    double,
                                                 `time`               string
                                                 )
                                                 >

                                                 )
                                                 )
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'iot-device-message-id1',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1720682617000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 属性数据存储
drop table if exists iot_device_message_kafka_02;
create table iot_device_message_kafka_02 (
                                             productKey    string     comment '产品编码',
                                             deviceId      string     comment '设备id',
                                             type          string     comment '类型',
                                             version       string     comment '版本',
                                             `timestamp`   bigint     comment '时间戳毫秒',
                                             tid           string     comment '当前请求的事务唯一ID',
                                             bid           string     comment '长连接整个业务的ID',
                                             `method`      string     comment '服务&事件标识',
                                             message  row(
                                                 tid                     string, -- 当前请求的事务唯一ID
                                                 bid                     string, -- 长连接整个业务的ID
                                                 version                 string, -- 版本
                                                 `timestamp`             bigint, -- 时间戳
                                                 `method`                string, -- 服务&事件标识
                                                 productKey              string, -- 产品编码
                                                 deviceId                string, -- 设备编码
                                                 `data`                  string  -- 属性数据
                                                 )
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'iot-device-message-id2',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 媒体拍照数据入库（Sink：mysql）
drop table if exists device_media_datasource;
create table device_media_datasource (
                                         device_id                      string        comment '设备编码',
                                         source_id                      string        comment '来源,截图(SCREENSHOT)，拍照(PHOTOGRAPH)',
                                         source_name                    string        comment '来源名称/应用名称',
                                         type                           string        comment 'PICTURE/HISTORY_VIDEO',
                                         start_time                     string        comment '开始时间',
                                         end_time                       string        comment '结束时间',
                                         url                            string        comment '原图/视频 url',
                                         width                          int           comment '图片宽度',
                                         height                         int           comment '图片高度',
                                         bid                            string        comment '长连接整个业务的ID',
                                         tid                            string        comment '当前请求的事务唯一ID',
                                         b_type                         string        comment '业务类型',
                                         extends                        string        comment 'json字符串 {}',
                                         gmt_create                     string        comment '创建时间',
                                         gmt_create_by                  string        comment '创建人',
                                         gmt_modified_by                string        comment '修改人',
                                         PRIMARY KEY (device_id,start_time,url) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true', -- ECS环境
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',  -- 201环境
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device_media_datasource',
      'sink.buffer-flush.interval' = '1s',
      'sink.buffer-flush.max-rows' = '10'
      );



-- 雷达 - 检测目标全量数据入库（Sink：doris）
drop table  if exists dwd_radar_target_all_rt;
create table dwd_radar_target_all_rt(
                                        device_id                  string     , -- '雷达设备id',
                                        target_id                  string     , -- '目标id',
                                        parent_id                  string     , -- 父设备的id,也就是望楼id
                                        acquire_timestamp_format   string     , -- '上游程序上报时间戳-时间戳格式化',
                                        acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                        source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                        device_name                string     , -- 设备名称
                                        source                     string     , -- 数据检测的来源[{deviceName,targetId,type}]
                                        object_label               string     , -- 目标类型
                                        x_distance                 double     , -- 'x距离',
                                        y_distance                 double     , -- 'y距离',
                                        speed                      double     , -- '目标速度',
                                        status                     string     , -- '0 目标跟踪 1 目标丢失 2 跟踪终止',
                                        target_altitude            double     , -- '目标海拔高度',
                                        longitude                  double     , -- '目标经度',
                                        latitude                   double     , -- '目标维度',
                                        target_pitch               double     , -- '俯仰角',
                                        target_yaw                 double     , -- 水平角,新版本加入
                                        distance                   double     , -- 距离，新雷达的距离，没有了x距离和y距离
                                        utc_time                   bigint     , -- '雷达上报的时间'	,
                                        tracked_times              double     , -- '已跟踪次数',
                                        loss_times                 double     , -- '连续丢失次数'
                                        target_credibility         double     ,
                                        time1                      string     ,
                                        tid                        string     , -- 当前请求的事务唯一ID
                                        bid                        string     , -- 长连接整个业务的ID
                                        `method`                   string     , -- 服务&事件标识
                                        product_key                string     , -- 产品编码
                                        version                    string     , -- 版本
                                        update_time                string      -- 数据入库时间
)WITH (
     'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     'fenodes' = '172.21.30.202:30030',                                        -- 物理机器部署
     'table.identifier' = 'dushu.dwd_radar_target_all_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='10s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-- 雷达 - 设备检测目标状态数据入库（Sink：doris）
drop table  if exists dws_radar_target_status_rt;
create table dws_radar_target_status_rt(
                                           device_id                  string     , -- '设备id',
                                           target_id                  string     , -- '目标id',
                                           parent_id                  string     , -- 父设备的id,也就是望楼id
                                           acquire_timestamp_format   string     , -- '上游程序上报时间戳-时间戳格式化',
                                           acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                           source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                           device_name                string     , -- 设备名称
                                           source                     string     , -- 数据检测的来源[{deviceName,targetId,type}]
                                           object_label               string     , -- 目标类型
                                           x_distance                 double     , -- 'x距离',
                                           y_distance                 double     , -- 'y距离',
                                           speed                      double     , -- '目标速度',
                                           status                     string     , -- '0 目标跟踪 1 目标丢失 2 跟踪终止',
                                           target_altitude            double     , -- '目标海拔高度',
                                           longitude                  double     , -- '目标经度',
                                           latitude                   double     , -- '目标维度',
                                           target_pitch               double     , -- '俯仰角',
                                           target_yaw                 double     , -- 水平角,新版本加入
                                           distance                   double     , -- 距离，新雷达的距离，没有了x距离和y距离
                                           utc_time                   bigint     , -- '雷达上报的时间'	,
                                           tracked_times              double     , -- '已跟踪次数',
                                           loss_times                 double     , -- '连续丢失次数'
                                           target_credibility         double     ,
                                           time1                      string     ,
                                           tid                        string     , -- 当前请求的事务唯一ID
                                           bid                        string     , -- 长连接整个业务的ID
                                           `method`                   string     , -- 服务&事件标识
                                           product_key                string     , -- 产品编码
                                           version                    string     , -- 版本
                                           update_time                string      -- 数据入库时间
)WITH (
     'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',  -- k8s部署
     'fenodes' = '172.21.30.202:30030',                                       -- 物理机器部署
     'table.identifier' = 'dushu.dws_radar_target_status_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='5s',
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
                                     attitude_head             DECIMAL(30,18)      comment '无人机机头朝向',
                                     gimbal_head               DECIMAL(30,18)      comment '无人机云台朝向',
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
)WITH (
     'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',  -- k8s部署
     'fenodes' = '172.21.30.202:30030',                                       -- 物理机器部署
     'table.identifier' = 'dushu.dwd_device_track_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='5000',
     'sink.batch.interval'='5s'
     );



-- 设备属性存储（Sink：doris）
drop table if exists dwd_device_attr_info;
create table dwd_device_attr_info (
                                      device_id                 string          comment '设备id:望楼id,雷达ID,可见光红外id,震动器id',
                                      parent_id                 string          comment '父设备id',
                                      acquire_timestamp_format  string          comment '时间戳格式化',
                                      acquire_timestamp         bigint          comment '采集时间戳毫秒级别',
                                      device_type               string          comment '设备类型（根据product_key赋值的）:望楼1,可见光2,红外3,雷达4,北斗5,电池6,震动仪7',
                                      source_type               string          comment '类型，从mysql中关联取出的',
                                      properties                string          comment '设备属性json',
                                      tid                       string          comment '当前请求的事务唯一ID',
                                      bid                       string          comment '长连接整个业务的ID',
                                      `method`                  string          comment '服务&事件标识',
                                      product_key               string          comment '产品编码',
                                      `version`                 string          comment '版本',
                                      `type`                    string          comment '类型',
                                      update_time               string          comment '数据入库时间'
)WITH (
     'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',  -- k8s部署
     'fenodes' = '172.21.30.202:30030',                                       -- 物理机器部署
     'table.identifier' = 'dushu.dwd_device_attr_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='10s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
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
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true', -- ECS环境
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',    -- 201环境
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'iot_device',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '3'
     );


-- 建立映射mysql的表（device）
drop table if exists device;
create table device (
                        id	             int,    -- 自增id
                        device_id	     string, -- 设备id
                        type             string, -- 设备名称
                        primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true', -- ECS环境
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',  -- 201环境
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'device',
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
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja-4a?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true', -- 所有环境
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
    coalesce(productKey,message.productKey)   as product_key,
    coalesce(deviceId,message.deviceId)       as device_id,
    coalesce(version,message.version)         as version,
    coalesce(`timestamp`,message.`timestamp`) as acquire_timestamp,
    coalesce(tid,message.tid)                 as tid,
    coalesce(bid,message.bid)                 as bid,
    coalesce(`method`,message.`method`)       as `method`,
    type,

    -- 截图数据
    `data`.pictureUrl as picture_url,
    `data`.width  as width,
    `data`.height as height,

    -- 雷达、振动仪检测目标数据
    message.`data`.targets     as targets,

    -- 轨迹数据
    message.`data`.longitude   as longitude,       -- 轨迹经度
    message.`data`.latitude    as latitude,        -- 轨迹纬度
    message.`data`.deviceName  as device_name,     -- 设备名称，有些设备带
    message.`data`.attitudeHead  as attitude_head, -- 无人机机头朝向
    message.`data`.gimbalHead    as gimbal_head,   -- 无人机云台朝向

    -- 手动拍照
    message.`data`.height                     as height,
    message.`data`.photoUrl    as photo_url,
    message.`data`.isCapture   as is_capture,

    PROCTIME()  as proctime
from iot_device_message_kafka_01
where coalesce(deviceId,message.deviceId) is not null
  -- and abs(coalesce(`timestamp`,message.`timestamp`)/1000 - UNIX_TIMESTAMP()) <=86400;
  and coalesce(`timestamp`,message.`timestamp`) > 1704096000000;



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
                   on t2.gmt_create_by = t3.username;



-- 设备(雷达、振动仪)检测数据筛选处理 r4ae3Loh78v(振动仪)、k8dNIRut1q3（雷达）
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
where `method` = 'event.targetInfo.info'
  and product_key in('r4ae3Loh78v','k8dNIRut1q3');



-- 设备检测数据（雷达、振动仪）数据进一步解析数组
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
    t1.acquire_timestamp,
    t1.device_name_join    as device_name,    -- 设备名称
    if(t1.parent_id is null or t1.parent_id = '',t1.device_id, t1.parent_id) as parent_id,
    t2.targetId          as target_id,     -- 目标id
    t2.xDistance         as x_distance,
    t2.yDistance         as y_distance,
    t2.speed             as speed,
    t2.targetPitch       as target_pitch,
    t2.targetAltitude    as target_altitude, -- 高度
    t2.status            as status,          -- 状态
    t2.targetLongitude   as longitude,       -- 经度
    t2.targetLatitude    as latitude,        -- 纬度
    t2.targetYaw         as target_yaw,
    t2.distance,
    t2.utc_time,
    t2.tracked_times,
    t2.loss_times,
    t2.targetCredibility   as target_credibility,
    t2.`time` as time1,

    if(t2.sourceType <> '',t2.sourceType,t1.device_name_join) as source_type,  -- 设备来源，M300、RADAR

    -- 雷达无目标类型都给未知｜可见光红外需要null、''、未知目标更改为未知｜ 信火一体的需要'' 改为未知
    if(
                coalesce(t2.objectLabel,t2.targetType) is null
                or coalesce(t2.objectLabel,t2.targetType) = ''
                or coalesce(t2.objectLabel,t2.targetType) = '未知目标',
                '未知',coalesce(t2.objectLabel,t2.targetType)
        ) as object_label,

    concat('[{',
           concat('"deviceName":"',t1.device_name_join,'",'),
           concat('"deviceId":"',t1.device_id,'",'),
           concat('"targetId":"',t2.targetId,'",'),
           concat('"type":"',t2.sourceType,'"}]')
        ) as source

from tmp_source_kafka_03 as t1
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
                                            sourceType       ,
                                            targetType       ,

    -- 振动仪数据
                                            objectLabel      ,
                                            targetCredibility,
                                            `time`
    )
where t2.sourceType in('RADAR','VIBRATOR');



-- 轨迹数据筛选处理 QptZJHOd1KD（执法仪），zyrFih3kept(无人机),00000000002（机库子设备-无人机）,xmYA9WCWCtk\0hcOWdhPRzy（警航版本中小川加的）
drop view if exists tmp_track_01;
create view tmp_track_01 as
select
    device_id,
    product_key,
    tid,
    bid,
    acquire_timestamp,
    attitude_head,
    gimbal_head,
    device_name,
    longitude,
    latitude,
    username,
    group_id
from tmp_source_kafka_02
where product_key in('QptZJHOd1KD','zyrFih3kept','00000000002','xmYA9WCWCtk','0hcOWdhPRzy')
  and longitude is not null
  and latitude is not null;


-- 各种设备属性处理存储   properties.state（无人机上报的属性）、event.property.post（盒子上报的属性）
drop view if exists tmp_attr_01;
create view tmp_attr_01 as
select
    t1.device_id,
    case
        when t1.product_key = 'Y95SjAkrmRG' then '1' -- 望楼
        when t1.product_key = 'uvFrSFW2zMs' then '2' -- 可见光
        when t1.product_key = 'mVpLCOnTPLz' then '3' -- 红外
        when t1.product_key = 'k8dNIRut1q3' then '4' -- 雷达
        when t1.product_key = 'raYeBHvRKYP' then '5' -- 北斗
        when t1.product_key = 'eX71parWGpf' then '6' -- 电池
        when t1.product_key = 'r4ae3Loh78v' then '7' -- 振动仪
        when t1.product_key = 'dTz5djGU3Jb' then '8' -- 边缘设备
        when t1.product_key = '68ai6goNgw5' then '9' -- 网络设备（mesh）
        when t1.product_key in ('zyrFih3kept','0hcOWdhPRzy','00000000002') then '10' -- 无人机
        when t1.product_key = '00000000001' then '11' -- 无人机机库
        else t1.product_key end as device_type,

    if(t2.parent_id is not null and t2.parent_id <> '',t2.parent_id,t1.device_id) as parent_id,

    -- TO_TIMESTAMP_LTZ(acquire_timestamp,3) as acquire_timestamp_format,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    t3.type as source_type,
    acquire_timestamp,
    properties,
    tid,
    bid,
    `method`,
    product_key,
    version,
    t1.`type`
from (
         select
             type,
             coalesce(productKey,message.productKey)   as product_key,       -- message_product_key
             coalesce(deviceId,message.deviceId)       as device_id,         -- message_device_id
             coalesce(version,message.version)         as version,           -- message_version
             coalesce(`timestamp`,message.`timestamp`) as acquire_timestamp, -- message_acquire_timestamp
             coalesce(tid,message.tid)                 as tid,               -- message_tid
             coalesce(bid,message.bid)                 as bid,               -- message_bid
             coalesce(`method`,message.`method`)       as `method`,          -- message_method
             message.`data`                            as properties,
             PROCTIME()  as proctime
         from iot_device_message_kafka_02
         where coalesce(productKey,message.productKey) in('Y95SjAkrmRG','uvFrSFW2zMs','mVpLCOnTPLz','k8dNIRut1q3','raYeBHvRKYP','r4ae3Loh78v','eX71parWGpf',
                                                          'dTz5djGU3Jb','68ai6goNgw5','zyrFih3kept','00000000001','00000000002','lZezNYnHUO0','3m8d1RppMas','ZN5WjyZvfcP','BBqdWxlqTM3','0hcOWdhPRzy')
           and coalesce(`method`,message.`method`) in ('properties.state','event.property.post')
           and coalesce(`timestamp`,message.`timestamp`) > 1704096000000
     ) as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.device_id = t2.device_id

         left join device FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.device_id = t3.device_id;




-----------------------

-- 数据插入

-----------------------


begin statement set;


-- 轨迹数据入库
insert into dwd_device_track_rt
select
    device_id               ,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format  ,
    acquire_timestamp         ,
    attitude_head             ,
    gimbal_head               ,
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



-- 截图拍照数据入库
insert into device_media_datasource
select
    device_id,
    if(`method` = 'platform.capture.post','SCREENSHOT','PHOTOGRAPH') as source_id,
    device_name_join         as source_name,
    'PICTURE'  as type,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as start_time,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as end_time,
    if(`method` = 'platform.capture.post',concat('/',picture_url),photo_url) as url,
    width,
    height,
    bid,
    tid,
    cast(null as varchar) as b_type,
    '{}'                  as extends,
    from_unixtime(unix_timestamp()) as gmt_create,
    'ja-flink' as gmt_create_by,
    'ja-flink' as gmt_modified_by
from tmp_source_kafka_02
where acquire_timestamp is not null
  and (
        (`method` = 'platform.capture.post' and picture_url is not null)  -- 截图
        or (`method` = 'event.mediaFileUpload.info' and photo_url is not null and is_capture is null) -- 拍照数据过滤截图的
    );



-- 雷达目标 - 检测全量数据入库doris
insert into dwd_radar_target_all_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    source_type,
    device_name,
    source,
    object_label,
    x_distance,
    y_distance,
    speed,
    status,
    target_altitude,
    longitude,
    latitude,
    target_pitch,
    target_yaw,
    distance,
    utc_time,
    tracked_times,
    loss_times,
    target_credibility,
    time1,
    tid,
    bid,
    `method`,
    product_key,
    version,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_04;


-- 雷达目标 - 检测状态数据入库doris
insert into dws_radar_target_status_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    source_type,
    device_name,
    source,
    object_label,
    x_distance,
    y_distance,
    speed,
    status,
    target_altitude,
    longitude,
    latitude,
    target_pitch,
    target_yaw,
    distance,
    utc_time,
    tracked_times,
    loss_times,
    target_credibility,
    time1,
    tid,
    bid,
    `method`,
    product_key,
    version,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_04;


insert into dwd_device_attr_info
select
    device_id                 ,
    parent_id                 ,
    acquire_timestamp_format  ,
    acquire_timestamp         ,
    device_type               ,
    source_type,
    properties                ,
    tid                       ,
    bid                       ,
    `method`                  ,
    product_key               ,
    `version`                 ,
    `type`                    ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_attr_01;


end;


