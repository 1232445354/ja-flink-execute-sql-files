--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/05/09 14:06:19
-- description: 旌旗、望楼、最新版本、信火一体
-- version: 240521
--********************************************************************--


set 'pipeline.name' = 'ja-chingchi-icos3.0-rt-new';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '4';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-chingchi-icos3.0-rt-new' ;



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

    -- 手动拍照截图数据
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

                                                 -- 执法仪轨迹
                                                 longitude           double, -- 经度
                                                 latitude            double, -- 纬度
                                                 deviceName          string,  -- 执法仪名称


                                                 -- 雷达检测数据
                                                 targets array<
                                                 row(
                                                 targetId         string   ,-- 雷达、振动仪目标id
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
                                                 loss_times       double   ,  -- 连续丢失次数
                                                 sourceType       string   ,  -- 数据来源类型：M300、RADAR、可见光、红外、融合、振动仪
                                                 targetType       string   ,   -- 目标类型

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
      'properties.group.id' = 'iot-device-message-group-id1',
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
      'properties.group.id' = 'iot-device-message-group-id2',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 可见光红外检测的数据（Source：kafka）
drop table if exists photoelectric_inspection_result_kafka;
create table photoelectric_inspection_result_kafka(
                                                      batch_id             bigint,
                                                      frame_num            bigint,
                                                      frame_tensor_list    string,
                                                      image_path           string, -- 大图
                                                      record_path          string, -- 告警视频地址
                                                      infer_done           bigint,
                                                      ntp_timestamp        bigint, -- 发出消息的ntp时间
                                                      pts                  bigint,
                                                      source_frame_height  bigint, -- 原视频高度
                                                      source_frame_width   bigint, -- 原视频宽度
                                                      source_id            string, -- 设备id
                                                      video_infer_done     boolean,
                                                      video_path           string,
                                                      object_list array<
                                                          row<
                                                          bbox_height          bigint,
                                                      bbox_width           bigint,
                                                      bbox_left            bigint,
                                                      bbox_top             bigint,
                                                      class_id             bigint,
                                                      confidence           double,
                                                      image_path           string,  -- 目标小图
                                                      image_source         string, -- 图片来源设备
                                                      infer_id             bigint,
                                                      object_id            bigint,  -- 可见光、红外检测目标的id
                                                      object_label         string,  -- 舰船
                                                      object_sub_label     string,  -- 舰船
                                                      radar_target_id      string,  -- 对应的雷达检出目标id
                                                      radar_device_id      string,  -- 雷达设备id
                                                      video_time           bigint,
                                                      speed                double,  -- 速度m/s
                                                      distance             double,  -- 距离单位m
                                                      yaw                  double,  -- 目标方位
                                                      source_type          string,  -- 数据来源:VISUAL:可见光,INFRARED:红外,FUSHION:	融合,VIBRATOR: 震动器
                                                      longitude            double,  -- 目标经度 合上就有经纬度
                                                      latitude             double,  -- 目标纬度
                                                      altitude             double,  -- 高度
                                                      obj_label_list       string,
                                                      obj_tensor_list      string,
                                                      obj_track_list       string
                                                          >
                                                          >
) WITH (
      'connector' = 'kafka',
      'topic' = 'ja-ai-detection-output',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'photoelectric_inspection_result_group_id',
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
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8', -- ECS环境
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',  -- 201环境
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device_media_datasource',
      'sink.buffer-flush.interval' = '1s',
      'sink.buffer-flush.max-rows' = '20'
      );



-- 检测目标全量数据入库（Sink：doris）
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



-- 设备检测目标状态数据入库（Sink：doris）
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



-- -- 设备（可见光、红外）检测全量数据入库（Sink：doris）
-- drop table  if exists dwd_photoelectric_target_all_rt;
-- create table dwd_photoelectric_target_all_rt(
--     device_id                  string     , -- '设备id',
--     target_id                  bigint     , -- '目标id',
--     parent_id                  string     , -- 父设备的id,也就是望楼id
--     acquire_timestamp_format   string     , -- '上游程序上报时间戳-时间戳格式化',
--     acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
--     source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
--     device_name                string     , -- 设备名称
--     radar_id                   string     , -- 雷达id
--     radar_target_id            double     , -- 雷达检测的目标id
--     radar_device_name          string     , -- 雷达的设备名称
--     source                     string     , -- 数据检测的来源拼接 示例：雷达（11）、可见光（22）
--     record_path                string     , -- 可见光、红外告警视频地址
--     bbox_height                double     , -- 长度
-- 	bbox_left	               double     , -- 左
-- 	bbox_top	               double     , -- 上
-- 	bbox_width	               double     , -- 宽度
--     source_frame_height        bigint     , -- 原视频高度
--     source_frame_width         bigint     , -- 原视频宽度
-- 	longitude                  double     , -- 目标经度
--     latitude                   double 	  , -- 目标纬度
--     altitude                   double     , -- 高度
-- 	big_image_path             string     , -- 大图
-- 	small_image_path           string     , -- 小图
-- 	class_id                   double     ,
-- 	confidence                 string     , -- 置信度
-- 	infer_id                   double     ,
-- 	object_label               string     , -- 目标的类型，人，车
-- 	object_sub_label           string     , -- 目标的类型子类型
-- 	speed                      double     , -- 目标速度 m/s
-- 	distance                   double     , -- 距离 m
-- 	yaw                        double     , -- 目标方位
--     flag                       string     , -- 是否修改属性-修改、插入
--     create_by                  string     , -- 创建人
--     update_time                string      -- 数据入库时间
-- )WITH (
-- 'connector' = 'doris',
-- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
-- 'table.identifier' = 'dushu.dwd_photoelectric_target_all_rt',
-- 'username' = 'admin',
-- 'password' = 'Jingansi@110',
-- 'doris.request.tablet.size'='3',
-- 'doris.request.read.timeout.ms'='30000',
-- 'sink.batch.size'='50000',
-- 'sink.batch.interval'='10s',
-- 'sink.properties.escape_delimiters' = 'true',
-- 'sink.properties.column_separator' = '\x01',	 -- 列分隔符
-- 'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
-- 'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
-- );



-- 设备（可见光、红外）检测全量数据入库-原表-不支持修改的（Sink：doris）
drop table  if exists dwd_photoelectric_target_all_rt_source;
create table dwd_photoelectric_target_all_rt_source(
                                                       device_id                  string     , -- '设备id',
                                                       target_id                  bigint     , -- '目标id',
                                                       parent_id                  string     , -- 父设备的id,也就是望楼id
                                                       acquire_timestamp_format   string  , -- '上游程序上报时间戳-时间戳格式化',
                                                       acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                                       source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                                       device_name                string     , -- 设备名称
                                                       radar_id                   string     , -- 雷达id
                                                       radar_target_id            string     , -- 雷达检测的目标id
                                                       radar_device_name          string     , -- 雷达的设备名称
                                                       source                     string     , -- 数据检测的来源拼接 示例：雷达（11）、可见光（22）
                                                       record_path                string     , -- 可见光、红外告警视频地址
                                                       bbox_height                double     , -- 长度
                                                       bbox_left	               double     , -- 左
                                                       bbox_top	               double     , -- 上
                                                       bbox_width	               double     , -- 宽度
                                                       source_frame_height        bigint     , -- 原视频高度
                                                       source_frame_width         bigint     , -- 原视频宽度
                                                       longitude                  double     , -- 目标经度
                                                       latitude                   double 	  , -- 目标纬度
                                                       altitude                   double     , -- 高度
                                                       big_image_path             string     , -- 大图
                                                       small_image_path           string     , -- 小图
                                                       image_source               string     , -- 图片来源设备
                                                       class_id                   double     ,
                                                       confidence                 string     , -- 置信度
                                                       infer_id                   double     ,
                                                       object_label               string     , -- 目标的类型，人，车
                                                       object_sub_label           string     , -- 目标的类型子类型
                                                       speed                      double     , -- 目标速度 m/s
                                                       distance                   double     , -- 距离 m
                                                       yaw                        double     , -- 目标方位
                                                       flag                       string     , -- 是否修改属性-修改、插入
                                                       create_by                  string     , -- 创建人
                                                       update_time                string      -- 数据入库时间
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dwd_photoelectric_target_all_rt_source',
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



-- 设备（可见光、红外）检测目标状态数据入库（Sink：doris）
drop table  if exists dws_photoelectric_target_status_rt;
create table dws_photoelectric_target_status_rt(
                                                   device_id                  string     , -- '设备id',
                                                   target_id                  bigint     , -- '目标id',
                                                   parent_id                  string     , -- 父设备的id,也就是望楼id
                                                   acquire_timestamp_format   string  , -- '上游程序上报时间戳-时间戳格式化',
                                                   acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                                   source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                                   device_name                string     , -- 设备名称
                                                   radar_id                   string     , -- 雷达id
                                                   radar_target_id            string     , -- 雷达检测的目标id
                                                   radar_device_name          string     , -- 雷达的设备名称
                                                   source                     string     , -- 数据检测的来源拼接 示例：雷达（11）、可见光（22）
                                                   record_path                string     , -- 可见光、红外告警视频地址
                                                   bbox_height                double     , -- 长度
                                                   bbox_left	               double     , -- 左
                                                   bbox_top	               double     , -- 上
                                                   bbox_width	               double     , -- 宽度
                                                   source_frame_height        bigint     , -- 原视频高度
                                                   source_frame_width         bigint     , -- 原视频宽度
                                                   longitude                  double     , -- 目标经度
                                                   latitude                   double 	  , -- 目标纬度
                                                   altitude                   double     , -- 高度
                                                   big_image_path             string     , -- 大图
                                                   small_image_path           string     , -- 小图
                                                   image_source               string     , -- 图片来源设备
                                                   class_id                   double     ,
                                                   confidence                 string     , -- 置信度
                                                   infer_id                   double     ,
                                                   object_label               string     , -- 目标的类型，人，车
                                                   object_sub_label           string     , -- 目标的类型子类型
                                                   speed                      double     , -- 目标速度 m/s
                                                   distance                   double     , -- 距离 m
                                                   yaw                        double     , -- 目标方位
                                                   flag                       string     , -- 是否修改属性-修改、插入
                                                   create_by                  string     , -- 创建人
                                                   update_time                string      -- 数据入库时间

)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dws_photoelectric_target_status_rt',
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
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dwd_device_track_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='5000',
     'sink.batch.interval'='5s'
     );



-- 设备属性存储（望楼、可见光、红外、雷达、北斗）（Sink：doris）
drop table if exists dwd_device_attr_info;
create table dwd_device_attr_info (
                                      device_id                 string          comment '设备id:望楼id,雷达ID,可见光红外id,震动器id',
                                      parent_id                 string          comment '父设备id',
                                      acquire_timestamp_format  string          comment '时间戳格式化',
                                      acquire_timestamp         bigint          comment '采集时间戳毫秒级别',
                                      device_type               string          comment '设备类型（根据product_key赋值的）:望楼1,可见光2,红外3,雷达4,北斗5,电池6,震动仪7,边缘计算8,网络状态9',
                                      source_type               string          comment '类型，从mysql中关联取出的',
                                      properties                string          comment '设备属性json',
                                      tid                       string          comment '当前请求的事务唯一ID',
                                      bid                       string          comment '长连接整个业务的ID',
                                      `method`                  string          comment '服务&事件标识',
                                      product_key               string          comment '产品编码',
                                      `version`                 string          comment '版本',
                                      `type`                    string          comment '类型',
                                      create_by                 string          comment '创建人',
                                      update_time               string          comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dwd_device_attr_info',
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
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
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
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
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

    -- 手动拍照截图数据
    `data`.pictureUrl as picture_url,
    `data`.width  as width,
    `data`.height as height,

    -- 雷达、振动仪检测目标数据
    message.`data`.targets     as targets,

    -- 执法仪轨迹
    message.`data`.longitude   as longitude,
    message.`data`.latitude    as latitude,
    message.`data`.deviceName  as device_name,

    -- 媒体拍照的
    message.`data`.photoUrl    as photo_url,

    PROCTIME()  as proctime
from iot_device_message_kafka_01
where coalesce(deviceId,message.deviceId) is not null
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



-- 设备(雷达、振动仪)检测数据筛选处理
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
  -- r4ae3Loh78v(振动仪)、k8dNIRut1q3（雷达）
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
    t1.acquire_timestamp   as acquire_timestamp,
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
    t2.targetCredibility as target_credibility,
    t2.`time` as time1,
    if(t2.sourceType <> '',t2.sourceType,t1.device_name_join) as source_type,  -- 设备来源，M300、RADAR

    if(t2.sourceType = 'RADAR','未知',if(t2.objectLabel = '' or t2.objectLabel is null or t2.objectLabel = '未知目标','未知',t2.objectLabel)) as object_label,  -- 目标类型，海马斯

    if(t2.targetType is null or t2.targetType = '','未知',t2.targetType) as object_label1,

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
                                            objectLabel       ,
                                            targetCredibility,
                                            `time`

    )
where t2.sourceType in ('RADAR','VIBRATOR');



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



-- -- 振动仪数据筛选处理
-- drop view if exists tmp_source_kafka_05;
-- create view tmp_source_kafka_05 as
-- select
--   product_key,
--   device_id,
--   type,
--   tid,
--   bid,
--   version,
--   -- TO_TIMESTAMP_LTZ(acquire_timestamp,3) as acquire_timestamp_format,
--   from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
--   acquire_timestamp as acquire_timestamp,
--   `method`,
--   if(object_label is null or object_label = '未知目标','未知',object_label) as object_label,
--   distance,
--   if(source_type is not null,source_type,device_name_join) as source_type,
--   longitude,
--   latitude,
--   target_credibility,
--   time1,
--   altitude,
--   device_name_join as device_name,
--   parent_id,
--   target_id,
--   concat('[{',
--       concat('"deviceName":"',if(device_name_join is not null,device_name_join,''),'",'),
--       concat('"deviceId":"',device_id,'",'),
--       concat('"targetId":"',target_id,'",'),
--       concat('"type":"',coalesce(source_type,'VIBRATOR'),'"}]')
--     ) as source
-- from tmp_source_kafka_02
--   where `method` = 'event.alarmEvent.warning'
--     and target_id is not null;



-- 各种设备属性处理存储 - 望楼1,可见光2,红外3,雷达4,北斗5,电池6,震动仪7
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
        end as device_type,

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
              -- Y95SjAkrmRG（望楼）、uvFrSFW2zMs(可见光)、mVpLCOnTPLz(红外)、k8dNIRut1q3(雷达)、raYeBHvRKYP(北斗)、r4ae3Loh78v(振动仪)、eX71parWGpf（电池）、边缘设备（dTz5djGU3Jb）、网络状态（68ai6goNgw5）
         where coalesce(productKey,message.productKey) in('Y95SjAkrmRG','uvFrSFW2zMs','mVpLCOnTPLz','k8dNIRut1q3','raYeBHvRKYP','r4ae3Loh78v','eX71parWGpf','dTz5djGU3Jb','68ai6goNgw5')
           and coalesce(`method`,message.`method`)= 'properties.state'
           and coalesce(`timestamp`,message.`timestamp`) > 1704096000000
     ) as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.device_id = t2.device_id

         left join device FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.device_id = t3.device_id;



-- 可见光、红外检测数据处理
drop view if exists tmp_source_kafka_001;
create view tmp_source_kafka_001 as
select
    t1.batch_id,
    t1.image_path as big_image_path,     -- 外层的大图
    t1.record_path,                      -- 告警视频地址
    t1.source_id as device_id,           -- 设备id
    t1.ntp_timestamp as acquire_timestamp,
    -- TO_TIMESTAMP_LTZ(t1.ntp_timestamp,3) as acquire_timestamp_format,
    from_unixtime(t1.ntp_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    t1.source_frame_height,
    t1.source_frame_width,
    t2.image_path small_image_path,   -- 里层的小图
    t2.image_source,
    t2.object_id as target_id,
    t2.radar_target_id,
    t2.radar_device_id as radar_id,
    t2.bbox_height,
    t2.bbox_left,
    t2.bbox_top,
    t2.bbox_width,
    t2.longitude,
    t2.latitude,
    t2.altitude,
    t2.class_id,
    cast(t2.confidence as varchar) as confidence,
    t2.infer_id,
    t2.object_label,
    t2.object_sub_label,
    t2.speed,
    t2.distance,
    t2.yaw,
    t2.source_type,
    PROCTIME()  as proctime

from photoelectric_inspection_result_kafka as t1
         cross join unnest (object_list) as t2 (
                                                bbox_height          ,
                                                bbox_width           ,
                                                bbox_left            ,
                                                bbox_top             ,
                                                class_id             ,
                                                confidence           ,
                                                image_path           , -- 目标小图
                                                image_source         , -- 图片来源设备
                                                infer_id             ,
                                                object_id            , -- 可见光、红外检测的目标id
                                                object_label         , -- 舰船
                                                object_sub_label     , -- 舰船
                                                radar_target_id      , -- 雷达检测到的目标的目标id
                                                radar_device_id      , -- 雷达设备id
                                                video_time           ,
                                                speed                , -- 速度m/s
                                                distance             , -- 距离单位m
                                                yaw                  , -- 目标方位
                                                source_type          , -- 数据来源:VISUAL:	可见光,INFRARED:	红外,FUSHION:	融合,VIBRATOR: 震动器
                                                longitude            , -- 经度
                                                latitude             , -- 纬度
                                                altitude             , -- 高度
                                                obj_label_list       ,
                                                obj_tensor_list      ,
                                                obj_track_list
    )
where t1.source_id is not null  -- 光电设备ID
  and t2.object_id is not null  -- 光电检测目标ID
  and t1.ntp_timestamp is not null -- 类型
  and t2.source_type is not null;



-- 可见光红外数据关联设备表取出设备名称
drop view if exists tmp_source_kafka_002;
create view tmp_source_kafka_002 as
select
    t1.*,
    t2.device_name, -- 设备名称可见光、红外
    t2.parent_id,
    t3.device_name as radar_device_name, -- 设备名称雷达
    cast(null as varchar) as flag,
    case when source_type in ('VISUAL','INFRARED') then -- 可见光、红外
             concat('[{',
                    concat('"deviceName":"',t2.device_name,'",'),
                    concat('"deviceId":"',t1.device_id,'",'),
                    concat('"targetId":"',cast(target_id as varchar),'",'),
                    concat('"type":"',source_type,'"}]')
                 )
         when source_type in ('FUSION') then
             concat('[{',
                    concat('"deviceName":"',t2.device_name,'",'),
                    concat('"deviceId":"',t1.device_id,'",'),
                    concat('"targetId":"',cast(target_id as varchar),'",'),
                    concat('"type":"','"},{'),
                    concat('"deviceName":"',t3.device_name,'",'),
                    concat('"deviceId":"',radar_id,'",'),
                    concat('"targetId":"',radar_target_id,'",'),
                    concat('"type":"RADAR"}]')
                 )
         else cast(null as varchar) end
        as source

from tmp_source_kafka_001 as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.device_id = t2.device_id
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.radar_id = t3.device_id;


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
        (`method` = 'platform.capture.post' and picture_url is not null)
        or (`method` = 'event.mediaFileUpload.info' and photo_url is not null)
    );



-- 设备(雷达)检测全量数据入库doris
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
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_04;


-- 设备(红外可见光)检测全量数据入库doris-原表-不支持修改的
insert into dwd_photoelectric_target_all_rt_source
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    source_type,
    device_name,
    radar_id,
    radar_target_id,
    radar_device_name,
    source,
    record_path,
    bbox_height,
    bbox_left,
    bbox_top,
    bbox_width,
    source_frame_height,
    source_frame_width,
    longitude,
    latitude,
    altitude,
    big_image_path,
    small_image_path,
    image_source,
    class_id,
    confidence,
    infer_id,
    object_label,
    object_sub_label,
    speed,
    distance,
    yaw,
    flag,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_002;



-- 设备(红外可见光)检测状态数据入库doris
insert into dws_photoelectric_target_status_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    source_type,
    device_name,
    radar_id,
    radar_target_id,
    radar_device_name,
    source,
    record_path,
    bbox_height,
    bbox_left,
    bbox_top,
    bbox_width,
    source_frame_height,
    source_frame_width,
    longitude,
    latitude,
    altitude,
    big_image_path,
    small_image_path,
    image_source,
    class_id,
    confidence,
    infer_id,
    object_label,
    object_sub_label,
    speed,
    distance,
    yaw,
    flag,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_002;



-- -- 设备(振动仪)检测全量数据入库doris
-- insert into dwd_radar_target_all_rt(
--   device_id,target_id,parent_id,acquire_timestamp_format,acquire_timestamp,source_type,device_name,source,target_altitude,longitude,latitude,distance,object_label,target_credibility,time1,tid,bid,`method`,product_key,version,create_by
--   )
-- select
--   device_id,
--   target_id,
--   parent_id,
--   acquire_timestamp_format,
--   acquire_timestamp,
--   source_type,
--   device_name,
--   source,
--   altitude,
--   longitude,
--   latitude,
--   distance,
--   object_label,
--   target_credibility,
--   time1,
--   tid,
--   bid,
--   `method`,
--   product_key,
--   version,
--   'ja-flink' as create_by,
--   from_unixtime(unix_timestamp()) as update_time
-- from tmp_source_kafka_05;


-- -- 设备(振动仪)检测状态数据入库doris
-- insert into dws_radar_target_status_rt(
--     device_id,target_id,parent_id,acquire_timestamp_format,acquire_timestamp,source_type,device_name,source,target_altitude,longitude,latitude,distance,object_label,target_credibility,time1,tid,bid,`method`,product_key,version,create_by
-- )
-- select
--   device_id,
--   target_id,
--   parent_id,
--   acquire_timestamp_format,
--   acquire_timestamp,
--   source_type,
--   device_name,
--   source,
--   altitude,
--   longitude,
--   latitude,
--   distance,
--   object_label,
--   target_credibility,
--   time1,
--   tid,
--   bid,
--   `method`,
--   product_key,
--   version,
--   'ja-flink' as create_by,
--   from_unixtime(unix_timestamp()) as update_time
-- from tmp_source_kafka_05;




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
    'ja-flink' as create_by   ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_attr_01;


end;

