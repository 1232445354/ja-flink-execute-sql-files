--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/05/09 14:06:19
-- description: 旌旗、望楼
-- version: 新版本的望楼程序 3.2.0.240509
--********************************************************************--


set 'pipeline.name' = 'ja-chingchi-icos3.0-rt-new';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'parallelism.default' = '2';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-chingchi-icos3.0-rt-new' ;


-- 设备检测数据上报，雷达数据（Source：kafka）
drop table if exists iot_device_message_kafka;
create table iot_device_message_kafka (
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
                                              targetId         bigint   ,-- -目标id
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
                                              sourceType       string     -- 数据来源类型RADAR
                                              )
                                              >,

                                              -- 振动仪数据
                                              objectType           string, -- 大类型
                                              distance             double, -- 上报距离
                                              source_type          string,
                                              targetCredibility    double,
                                              `time`               string,
                                              target_id            bigint,

                                              -- 振动仪属性（望楼子设备）
                                              alarmStatus         string,
                                              `power`             bigint,
                                              detectionRadius     double,
                                              workStatus          string,

                                              -- 望楼/转台 属性
                                              yaw                 bigint,
                                              pitch               bigint,
                                              altitude            bigint,
                                              zeroYaw             bigint,
                                              zeroPitch           bigint,
                                              controlTag          string,
                                              presetPointList     string,

                                              -- 可见光/红外属性（望楼子设备）

                                              zoomRadioMin        bigint,
                                              zoomRadioMax        bigint,
                                              focalDistanceMin    bigint,
                                              focalDistanceMax    bigint,
                                              focalDistance       bigint,
                                              zoomRadio           bigint,
                                              fov                 double,
                                              aspectRatio         double,
                                              videoList           string,
                                              width               bigint,
                                              height              bigint,

                                              -- 雷达属性（望楼子设备）
                                              `scope`               bigint,
                                              receivePort         bigint,
                                              scanRangeProfile    string,

                                              -- 北斗属性（望楼子设备）
                                              tUtcTime            bigint,
                                              cStatus             string,
                                              cLatHemi            string,
                                              cLonHemi            string,
                                              cDirection          string,
                                              dbLatitude          double,
                                              dbLongitude         double,
                                              dbSpeed             double,
                                              dbCourse            double,
                                              dbMagDegree         double

                                              )
                                              )
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'iot-device-message-group-id',
      'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1715961627000',
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
                                                      radar_id             string, -- 雷达id
                                                      object_list array<
                                                          row<
                                                          bbox_height          bigint,
                                                      bbox_width           bigint,
                                                      bbox_left            bigint,
                                                      bbox_top             bigint,
                                                      class_id             bigint,
                                                      confidence           double,
                                                      image_path           string,  -- 目标小图
                                                      infer_id             bigint,
                                                      object_id            bigint,  -- 可见光、红外检测目标的id
                                                      object_label         string,  -- 舰船
                                                      object_sub_label     string,  -- 舰船
                                                      radar_target_id      bigint,  -- 对应的雷达检出目标id
                                                      video_time           bigint,
                                                      speed                double,  -- 速度m/s
                                                      distance             double,  -- 距离单位m
                                                      yaw                  double,  -- 目标方位
                                                      source_type          string,  -- 数据来源:VISUAL:	可见光,INFRARED:	红外,FUSHION:	融合,VIBRATOR: 震动器
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
      'topic' = 'wanglou_test',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'photoelectric_inspection_result_group_id',
      'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1716134687000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 媒体拍照数据入库（Sink：mysql）
drop table if exists device_media_datasource;
create table device_media_datasource (
                                         device_id                      string        comment '设备编码',
                                         source_id                      int           comment '来源id/应用id',
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
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8', -- ECS环境
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',  -- 201环境
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
                                        acquire_timestamp_format   timestamp  , -- '上游程序上报时间戳-时间戳格式化',
                                        acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                        source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                        x_distance                 double     , -- 'x距离',
                                        y_distance                 double     , -- 'y距离',
                                        speed                      double     , -- '目标速度',
                                        status                     string     , -- '0 目标跟踪 1 目标丢失 2 跟踪终止',
                                        target_altitude            double     , -- '目标海拔高度',
                                        target_longitude           double     , -- '目标经度',
                                        target_latitude            double     , -- '目标维度',
                                        target_pitch               double     , -- '俯仰角',
                                        target_yaw                 double     , -- 水平角,新版本加入
                                        distance                   double     , -- 距离，新雷达的距离，没有了x距离和y距离
                                        utc_time                   bigint     , -- '雷达上报的时间'	,
                                        tracked_times              double     , -- '已跟踪次数',
                                        loss_times                 double     , -- '连续丢失次数'
                                        device_name                string     , -- 设备名称
                                        object_label               string     , -- 目标的类型，人，车...
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
     'doris.request.tablet.size'='1',
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
                                           acquire_timestamp_format   timestamp  , -- '上游程序上报时间戳-时间戳格式化',
                                           acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                           source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                           x_distance                 double     , -- 'x距离',
                                           y_distance                 double     , -- 'y距离',
                                           speed                      double     , -- '目标速度',
                                           status                     string     , -- '0 目标跟踪 1 目标丢失 2 跟踪终止',
                                           target_altitude            double     , -- '目标海拔高度',
                                           target_longitude           double     , -- '目标经度',
                                           target_latitude            double     , -- '目标维度',
                                           target_pitch               double     , -- '俯仰角',
                                           target_yaw                 double     , -- 水平角,新版本加入
                                           distance                   double     , -- 距离，新雷达的距离，没有了x距离和y距离
                                           utc_time                   bigint     , -- '雷达上报的时间'	,
                                           tracked_times              double     , -- '已跟踪次数',
                                           loss_times                 double     , -- '连续丢失次数'
                                           device_name                string     , -- 设备名称
                                           object_label               string     , -- 目标的类型，人，车...
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
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='2s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 设备（可见光、红外）检测全量数据入库（Sink：doris）
drop table  if exists dwd_photoelectric_target_all_rt;
create table dwd_photoelectric_target_all_rt(
                                                device_id                  string     , -- '设备id',
                                                target_id                  bigint     , -- '目标id',
                                                parent_id                  string     , -- 父设备的id,也就是望楼id
                                                source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                                acquire_timestamp_format   timestamp  , -- '上游程序上报时间戳-时间戳格式化',
                                                acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                                radar_id                   string     , -- 雷达id
                                                radar_target_id            double     , -- 雷达检测的目标id
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
                                                class_id                   double     ,
                                                confidence                 string     , -- 置信度
                                                infer_id                   double     ,
                                                object_label               string     , -- 目标的类型，人，车
                                                object_sub_label           string     , -- 目标的类型子类型
                                                speed                      double     , -- 目标速度 m/s
                                                distance                   double     , -- 距离 m
                                                yaw                        double     , -- 目标方位
                                                device_name                string     , -- 设备名称
                                                create_by                  string     , -- 创建人
                                                update_time                string      -- 数据入库时间

)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dwd_photoelectric_target_all_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
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
                                                   source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                                   acquire_timestamp_format   timestamp  , -- '上游程序上报时间戳-时间戳格式化',
                                                   acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                                   radar_id                   string     , -- 雷达id
                                                   radar_target_id            double     , -- 雷达检测的目标id
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
                                                   class_id                   double     ,
                                                   confidence                 string     , -- 置信度
                                                   infer_id                   double     ,
                                                   object_label               string     , -- 目标的类型，人，车
                                                   object_sub_label           string     , -- 目标的类型子类型
                                                   speed                      double     , -- 目标速度 m/s
                                                   distance                   double     , -- 距离 m
                                                   yaw                        double     , -- 目标方位
                                                   device_name                string     , -- 设备名称
                                                   create_by                  string     , -- 创建人
                                                   update_time                string      -- 数据入库时间

)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dws_photoelectric_target_status_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='2s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-- 设备（振动仪）检测全量数据入库（Sink：doris）
drop table  if exists dwd_vibrator_target_all_rt;
create table dwd_vibrator_target_all_rt(
                                           device_id                   string, -- 振动仪设备ID-source_id
                                           target_id                   bigint, -- 可见光、红外检测到的目标的id
                                           parent_id                   string, -- 父设备的id,也就是望楼id
                                           acquire_timestamp_format    timestamp, -- 上游程序上报时间戳-时间戳格式化
                                           acquire_timestamp           bigint, -- 采集时间戳毫秒级别，上游程序上报时间戳
                                           source_type                string, -- VIBRATOR: 震动器
                                           longitude                   double, -- 目标经度
                                           latitude                    double, -- 目标纬度
                                           distance                    double, -- 距离 m
                                           object_label                string, -- 目标的类型，人，车...
                                           target_credibility          double,
                                           time1                       string, -- 数据来的time时间
                                           device_name                 string, -- 设备名称
                                           tid                         string, -- 当前请求的事务唯一ID
                                           bid                         string, -- 长连接整个业务的ID
                                           `method`                    string, -- 服务&事件标识
                                           product_key                 string, -- 产品编码
                                           version                     string, -- 版本
                                           create_by                   string, -- 创建人
                                           update_time                 string -- 数据入库时间
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dwd_vibrator_target_all_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='10s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-- 设备（振动仪）检测目标状态数据入库（Sink：doris）
drop table  if exists dws_vibrator_target_status_rt;
create table dws_vibrator_target_status_rt(
                                              device_id                   string, -- 振动仪设备ID-source_id
                                              target_id                   bigint, -- 可见光、红外检测到的目标的id
                                              parent_id                   string, -- 父设备的id,也就是望楼id
                                              acquire_timestamp_format    timestamp, -- 上游程序上报时间戳-时间戳格式化
                                              acquire_timestamp           bigint, -- 采集时间戳毫秒级别，上游程序上报时间戳
                                              source_type                 string, -- VIBRATOR: 震动器
                                              longitude                   double, -- 目标经度
                                              latitude                    double, -- 目标纬度
                                              distance                    double, -- 距离 m
                                              object_label                string, -- 目标的类型，人，车...
                                              target_credibility          double,
                                              time1                       string, -- 数据来的time时间
                                              device_name                 string, -- 设备名称
                                              tid                         string, -- 当前请求的事务唯一ID
                                              bid                         string, -- 长连接整个业务的ID
                                              `method`                    string, -- 服务&事件标识
                                              product_key                 string, -- 产品编码
                                              version                     string, -- 版本
                                              create_by                   string, -- 创建人
                                              update_time                 string -- 数据入库时间

)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
     'table.identifier' = 'dushu.dws_vibrator_target_status_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
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
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='5000',
     'sink.batch.interval'='5s'
     );



-- 设备属性存储（望楼、可见光、红外、雷达、北斗）（Sink：doris）
drop table if exists dwd_device_attr_info;
create table dwd_device_attr_info (
                                      device_id                 string          comment '设备id:望楼id,雷达ID,可见光红外id,震动器id',
                                      device_type               string          comment '设备类型:望楼1,可见光2,红外3,雷达4,北斗5,电池6,震动仪7',
                                      parent_id                 string          comment '父设备id',
                                      acquire_timestamp_format  timestamp       comment '时间戳格式化',
                                      acquire_timestamp         bigint          comment '采集时间戳毫秒级别',
                                      longitude                 double          comment '1',
                                      latitude                  double          comment '1',
                                      yaw                       bigint          comment '1',
                                      pitch                     bigint          comment '1',
                                      altitude                  bigint          comment '1',
                                      zero_yaw                  bigint          comment '1',
                                      zero_pitch                bigint          comment '1',
                                      control_tag               string          comment '1',
                                      preset_point_list         string          comment '1',
                                      zoom_radio_min            bigint          comment '2-3',
                                      zoom_radio_max            bigint          comment '2-3',
                                      focal_distance_min        bigint          comment '2-3',
                                      focal_distance_max        bigint          comment '2-3',
                                      focal_distance            bigint          comment '2-3',
                                      zoom_radio                bigint          comment '2-3',
                                      fov                       decimal(25,20)  comment '2-3',
                                      aspect_ratio              double          comment '2-3',
                                      video_list                string          comment '2-3',
                                      width                     bigint          comment '2-3',
                                      height                    bigint          comment '2-3',
                                      `scope`                   bigint          comment '4',
                                      receive_port              bigint          comment '4',
                                      scan_range_profile        string          comment '4',
                                      t_utc_time                bigint          comment '5',
                                      c_status                  string          comment '5',
                                      c_lat_hemi                string          comment '5',
                                      c_lon_hemi                string          comment '5',
                                      c_direction               string          comment '5',
                                      db_latitude               double          comment '5',
                                      db_longitude              double          comment '5',
                                      db_speed                  double          comment '5',
                                      db_course                 double          comment '5',
                                      db_mag_degree             double          comment '5',
                                      alarm_status				string          comment '7',
                                      object_label				string          comment '7',
                                      `power`					bigint			comment '7',
                                      detection_radius			double			comment '7',
                                      target_credibility		double   		comment '7',
                                      work_status				string          comment '7',
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
     'doris.request.tablet.size'='1',
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
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
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
    productKey  as product_key,
    deviceId    as device_id,
    version     as version,
    `timestamp` as acquire_timestamp,
    type,
    tid,
    bid,
    `method`,

    -- 共有的
    message.tid as message_tid,
    message.bid as message_bid,
    message.version as message_version,
    message.`timestamp` as message_acquire_timestamp,
    message.productKey as message_product_key,
    message.deviceId as message_device_id,
    message.`method` as message_method,

    -- 手动拍照截图数据
    `data`.pictureUrl as picture_url,
    `data`.width  as width,
    `data`.height as height,

    -- 雷达检测目标数据
    message.`data`.targets     as targets,

    -- 执法仪轨迹
    message.`data`.longitude   as longitude,
    message.`data`.latitude    as latitude,
    message.`data`.deviceName                 as device_name,


    -- 媒体拍照的
    message.`data`.photoUrl                   as photo_url,

    -- 振动仪数据
    message.`data`.objectType   as object_label,
    message.`data`.distance,
    message.`data`.source_type,
    -- 上面有
    -- message.`data`.longitude,
    -- message.`data`.latitude,
    message.`data`.targetCredibility as target_credibility,
    message.`data`.`time` as time1,
    message.`data`.target_id,

    -- 振动仪属性
    message.`data`.alarmStatus as alarm_status,
    message.`data`.`power`,
    message.`data`.detectionRadius as detection_radius,
    message.`data`.workStatus  as work_status,


    -- 望楼/转台 属性
    message.`data`.yaw,
    message.`data`.pitch,
    message.`data`.altitude,
    message.`data`.zeroYaw             as zero_yaw,
    message.`data`.zeroPitch           as zero_pitch,
    message.`data`.controlTag          as control_tag,
    message.`data`.presetPointList     as preset_point_list,

    -- 可见光/红外属性（望楼子设备）
    message.`data`.zoomRadioMin        as zoom_radio_min,
    message.`data`.zoomRadioMax        as zoom_radio_max,
    message.`data`.focalDistanceMin    as focal_distance_min,
    message.`data`.focalDistanceMax    as focal_distance_max,
    message.`data`.focalDistance       as focal_distance,
    message.`data`.zoomRadio           as zoom_radio,
    message.`data`.fov,
    message.`data`.aspectRatio         as aspect_ratio,
    message.`data`.videoList           as video_list,
    message.`data`.width               as message_width,
    message.`data`.height              as message_height,

    -- 雷达属性（望楼子设备）
    message.`data`.`scope`,
    message.`data`.receivePort         as receive_port,
    message.`data`.scanRangeProfile    as scan_range_profile,

    -- 北斗属性（望楼子设备）
    message.`data`.tUtcTime            as t_utc_time,
    message.`data`.cStatus             as c_status,
    message.`data`.cLatHemi            as c_lat_hemi,
    message.`data`.cLonHemi            as c_lon_hemi,
    message.`data`.cDirection          as c_direction,
    message.`data`.dbLatitude          as db_latitude,
    message.`data`.dbLongitude         as db_longitude,
    message.`data`.dbSpeed             as db_speed,
    message.`data`.dbCourse            as db_course,
    message.`data`.dbMagDegree         as db_mag_degree,

    PROCTIME()  as proctime
from iot_device_message_kafka;


-- 关联设备表取出设备名称,取出父设备望楼id，数据来源都是子设备id
drop view if exists tmp_source_kafka_01_01;
create view tmp_source_kafka_01_01 as
select
    t1.*,
    t2.gmt_create_by as username,
    t2.device_name as device_name_join,
    t2.parent_id,
    t3.group_id
from tmp_source_kafka_01 as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on if(t1.message_device_id is not null,t1.message_device_id,t1.device_id) = t2.device_id

         left join users FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t2.gmt_create_by = t3.username;



-- 设备(雷达)检测数据筛选处理
drop view if exists tmp_source_kafka_02;
create view tmp_source_kafka_02 as
select
    message_tid,
    message_bid,
    message_method,
    message_product_key,
    message_device_id,
    message_version,
    message_acquire_timestamp,
    targets,
    device_name_join,
    parent_id,
    proctime
from tmp_source_kafka_01_01
where message_acquire_timestamp is not null
  and message_method = 'event.targetInfo.info'
  and message_device_id is not null;


-- 设备检测数据（雷达）数据进一步解析数组
drop view if exists tmp_source_kafka_03;
create view tmp_source_kafka_03 as
select
    t1.message_tid as tid,
    t1.message_bid as bid,
    t1.message_method as `method`,
    t1.message_product_key as product_key,
    t1.message_device_id as device_id,
    t1.message_version as version,
    TO_TIMESTAMP_LTZ(t1.message_acquire_timestamp,3) as acquire_timestamp_format,
    t1.message_acquire_timestamp as acquire_timestamp,
    t1.device_name_join as device_name,
    t1.parent_id,
    t2.targetId          as target_id,
    t2.xDistance         as x_distance,
    t2.yDistance         as y_distance,
    t2.speed             as speed,
    t2.targetPitch       as target_pitch,
    t2.targetAltitude    as target_altitude,
    t2.status            as status,
    t2.targetLongitude   as target_longitude,
    t2.targetLatitude    as target_latitude,
    t2.targetYaw         as target_yaw,
    t2.distance,
    t2.utc_time,
    t2.tracked_times,
    t2.loss_times,
    t2.sourceType as source_type
from tmp_source_kafka_02 as t1
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
                                            sourceType
    );


-- 执法仪、无人机轨迹数据处理
drop view if exists tmp_source_kafka_05;
create view tmp_source_kafka_05 as
select
    device_id,
    product_key,
    type,
    message_tid as tid,
    message_bid as bid,
    message_acquire_timestamp as acquire_timestamp,
    device_name,
    longitude,
    latitude,
    username,
    group_id
from tmp_source_kafka_01_01
where product_key in('QptZJHOd1KD','zyrFih3kept','00000000002')  -- QptZJHOd1KD :执法仪，zyrFih3kept,00000000002:无人机
  and device_id is not null
  and message_acquire_timestamp is not null;


-- 振动仪数据处理
drop view if exists tmp_source_kafka_09;
create view tmp_source_kafka_09 as
select
    product_key,
    device_id,
    type,
    message_tid as tid,
    message_bid as bid,
    message_version as version,
    TO_TIMESTAMP_LTZ(message_acquire_timestamp,3) as acquire_timestamp_format,
    message_acquire_timestamp as acquire_timestamp,
    message_method as `method`,
    object_label,
    distance,
    source_type,
    longitude,
    latitude,
    target_credibility,
    time1,
    device_name_join as device_name,
    parent_id,
    target_id
from tmp_source_kafka_01_01
where message_method = 'event.alarmEvent.warning'
  and message_acquire_timestamp is not null
  and device_id is not null
  and target_id is not null;



-- 设备属性存储
drop view if exists tmp_attr_01;
create view tmp_attr_01 as
select
    message_device_id as device_id,
    case
        when message_product_key = 'Y95SjAkrmRG' then '1'
        when message_product_key = 'uvFrSFW2zMs' then '2'
        when message_product_key = 'mVpLCOnTPLz' then '3'
        when message_product_key = 'k8dNIRut1q3' then '4'
        when message_product_key = 'raYeBHvRKYP' then '5'
        when message_product_key = 'r4ae3Loh78v' then '7'
        end as device_type,
    if(message_product_key = 'Y95SjAkrmRG',message_device_id,parent_id) as parent_id,
    TO_TIMESTAMP_LTZ(message_acquire_timestamp,3) as acquire_timestamp_format,
    message_acquire_timestamp as acquire_timestamp,
    longitude,
    latitude,
    yaw                      ,
    pitch                    ,
    altitude                 ,
    zero_yaw                 ,
    zero_pitch               ,
    control_tag              ,
    preset_point_list        ,
    zoom_radio_min           ,
    zoom_radio_max           ,
    focal_distance_min       ,
    focal_distance_max       ,
    focal_distance           ,
    zoom_radio               ,
    fov                      ,
    aspect_ratio             ,
    video_list               ,
    message_width as width   ,
    message_height as height ,
    `scope`                  ,
    receive_port             ,
    scan_range_profile       ,
    t_utc_time               ,
    c_status                 ,
    c_lat_hemi               ,
    c_lon_hemi               ,
    c_direction              ,
    db_latitude              ,
    db_longitude             ,
    db_speed                 ,
    db_course                ,
    db_mag_degree            ,
    alarm_status             ,
    object_label   ,
    `power`        ,
    detection_radius,
    target_credibility,
    work_status,
    message_tid as tid,
    message_bid as bid,
    message_method as `method`,
    message_product_key as product_key,
    message_version as version,
    type

from tmp_source_kafka_01_01
where message_product_key in('Y95SjAkrmRG','uvFrSFW2zMs','mVpLCOnTPLz','k8dNIRut1q3','raYeBHvRKYP','r4ae3Loh78v') -- Y95SjAkrmRG（望楼）、uvFrSFW2zMs(可见光)、mVpLCOnTPLz(红外)、k8dNIRut1q3(雷达)、raYeBHvRKYP(北斗)、r4ae3Loh78v(振动仪)
  and message_acquire_timestamp is not null
  and message_acquire_timestamp > 1704096000000
  and message_method = 'properties.state'
  and message_device_id is not null;



-- 可见光、红外检测数据处理
drop view if exists tmp_source_kafka_001;
create view tmp_source_kafka_001 as
select
    t1.batch_id,
    t1.image_path as big_image_path,     -- 外层的大图
    t1.record_path,                      -- 告警视频地址
    t1.source_id as device_id,           -- 设备id
    t1.ntp_timestamp as acquire_timestamp,
    TO_TIMESTAMP_LTZ(t1.ntp_timestamp,3) as acquire_timestamp_format,
    t1.radar_id as radar_id,
    t1.source_frame_height,
    t1.source_frame_width,
    t2.image_path small_image_path,   -- 里层的小图
    t2.object_id as target_id,
    t2.radar_target_id,
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
                                                infer_id             ,
                                                object_id            , -- 可见光、红外检测的目标id
                                                object_label         , -- 舰船
                                                object_sub_label     , -- 舰船
                                                radar_target_id      , -- 雷达检测到的目标的目标id
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
  and t1.ntp_timestamp is not null
  and t2.source_type is not null;


-- 可见光红外数据关联设备表取出设备名称
drop view if exists tmp_source_kafka_002;
create view tmp_source_kafka_002 as
select
    t1.*,
    t2.device_name,
    t2.parent_id,
    t3.device_name as radar_device_name,
    if(source_type = 'FUSHION',concat(t3.device_name,'(',cast(t1.radar_target_id as varchar),')、',t2.device_name,'(',cast(target_id as varchar),')'),cast(null as varchar)) as source
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
from tmp_source_kafka_05;



-- 数据手动截图数据入库
insert into device_media_datasource
select
    device_id            as device_id,
    1                    as source_id,
    cast(null as string) as source_name,
    'PICTURE' as type,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as start_time,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as end_time,
    picture_url          as url,
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
where
    acquire_timestamp is not null
  and picture_url is not null
  and `method` = 'platform.capture.post';



-- 驾驶舱遥控器拍照
insert into device_media_datasource
select
    device_id,
    cast(null as int)            as source_id,
    cast(null as string)         as source_name,
    'PICTURE'  as type,
    from_unixtime(message_acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as start_time,
    from_unixtime(message_acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as end_time,
    photo_url                    as url,
    0 as width,
    0 as height,
    message_bid as bid,
    message_tid as tid,
    'WAYLINE_TASK' as b_type,
    '{}'           as extends,
    from_unixtime(unix_timestamp()) as gmt_create,
    'ja-flink' as gmt_create_by,
    'ja-flink' as gmt_modified_by
from tmp_source_kafka_01
where
    message_acquire_timestamp is not null
  and message_method = 'event.mediaFileUpload.info'
  and photo_url is not null;



-- 设备(雷达)检测全量数据入库doris
insert into dwd_radar_target_all_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    coalesce(source_type,'RADAR') as source_type,
    x_distance,
    y_distance,
    speed,
    status,
    target_altitude,
    target_longitude,
    target_latitude,
    target_pitch,
    target_yaw,
    distance,
    utc_time,
    tracked_times,
    loss_times,
    device_name,
    cast(null as varchar) as object_label,
    tid,
    bid,
    `method`,
    product_key,
    version,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_03;


-- 设备(雷达)检测状态数据入库doris
insert into dws_radar_target_status_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    coalesce(source_type,'RADAR') as source_type,
    x_distance,
    y_distance,
    speed,
    status,
    target_altitude,
    target_longitude,
    target_latitude,
    target_pitch,
    target_yaw,
    distance,
    utc_time,
    tracked_times,
    loss_times,
    device_name,
    cast(null as varchar) as object_label,
    tid,
    bid,
    `method`,
    product_key,
    version,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_03;


-- 设备(红外可见光)检测全量数据入库doris
insert into dwd_photoelectric_target_all_rt
select
    device_id,
    target_id,
    parent_id,
    source_type,
    acquire_timestamp_format,
    acquire_timestamp,
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
    class_id,
    confidence,
    infer_id,
    object_label,
    object_sub_label,
    speed,
    distance,
    yaw,
    device_name,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_002;


-- 设备(红外可见光)检测状态数据入库doris
insert into dws_photoelectric_target_status_rt
select
    device_id,
    target_id,
    parent_id,
    source_type,
    acquire_timestamp_format,
    acquire_timestamp,
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
    class_id,
    confidence,
    infer_id,
    object_label,
    object_sub_label,
    speed,
    distance,
    yaw,
    device_name,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_002;


-- 设备(振动仪)检测全量数据入库doris
insert into dwd_vibrator_target_all_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    coalesce(source_type,'VIBRATOR') as source_type,
    longitude,
    latitude,
    distance,
    object_label,
    target_credibility,
    time1,
    device_name,
    tid,
    bid,
    `method`,
    product_key,
    version,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_09;



-- 设备(振动仪)检测状态数据入库doris
insert into dws_vibrator_target_status_rt
select
    device_id,
    target_id,
    parent_id,
    acquire_timestamp_format,
    acquire_timestamp,
    coalesce(source_type,'VIBRATOR') as source_type,
    longitude,
    latitude,
    distance,
    object_label,
    target_credibility,
    time1,
    device_name,
    tid,
    bid,
    `method`,
    product_key,
    version,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_09;



insert into dwd_device_attr_info
select
    *,
    'ja-flink' as create_by,
    from_unixtime(unix_timestamp()) as update_time
from tmp_attr_01;


end;

