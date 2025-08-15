--********************************************************************--
-- author:      write your name here
-- create time: 2024/12/2 19:42:10
-- description: 截图拍照、属性、轨迹、雷达、振动仪
-- version:ja-chingchi-icos3.0-rt-v250701 密集数据接入
--********************************************************************--

set 'pipeline.name' = 'ja-chingchi-icos3.0-rt';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '4';
-- set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-chingchi-icos3.0-rt';



-- 设备检测数据上报 （Source：kafka）
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
                                                 height              double,
                                                 isCapture           bigint, -- 筛选过滤字段

                                                 -- 执法仪轨迹
                                                 longitude           double, -- 经度
                                                 latitude            double, -- 纬度
                                                 attitudeHead        double, -- 无人机机头朝向
                                                 gimbalHead          double, -- 无人机云台朝向
                                                 altitude            double, -- 海拔
                                                 -- height              double, -- 跟海拔差不多的字段，一起给前段，回溯轨迹需要海拔，3维

                                                 -- 密集数据统计
                                                 statisticalInterval  bigint, --
                                                 resolution           bigint, --
                                                 densityMap array<
                                                 row(
                                                 h3Code         string, -- h3code
                                                 averageDensity bigint  -- 表示该地区的过去一段时间的平均人数
                                                 )
                                                 >,

                                                 -- 雷达、振动仪检测数据
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
                                                 targetType       string   , -- 目标类型,信火一体时候的 ，天朗项目-也有，目标识别类型

                                                 -- 振动仪数据
                                                 objectLabel          string, -- 大类型
                                                 targetCredibility    double,
                                                 `time`               string,

                                                 -- 天朗设备
                                                 RCS               bigint, -- 目标RCS
                                                 radialDistance    double, -- 目标位置信息
                                                 targetState       bigint, -- 目标状态，
                                                 `timestamp`       bigint, -- 录取时间信息（基日）
                                                 timestampBase     bigint, -- 录取时间信息（时间）
                                                 uploadMode        string
                                                 )
                                                 >
                                                 )
                                                 )
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'iot-device-message-group-id4',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1750521634000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 属性数据存储
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
                                                 ),
                                             operator      string   -- 操作人员
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'iot-device-message-group-id5',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1750521634000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 媒体拍照数据入库（Sink：mysql）
create table device_media_datasource (
                                         device_id                      string        comment '设备编码',
                                         source_id                      string        comment '来源,截图(SCREENSHOT)，拍照(PHOTOGRAPH)',
                                         source_name                    string        comment '来源名称/应用名称',
                                         type                           string        comment 'PICTURE/HISTORY_VIDEO',
                                         action_id                      bigint        comment '行动id',
                                         action_item_id                 bigint        comment '子行动id',
                                         start_time                     string        comment '开始时间',
                                         end_time                       string        comment '结束时间',
                                         url                            string        comment '原图/视频 url',
                                         longitude                      double        comment '经度',
                                         latitude                       double        comment '纬度',
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
create table dwd_radar_target_all_rt(
                                        device_id                  string     , -- '雷达设备id',
                                        target_id                  string     , -- '目标id',
                                        acquire_timestamp_format   string     , -- '上游程序上报时间戳-时间戳格式化',
                                        parent_id                  string     , -- 父设备的id,也就是望楼id
                                        acquire_timestamp          bigint     , -- '采集时间戳毫秒级别，上游程序上报时间戳',
                                        source_type                string     , -- 类型，VISUAL:可见光,INFRARED:红外,FUSHION:	融合,RADAR:雷达,VIBRATOR: 震动器
                                        source_type_name           string     , -- 数据设备来源名称，就是设备类型，使用product_key区分的
                                        device_name                string     , -- 设备名称
                                        device_info                string     , -- 数据检测的来源[{deviceName,targetId,type}]
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
                                        upload_mode                string     , -- 天朗雷达-模式
                                        target_state               bigint     , -- 天朗雷达-目标状态
                                        tid                        string     , -- 当前请求的事务唯一ID
                                        bid                        string     , -- 长连接整个业务的ID
                                        `method`                   string     , -- 服务&事件标识
                                        product_key                string     , -- 产品编码
                                        version                    string     , -- 版本
                                        update_time                string      -- 数据入库时间
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     -- 'fenodes' = '172.21.30.105:30030',
     -- 'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'dushu.dwd_radar_target_all_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='5000',
     'sink.batch.interval'='5s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );




-- 设备执法仪、无人机轨迹数据（Sink：doris）
create table dwd_device_track_rt (
                                     device_id                 string              comment '上报的设备id',
                                     acquire_timestamp_format  string              comment '格式化时间',
                                     acquire_timestamp         bigint              comment '上报时间戳',
                                     attitude_head             DECIMAL(30,18)      comment '无人机机头朝向',
                                     gimbal_head               DECIMAL(30,18)      comment '无人机云台朝向',
                                     altitude                  double              comment '海拔高度',
                                     height                    double              comment '跟海拔差不多的一个高度，用于回溯轨迹3维',
                                     lng_02                    DECIMAL(30,18)      comment '经度—高德坐标系、火星坐标系',
                                     lat_02                    DECIMAL(30,18)      comment '纬度—高德坐标系、火星坐标系',
                                     username                  string              comment '设备用户',
                                     group_id                  string              comment '组织id',
                                     product_key               string              comment '产品key',
                                     tid                       string              comment 'tid',
                                     bid                       string              comment 'bid',
                                     device_name               string              comment '设备名称',
                                     device_type               string              comment '设备类型',
                                     update_time               string              comment '更新插入时间（数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     -- 'fenodes' = '172.21.30.105:30030',
     -- 'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'dushu.dwd_device_track_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='5000',
     'sink.batch.interval'='5s'
     );



-- 设备属性存储（望楼、可见光、红外、雷达、北斗）（Sink：doris）
create table dwd_device_attr_info (
                                      device_id                 string          comment '设备id:望楼id,雷达ID,可见光红外id,震动器id',
                                      acquire_timestamp_format  string          comment '时间戳格式化',
                                      device_type               string          comment '类型，从mysql中关联取出的',
                                      parent_id                 string          comment '父设备id',
                                      acquire_timestamp         bigint          comment '采集时间戳毫秒级别',
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
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     -- 'fenodes' = '172.21.30.105:30030',
     -- 'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'dushu.dwd_device_attr_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='5000',
     'sink.batch.interval'='5s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );




-- 设备事件&服务数据存储（Sink：doris）
create table dwd_device_operate_report_info (
                                                device_id                 string          comment '设备id:望楼id,雷达ID,可见光红外id,震动器id',
                                                acquire_timestamp_format  string          comment '时间戳格式化',
                                                device_type               string          comment '类型，从mysql中关联取出的',
                                                parent_id                 string          comment '父设备id',
                                                acquire_timestamp         bigint          comment '采集时间戳毫秒级别',
                                                properties                string          comment '设备属性json',
                                                health_info               string          comment '属性-健康信息',
                                                operator                  string          comment '操作人',
                                                tid                       string          comment '当前请求的事务唯一ID',
                                                bid                       string          comment '长连接整个业务的ID',
                                                `method`                  string          comment '服务&事件标识',
                                                product_key               string          comment '产品编码',
                                                `version`                 string          comment '版本',
                                                `type`                    string          comment '类型',
                                                update_time               string          comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     -- 'fenodes' = '172.21.30.105:30030',
--   'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'dushu.dwd_device_operate_report_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='5000',
     'sink.batch.interval'='5s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-- 密集检测人数数据存储（Sink：doris）
create table dwd_dense_person_cnt (
                                      device_id                 string          comment '设备ID',
                                      h3_code                   string          comment 'h3code编码',
                                      acquire_timestamp_format  string          comment '时间戳格式化',
                                      action_id                 bigint          comment '行动ID',
                                      average_density           bigint          comment '表示该地区的过去一段时间的平均人数',
                                      statistical_interval      bigint          comment '单位秒，表示统计人群密度的间隔',
                                      resolution                bigint          comment '分辨率',
                                      tid                       string          comment '当前请求的事务唯一ID',
                                      bid                       string          comment '长连接整个业务的ID',
                                      `method`                  string          comment '服务&事件标识',
                                      product_key               string          comment '产品编码',
                                      `version`                 string          comment '版本',
                                      `type`                    string          comment '类型',
                                      update_time               string          comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     -- 'fenodes' = '172.21.30.105:30030',
     -- 'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'dushu.dwd_dense_person_cnt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='5000',
     'sink.batch.interval'='5s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );




-- 建立映射mysql的表（为了查询用户名称）
create table iot_device (
                            id	             int,    -- 自增id
                            parent_id        string, -- 父设备的id,也就是望楼id
                            device_id	     string, -- 设备id
                            device_name      string, -- 设备名称
                            gmt_create_by	 string, -- 创建用户名
                            primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'iot_device',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '10'
     );



-- 建立映射mysql的表（device）
create table device (
                        id	             int,    -- 自增id
                        device_id	     string, -- 设备id
                        type             string, -- 设备类型
                        primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'device',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '10'
     );


-- 目标类型枚举
create table enum_target_name (
                                  id                bigint,
                                  source_type       string, -- 项目来源的枚举
                                  target_type_code  string, -- 目标的类型枚举代码
                                  target_name	   string, -- 目标类型名称
                                  primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'enum_target_name',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '10'
     );


-- 行动任务表
create table action_item (
                             id                bigint, -- 子任务ID
                             action_id         bigint, -- 行动任务ID
                             status            string, -- 子任务状态，PENDING：未开始,PROCESSING：行动中，FINISHED：已完成
                             device_id	        string, -- 设备ID
                             primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'action_item',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '20s',
     'lookup.max-retries' = '10'
     );


-- 建立映射mysql的表（为了查询组织id）
create table users (
                       user_id	    int,
                       username	string,
                       password	string,
                       name	    string,
                       group_id	string,
                       primary key (user_id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja-4a?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'users',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '10'
     );



-----------------------

-- 数据处理

-----------------------

-- kafka来源的所有数据解析
create view tmp_source_kafka_01 as
select
    coalesce(productKey,message.productKey)   as product_key, -- message_product_key
    coalesce(deviceId,message.deviceId)       as device_id,   -- message_device_id
    coalesce(version,message.version)         as version, -- message_version
    coalesce(`timestamp`,message.`timestamp`) as acquire_timestamp, -- message_acquire_timestamp
    coalesce(tid,message.tid)                 as tid, -- message_tid
    coalesce(bid,message.bid)                 as bid, -- message_bid
    coalesce(`method`,message.`method`)       as `method`, -- message_method
    type,

    -- 截图数据
    `data`.pictureUrl as picture_url,
    `data`.width  as width,
    `data`.height as height,

    -- 雷达、振动仪检测目标数据
    message.`data`.targets     as targets,

    -- 执法仪轨迹
    message.`data`.longitude     as longitude,
    message.`data`.latitude      as latitude,
    message.`data`.attitudeHead  as attitude_head,
    message.`data`.gimbalHead    as gimbal_head,
    message.`data`.height        as uav_height,
    message.`data`.altitude      as altitude,

    -- 密集检测
    message.`data`.statisticalInterval      as statistical_interval,
    message.`data`.resolution               as resolution,
    message.`data`.densityMap               as density_map,

    -- 手动拍照
    message.`data`.photoUrl    as photo_url,
    message.`data`.isCapture   as is_capture, -- 为了过滤截图截图送检的
    PROCTIME()  as proctime
from iot_device_message_kafka_01
where coalesce(deviceId,message.deviceId) is not null
  -- 小于10天的数据
  and abs(coalesce(`timestamp`,message.`timestamp`)/1000 - UNIX_TIMESTAMP()) <= 864000;


-- 关联设备表取出设备名称,取出父设备望楼id，数据来源都是子设备id
create view tmp_source_kafka_02 as
select
    t1.*,
    t2.gmt_create_by as username,
    t2.device_name as device_name_join,
    if(t2.parent_id = '',cast(null as varchar),t2.parent_id) as parent_id,
    t3.group_id,
    t4.type as device_type_join
from tmp_source_kafka_01 as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.device_id = t2.device_id
         left join users FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t2.gmt_create_by = t3.username
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.device_id = t4.device_id;


-- 拍照截图数据处理
create view tmp_image_01 as
select
    t1.device_id,
    if(t1.`method` = 'platform.capture.post','SCREENSHOT','PHOTOGRAPH') as source_id,
    t1.device_name_join as source_name,
    'PICTURE'  as type,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as start_time,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as end_time,
    if(`method` = 'platform.capture.post',concat('/',picture_url),photo_url) as url,
    t1.longitude,
    t1.latitude,
    t1.width,
    t1.height,
    t1.bid,
    t1.tid,
    cast(null as varchar) as b_type,
    '{}'                  as extends,
    from_unixtime(unix_timestamp()) as gmt_create,
    'ja-flink' as gmt_create_by,
    'ja-flink' as gmt_modified_by,
    t2.action_id,
    t2.id as action_item_id
from (
         select
             *
         from tmp_source_kafka_02
         where (
                       (`method` = 'platform.capture.post' and picture_url is not null)  -- 截图
                       or (`method` = 'event.mediaFileUpload.info' and photo_url is not null and is_capture is null and SPLIT_INDEX(photo_url,'.',1) <> 'MP4') -- 拍照数据过滤截图的，如果是截图（is_capture=1）
                   )
     ) as t1
         left join action_item FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.device_id = t2.device_id
                       and ('PENDING' = t2.status or 'PROCESSING' = t2.status);



-- 密集数据关联处理
create view tmp_dense_01 as
select
    t1.device_id,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    t1.statistical_interval,
    t1.resolution,
    t1.tid,
    t1.bid,
    t1.`method`,
    t1.product_key,
    t1.version,
    t1.type,
    t2.h3Code as h3_code,
    t2.averageDensity as average_density,
    t3.action_id

from (
         select
             *
         from tmp_source_kafka_01
         where `method` = 'event.densityMap.info'
     ) as t1
         cross join unnest (density_map) as t2 (
                                                h3Code,
                                                averageDensity
    )

         left join action_item FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.device_id = t3.device_id
                       and ('PENDING' = t3.status or 'PROCESSING' = t3.status)
where t2.h3Code is not null;



-- 设备(雷达、振动仪)检测数据筛选处理
create view tmp_source_kafka_03 as
select
    tid,
    bid,
    `method`,
    product_key,
    device_id,
    version,
    acquire_timestamp,
    targets,  -- 目标字段
    device_name_join,
    device_type_join,
    parent_id,
    proctime
from tmp_source_kafka_02
where `method` = 'event.targetInfo.info'
  -- -- r4ae3Loh78v(振动仪)、k8dNIRut1q3（雷达）、xjWO7NdIOYs（天朗雷达） 、00000000002（无人机检测目标数据）
  and product_key in('r4ae3Loh78v','k8dNIRut1q3','xjWO7NdIOYs','00000000002');



-- 设备检测数据（雷达）数据进一步解析数组
create view tmp_source_kafka_04 as
select
    t1.tid,
    t1.bid,
    t1.`method`,
    t1.product_key,
    t1.device_id,
    t1.version,
    t1.proctime,
    -- TO_TIMESTAMP_LTZ(t1.acquire_timestamp,3) as acquire_timestamp_format,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    t1.acquire_timestamp                 as acquire_timestamp,
    t1.device_name_join                  as device_name,    -- 设备名称
    coalesce(t1.parent_id,t1.device_id)  as parent_id,
    t2.targetId                          as target_id,     -- 目标id
    t2.xDistance                         as x_distance,
    t2.yDistance                         as y_distance,
    t2.speed                             as speed,
    t2.targetPitch                       as target_pitch,
    t2.targetAltitude                    as target_altitude, -- 高度
    t2.status                            as status,          -- 状态
    t2.targetLongitude                   as longitude,       -- 经度
    t2.targetLatitude                    as latitude,        -- 纬度
    t2.targetYaw                         as target_yaw,
    coalesce(t2.distance,radialDistance) as distance,
    t2.utc_time,
    t2.tracked_times,
    t2.loss_times,
    t2.targetCredibility                 as target_credibility,
    t2.`time` as time1,
    t2.targetState as target_state,
    t2.uploadMode  as upload_mode,

    t1.device_type_join as source_type,         -- RADAR、振动仪 的method是一样的
    t1.device_name_join as source_type_name,

    -- 雷达无目标类型都给未知｜可见光红外需要null、''、未知目标更改为未知｜ 信火一体的需要'' 改为未知,不是最终的，天朗雷达数据还需要关联一下
    coalesce(t2.objectLabel,cast(t2.targetType as varchar)) as object_label1,

    concat('[{',
           concat('"deviceName":"',t1.device_name_join,'",'),
           concat('"deviceId":"',t1.device_id,'",'),
           concat('"targetId":"',t2.targetId,'",'),
           concat('"type":"',t1.device_type_join,'"}]')
        ) as device_info

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
                                            `time`,
    -- 天朗雷达
                                            RCS              ,
                                            radialDistance   ,
                                            targetState      ,
                                            `timestamp`      ,
                                            timestampBase    ,
                                            uploadMode

    );
-- where t2.sourceType in('RADAR','VIBRATOR')
--   -- 天朗雷达数据是空的
--   or t2.sourceType is null;



-- 天朗雷达数据类型关联，
create view tmp_source_kafka_05 as
select
    t1.*,
    case when t1.product_key = 'xjWO7NdIOYs' then t2.target_name
         when t1.product_key <> 'xjWO7NdIOYs' and object_label1 is not null and object_label1 <> '' and object_label1 <> '未知目标' then object_label1
         else '未知' end as object_label

from tmp_source_kafka_04 as t1
         left join enum_target_name FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.object_label1 = t2.target_type_code
                       and '天朗雷达' = t2.source_type;



-- 执法仪、无人机轨迹数据处理
create view tmp_track_01 as
select
    device_id,
    product_key,
    tid,
    bid,
    acquire_timestamp,
    attitude_head,
    gimbal_head,
    altitude,
    uav_height as height,
    longitude,
    latitude,
    username,
    group_id as group_id,
    device_type_join as device_type,
    device_name_join as device_name
from tmp_source_kafka_02
where `method` in('properties.state','event.property.post')  -- 筛选属性数据里面 - 所有设备轨迹数据全部入库
  and longitude is not null
  and latitude is not null;



-- 各种设备属性处理存储 - 望楼1,可见光2,红外3,雷达4,北斗5,电池6,震动仪7
create view tmp_attr_01 as
select
    t1.device_id,
    if(t2.parent_id is not null and t2.parent_id <> '',t2.parent_id,t1.device_id) as parent_id,

    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    t3.type as device_type,
    acquire_timestamp,
    properties,
    tid,
    bid,
    `method`,
    product_key,
    version,
    t1.`type`,
    t1.operator,
    if(t1.type = 'properties',JSON_VALUE(properties,'$.healthInfo[0]'),JSON_VALUE(properties,'$.message')) as health_info
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
             operator,
             PROCTIME()  as proctime
         from iot_device_message_kafka_02
         where abs(coalesce(`timestamp`,message.`timestamp`)/1000 - UNIX_TIMESTAMP()) <= 864000     -- 小于10天的数据
           and (type = 'properties'                                                                 -- 属性数据
             or (coalesce(`method`,message.`method`) = 'event.commonEvent.info')                    -- 事件数据
             or (type = 'services' and coalesce(`method`,message.`method`) <> 'service.live.post')  -- 服务（指令）数据
             )

     ) as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.device_id = t2.device_id

         left join device FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.device_id = t3.device_id;




-----------------------

-- 数据插入

-----------------------


begin statement set;


-- 设备轨迹数据入库
insert into dwd_device_track_rt
select
    device_id               ,
    from_unixtime(acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format  ,
    acquire_timestamp         ,
    attitude_head             ,
    gimbal_head               ,
    altitude,
    height,
    longitude   as lng_02     ,
    latitude    as lat_02     ,
    username                  ,
    group_id                  ,
    product_key               ,
    tid                       ,
    bid                       ,
    device_name               ,
    device_type               ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_track_01;



-- 截图拍照数据入库
insert into device_media_datasource
select
    device_id,
    source_id,
    source_name,
    type,
    action_id,
    action_item_id,
    start_time,
    end_time,
    url,
    longitude,
    latitude,
    width,
    height,
    bid,
    tid,
    b_type,
    extends,
    gmt_create,
    gmt_create_by,
    gmt_modified_by
from tmp_image_01;


-- 雷达目标 - 检测全量数据入库doris
insert into dwd_radar_target_all_rt
select
    device_id,
    target_id,
    acquire_timestamp_format,
    parent_id,
    acquire_timestamp,
    source_type,
    source_type_name,
    device_name,
    device_info,
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
    upload_mode,
    target_state,
    tid,
    bid,
    `method`,
    product_key,
    version,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_05;


-- 属性数据入库
insert into dwd_device_attr_info
select
    device_id                 ,
    acquire_timestamp_format  ,
    device_type               ,
    parent_id                 ,
    acquire_timestamp         ,
    properties                ,
    tid                       ,
    bid                       ,
    `method`                  ,
    product_key               ,
    `version`                 ,
    `type`                    ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_attr_01
where `method` in ('properties.state','event.property.post');



-- 事件数据，服务数据入库
insert into dwd_device_operate_report_info
select
    device_id                 ,
    acquire_timestamp_format  ,
    device_type               ,
    parent_id                 ,
    acquire_timestamp         ,
    cast(null as varchar) as properties,
    health_info               ,
    operator                  ,
    tid                       ,
    bid                       ,
    `method`                  ,
    product_key               ,
    `version`                 ,
    `type`                    ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_attr_01    -- 筛选服务数据 & 事件数据入库（事件数据过滤雷达、拍照数据）
where type in ('services','events')
   or (type = 'properties' and health_info is not null);


insert into dwd_dense_person_cnt
select
    device_id,
    h3_code,
    acquire_timestamp_format,
    action_id,
    average_density,
    statistical_interval,
    resolution,
    tid,
    bid,
    `method`,
    product_key,
    version,
    type,
    from_unixtime(unix_timestamp()) as update_time
from tmp_dense_01;

end;

