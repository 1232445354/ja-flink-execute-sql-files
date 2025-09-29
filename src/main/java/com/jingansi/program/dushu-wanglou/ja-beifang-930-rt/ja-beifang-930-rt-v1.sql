


--********************************************************************--
-- author:      write your name here
-- create time: 2025/9/9 10:01:04
-- description: 930的多设备目标融合
-- version :v1
--********************************************************************--
set 'pipeline.name' = 'ja-beifang-930-rt';


-- SET 'parallelism.default' = '4';
set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-beifang-930-rt';



-- 流表
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

                                                 -- sy600
                                                 id INT, -- 目标id
                                                 trackStatus STRING, -- 跟踪状态（枚举：0-未跟踪，1-正在跟踪，2-跟踪完成）
                                                 yaw double, -- 光电跟踪方位
                                                 pitch double, -- 光电跟踪俯仰
                                                 type STRING, -- 识别类型（枚举：0-不明，1-人员，2-车辆，3-WR机-默认旋翼，4-WR机-固D翼，5-小船-RCS<5㎡，6-船只-默认中型-20×5米，7-大船-RCS>100㎡）
                                                 confidence INT,-- 置信度

                                                 -- ncs30
                                                 auvId STRING, -- 干扰车名称
                                                 `time` BIGINT, -- 发现时间
                                                 trackBatchNumber STRING, -- 目标批号
                                                 frequence INT, -- 侦测中心频率
                                                 bandWidth INT, -- 侦测中心带宽
                                                 amplitude INT, -- 信号幅度
                                                 `position` double, -- 目标信号方位
                                                 threatLevel INT, -- 威胁等级
                                                 signal int, -- 上行/下行信号（枚举：0-未知，1-上行信号，2-下行型号）
                                                 manufacturerNumber STRING, -- 目标无人机厂家
                                                 targetModelNumber INT, -- 目标无人机型号
                                                 sim int ,-- 目标类型（枚举：1-真实，2-模拟）

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
                                                 -- zy11 雷达
                                                 targetId INT, -- 目标id
                                                 xDistance INT, -- 横向距离
                                                 yDistance INT, -- 垂直距离
                                                 speed INT, -- 速度
                                                 status int, -- 状态（枚举：0-正常跟踪，1-目标丢失，2-目标终止）
                                                 targetLongitude double, -- 目标经度
                                                 targetLatitude double, -- 目标纬度
                                                 targetAltitude double, -- 目标海拔高度
                                                 targetPitch INT, -- 目标俯仰角
                                                 targetRange INT, -- 目标斜距
                                                 targetYaw INT, -- 目标方位
                                                 heading double,-- 目标航向


                                                 -- 算法检测
                                                 -- targetId int,
                                                 targetType string ,
                                                 sourceImage string
                                                 )
                                                 >
                                                 )
                                                 )
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.group.id' = 'ja-beifang-930-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1750521634000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- jy11和ncs30 合并的中间表
create table merge_jy11_ncs30 (
                                  dataType STRING	,
                                  device_id	STRING	,
                                  device_lng	DOUBLE	,
                                  device_lat	DOUBLE	,
                                  target_id	STRING	,
                                  target_lng	DOUBLE	,
                                  target_lat	DOUBLE	,
                                  target_azimuth	DOUBLE	,
                                  timestamp_millis	BIGINT
) WITH (
      'connector' = 'kafka',
      'topic' = 'merge_jy11_ncs30',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.group.id' = 'ja-beifang-930-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1750521634000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );




create table merge_jy11_ncs30_list (
                                       id	BIGINT	,
                                       jy11_device_id	STRING	,
                                       jy11_device_lat	DOUBLE	,
                                       jy11_device_lng	DOUBLE	,
                                       jy11_target_lat	DOUBLE	,
                                       jy11_target_lng	DOUBLE	,
                                       jy11_yaw	DOUBLE	,
                                       jy11_acquire_time	VARCHAR(2000)	,
                                       ncs30_track_batch_number	STRING	,
                                       ncs30_device_id	STRING	,
                                       ncs30_device_lat	DOUBLE	,
                                       ncs30_device_lng	DOUBLE	,
                                       ncs30_device_yaw	DOUBLE	,
                                       ncs30_acquire_time	VARCHAR(2000)	,
                                       yaw_diff	DOUBLE	,
                                       score_yaw	DOUBLE	,
                                       score_total	DOUBLE
) WITH (
      'connector' = 'kafka',
      'topic' = 'merge_jy11_ncs30_list',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.group.id' = 'ja-beifang-930-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1750521634000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 维表
create table dim_batch_id_info (
                                   batch_id  int  comment '批号',
                                   device_id  string  comment '设备编号',
                                   device_type  int  comment '设备类型',
                                   seen_cnt  int  comment '出现次数',
                                   last_seen_time  timestamp  comment '最后一次出现时间',
    -- upate_time  string  comment '更新时间',
                                   primary key (batch_id,device_id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'url' = 'jdbc:mysql://172.21.30.244:9030/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'username' = 'root',
     'password' = 'Jingansi@110',
     'table-name' = 'dim_batch_id_info',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '60s',
     'lookup.max-retries' = '10'
     );



create table three_level_overlay (
                                     id	int,
                                     level1_overlay_lng	double,
                                     level1_overlay_lat	double,
                                     level1_overlay_r	double,
                                     level2_overlay_lng	double,
                                     level2_overlay_lat	double,
                                     level2_overlay_r	double,
                                     level3_overlay_lng	double,
                                     level3_overlay_lat	double,
                                     level3_overlay_r	double,
                                     primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja_sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'three_level_overlay1',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '60s',
     'lookup.max-retries' = '10'
     );





-- 建立映射mysql的表（为了查询组织id）
create table users (
                       user_id	int,
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



-- 建立映射mysql的表（device）
create table device (
                        id	             int,    -- 自增id
                        device_id	     string, -- 设备id
                        type             string, -- 设备类型
                        longitude        decimal(12,8),
                        latitude         decimal(12,8),
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



-- 结果表
create table dwd_bhv_jy11_target_rt(
                                       id  bigint  comment 'ID',
                                       device_id  varchar(30)  comment '设备 id',
                                       acquire_time  string  comment '采集时间',
                                       device_name  string  comment '设备名称',
                                       target_id  int  comment '目标id',
                                       longitude  double  comment '目标经度',
                                       latitude  double  comment '目标纬度',
                                       altitude  double  comment '目标海拔高度',
                                       heading  double  comment '目标海拔航向',
                                       yaw  int  comment '方位角',
                                       pitch  int  comment '目标俯仰角',
                                       speed  int  comment '速度, 单位:米/秒',
                                       x_distance  int  comment '横向距离, 单位:米',
                                       y_distance  int  comment '垂直距离, 单位:米',
                                       status  int  comment '状态: 0-正常跟踪, 1-目标丢失, 2-目标终止',
                                       overlay_id  int  comment '防护区id',
                                       overlay_name  varchar(50)  comment '防护区名称',
                                       update_time  string  comment '更新时间'
)WITH (
     'connector' = 'doris',
     -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     'fenodes' = '172.21.30.244:8030',
     'table.identifier' = 'dushu.dwd_bhv_jy11_target_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='1s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


create table dwd_bhv_ncs30_target_rt(
                                        track_batch_number	VARCHAR(5)  comment '目标批号',
                                        device_id varchar(50) comment '设备id',
                                        acquire_time	string  comment '采集时间',
                                        device_name  string  comment '设备名称',
                                        auv_id	VARCHAR(5)  comment '干扰车名称',
                                        `position`	double  comment '目标信号方位',
                                        amplitude	INT  comment '信号幅度',
                                        signal	INT  comment '上行/下行信号: 0-未知, 1-上行信号, 2-下行型号',
                                        threat_level	INT  comment '威胁等级',
                                        manufacturer_number	VARCHAR(5)  comment '目标无人机厂家',
                                        target_model_number	int  comment '目标无人机型号',
                                        target_type varchar(200) comment '目标类型',
                                        frequence	INT  comment '侦测中心频率',
                                        band_width	INT  comment '侦测中心带宽',
                                        sim	INT  comment '目标类型: 1-真实, 2-模拟',
                                        update_time	string  comment '更新时间'
)WITH (
     'connector' = 'doris',
     -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     'fenodes' = '172.21.30.244:8030',
     'table.identifier' = 'dushu.dwd_bhv_ncs30_target_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='1s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


create table dws_rl_jy11_ncs30_list(
                                       ncs30_track_batch_number	varchar(50)	comment 'ncs30 批号',
                                       ncs30_device_id	varchar(50)	comment 'ncs30设备编号',
                                       ncs30_acquire_time	string	comment 'ncs30采集时间',
                                       ncs30_device_lat	DOUBLE	comment 'ncs30设备经度',
                                       ncs30_device_lng	DOUBLE	comment 'ncs30设备维度',
                                       ncs30_device_yaw	DOUBLE	comment 'ncs30 目标方位',
                                       id	bigint	comment 'jy11 id',
                                       jy11_device_id	varchar(50)	comment 'jy11 设备 id',
                                       jy11_device_lat	DOUBLE	comment 'jy11 设备纬度',
                                       jy11_device_lng	DOUBLE	comment 'jy11 设备经度',
                                       jy11_target_lat	DOUBLE	comment 'jy11 目标纬度',
                                       jy11_target_lng	DOUBLE	comment 'jy11 目标经度',
                                       jy11_yaw	DOUBLE	comment 'jy11 目标方位角',
                                       jy11_acquire_time	string	comment 'jy11 采集时间',
                                       yaw_diff	DOUBLE	comment '方位角差值',
                                       score_yaw	DOUBLE	comment '方位角得分',
                                       score_total	DOUBLE	comment '总得分',
                                       update_time	string	comment '更新时间'
)WITH (
     'connector' = 'doris',
     -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     'fenodes' = '172.21.30.244:8030',
     'table.identifier' = 'dushu.dws_rl_jy11_ncs30_list',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='1s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


create table dwd_bhv_sy600_track_target_rt(
                                              id INT comment '目标id, 范围: 1001-9999',
                                              device_id varchar(30) comment '设备 id',
                                              acquire_time string comment '采集时间',
                                              device_name varchar(200) comment '设备名称',
                                              yaw double comment '光电跟踪方位, 范围: 0-360度',
                                              pitch double comment '光电跟踪俯仰, 范围: -90-90度',
                                              track_status int comment '跟踪状态: 0-未跟踪, 1-正在跟踪, 2-跟踪完成',
                                              type int comment '识别类型: 0-不明, 1-人员, 2-车辆, 3-WR机(默认旋翼), 4-WR机(固D翼), 5-小船(RCS<5㎡), 6-船只(默认中型,20×5米), 7-大船(RCS>100㎡)',
                                              type_name varchar(200) comment '类型名称',
                                              confidence int comment '置信度',
                                              update_time string comment '更新时间'
)WITH (
     'connector' = 'doris',
     -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     'fenodes' = '172.21.30.244:8030',
     'table.identifier' = 'dushu.dwd_bhv_sy600_track_target_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='1s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


create table dwd_bhv_object_detection(
                                         id  INT  comment '目标id, 范围: 1001-9999',
                                         device_id varchar(50) comment '设备id',
                                         acquire_time	string  comment '采集时间',
                                         device_name  string  comment '设备名称',
                                         target_type  int  comment '目标类型',
                                         target_type_name  varchar(200)  comment '目标类名称',
                                         source_image  varchar(200)  comment '图片地址',
                                         update_time string comment '更新时间'
)WITH (
     'connector' = 'doris',
     -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     'fenodes' = '172.21.30.244:8030',
     'table.identifier' = 'dushu.dwd_bhv_object_detection',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='1s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


create table doris_dim_batch_id_info(
                                        batch_id  int  comment '批号',
                                        device_id  varchar(30)  comment '设备编号',
                                        device_type  int  comment '设备类型',
                                        seen_cnt  int  comment '出现次数',
                                        last_seen_time  string  comment '最后一次出现时间',
                                        upate_time  string  comment '更新时间'
)WITH (
     'connector' = 'doris',
     -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     'fenodes' = '172.21.30.244:8030',
     'table.identifier' = 'dushu.dim_batch_id_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='1s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


create table dim_rl_jy11_ncs30_id(
                                     id bigint comment 'ID',
                                     jy11_device_id varchar(30) comment '设备 id',
                                     track_batch_number VARCHAR(5) comment 'ncs30 追踪 id',
                                     ncs30_device_id varchar(30) comment '设备 id',
                                     target_id int comment '目标id',
                                     cnt bigint comment '匹配次数',
                                     update_time string comment '更新时间'
)WITH (
     'connector' = 'doris',
     -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     'fenodes' = '172.21.30.244:8030',
     'table.identifier' = 'dushu.dim_rl_jy11_ncs30_id',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='1s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );

-- ****************************规则引擎写入数据******************************** --
create table uav_source(
                           id                    string, -- id
                           name                  string, -- 名称
                           type                  string, -- 类型
                           manufacturer          string, -- 厂商
                           model                 string, -- 型号
                           ew                    double, -- 航迹角
                           height                double, -- 无人机距地高度
                           locationAlit          double, -- 气压高度
                           speedV                double, -- 垂直速度
                           speedH                double, -- 无人机地速,水平速度
                           recvtype              string, -- 无人机数据类型
                           rssi                  double, -- 信号强度
                           lng                   double, -- 融合的经度 - 给大武规则
                           lat                   double, -- 维度的纬度 - 给大武规则
                           longitude             double, -- 原始经度 - 给抓捕
                           latitude              double, -- 原始纬度 - 给抓捕
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



-- 计算距离
create function distance_udf as 'com.jingan.udf.geohash.DistanceUdf';
-- 雷达和光电融合
create function merge_udf as 'com.jingan.udf.merge.MergeJy11Ncs30TargetUdf';



-- 数据处理
create view tmp_source_kafka_01 as
select
    coalesce(productKey,message.productKey)   as product_key, -- message_product_key
    coalesce(deviceId,message.deviceId)       as device_id,   -- message_device_id
    coalesce(version,message.version)         as version, -- message_version
    coalesce(`timestamp`,message.`timestamp`) as acquire_timestamp, -- message_acquire_timestamp
    coalesce(tid,message.tid)                 as tid, -- message_tid
    coalesce(bid,message.bid)                 as bid, -- message_bid
    coalesce(`method`,message.`method`)       as `method`, -- message_method

    -- sy600
    message.`data`.id , -- 目标id
    message.`data`.trackStatus as track_status, -- 跟踪状态（枚举：0-未跟踪，1-正在跟踪，2-跟踪完成）
    message.`data`.yaw , -- 光电跟踪方位
    message.`data`.pitch , -- 光电跟踪俯仰
    message.`data`.type , -- 识别类型（枚举：0-不明，1-人员，2-车辆，3-WR机-默认旋翼，4-WR机-固D翼，5-小船-RCS<5㎡，6-船只-默认中型-20×5米，7-大船-RCS>100㎡）
    message.`data`.confidence ,-- 置信度

    -- ncs30
    message.`data`.auvId as auv_id, -- 干扰车名称
    message.`data`.`time` , -- 发现时间
    message.`data`.trackBatchNumber as track_batch_number, -- 目标批号
    message.`data`.frequence , -- 侦测中心频率
    message.`data`.bandWidth as band_width, -- 侦测中心带宽
    message.`data`.amplitude , -- 信号幅度
    message.`data`.`position` , -- 目标信号方位
    message.`data`.threatLevel as threat_level, -- 威胁等级
    message.`data`.signal , -- 上行/下行信号（枚举：0-未知，1-上行信号，2-下行型号）
    message.`data`.manufacturerNumber as manufacturer_number, -- 目标无人机厂家
    message.`data`.targetModelNumber as target_model_number, -- 目标无人机型号
    message.`data`.sim  ,-- 目标类型（枚举：1-真实，2-模拟）
    message.`data`.targets as targets,
    PROCTIME()  as proctime
from iot_device_message_kafka_01
where coalesce(deviceId,message.deviceId) is not null
  -- 小于10天的数据
  and abs(coalesce(`timestamp`,message.`timestamp`)/1000 - UNIX_TIMESTAMP()) <= 864000
  and coalesce(`method`,message.`method`) in ('event.targetInfo.info','event.trackerBindingResult.info','event.trackTargetInfo.info','event.targetDirection.info')
  and coalesce(productKey,message.productKey) in('ZIUO1Qmupsw','6dAYpNN4LTS','WnJL27UN3sc','aby554pqic0');


-- 关联设备表取出设备名称,取出父设备望楼id，数据来源都是子设备id
create view tmp_source_kafka_02 as
select
    t1.*,
    t2.gmt_create_by as username,
    t2.device_name as device_name_join,
    if(t2.parent_id = '',cast(null as varchar),t2.parent_id) as parent_id,
    t3.group_id,
    t4.type as device_type_join,
    cast(t4.longitude as double) as device_lng,
    cast(t4.latitude as double) as device_lat
from tmp_source_kafka_01 as t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.device_id = t2.device_id
         left join users FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t2.gmt_create_by = t3.username
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.device_id = t4.device_id;


create view tmp_source_kafka_03 as
select
    t1.*,
    -- zy11 雷达
    targetId as target_id, -- 目标id
    xDistance as x_distance, -- 横向距离
    yDistance as y_distance, -- 垂直距离
    speed , -- 速度
    status , -- 状态（枚举：0-正常跟踪，1-目标丢失，2-目标终止）
    targetLongitude as longitude, -- 目标经度
    targetLatitude as latitude, -- 目标纬度
    targetAltitude as  altitude, -- 目标海拔高度
    heading, -- 目标航向
    targetPitch as target_pitch, -- 目标俯仰角
    targetRange as `range`, -- 目标斜距
    targetYaw as target_yaw, -- 目标方位


    -- 算法检测
    -- targetId as target_id,
    targetType as target_type,
    sourceImage as source_image
from tmp_source_kafka_02 as t1
         cross join unnest (targets) as t2 (
    -- zy11 雷达
                                            targetId , -- 目标id
                                            xDistance , -- 横向距离
                                            yDistance , -- 垂直距离
                                            speed , -- 速度
                                            status , -- 状态（枚举：0-正常跟踪，1-目标丢失，2-目标终止）
                                            targetLongitude , -- 目标经度
                                            targetLatitude , -- 目标纬度
                                            targetAltitude , -- 目标海拔高度
                                            targetPitch , -- 目标俯仰角
                                            targetRange , -- 目标斜距
                                            targetYaw , -- 目标方位
                                            heading, -- 目标航向

    -- 算法检测
    -- targetId ,
                                            targetType  ,
                                            sourceImage
    );


-- 处理 jy11雷达数据
create view tmp_jy11_target_01 as
select
    if(acquire_timestamp/1000-UNIX_TIMESTAMP(DATE_FORMAT(last_seen_time,'yyyy-MM-dd HH:mm:ss.SSS'))<=300,coalesce(seen_cnt,0), coalesce(seen_cnt,0)+1) as seen_cnt, --  计算 id ,时间小于 5分钟 批号加1
    -- if(TIMESTAMPDIFF(SECOND,UNIX_TIMESTAMP(from_unixtime(acquire_timestamp/1000)),last_seen_time) <=300,coalesce(seen_cnt,0), coalesce(seen_cnt,0)+1) as seen_cnt, --  计算 id ,时间小于 5分钟 批号加1
    t1.device_id as device_id,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_time,
    acquire_timestamp,
    device_name_join as device_name,
    target_id,
    longitude,
    latitude,
    altitude,
    heading,
    target_yaw as yaw,
    target_pitch as pitch,
    speed,
    x_distance,
    y_distance,
    status ,
    -- distance_udf(latitude,longitude,level1_overlay_lat,level1_overlay_lng)*1000 as level1_distance,
    -- distance_udf(latitude,longitude,level2_overlay_lat,level2_overlay_lng)*1000 as level2_distance,
    -- distance_udf(latitude,longitude,level3_overlay_lat,level3_overlay_lng)*1000 as level3_distance,
    -- level1_overlay_r,
    -- level2_overlay_r,
    -- level3_overlay_r,
    device_lng,
    device_lat,
    proctime
    -- overlay_id,
    -- overlay_name
from tmp_source_kafka_03 t1
         left join dim_batch_id_info FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.target_id = t2.batch_id and t1.device_id = t2.device_id
     -- left join three_level_overlay FOR SYSTEM_TIME AS OF t1.proctime as t3
     -- on 1 = t3.id
where product_key = '6dAYpNN4LTS';

create view tmp_jy11_target_02 as
select
    (seen_cnt*1000 + target_id) as id,
    seen_cnt,
    device_id,
    device_lng,
    device_lat,
    acquire_time,
    acquire_timestamp,
    device_name,
    target_id,
    longitude,
    latitude,
    altitude,
    heading,
    yaw,
    pitch,
    speed,
    x_distance,
    y_distance,
    status,
    -- case
    --   when level1_distance<level1_overlay_r then 1
    --   when level2_distance<level2_overlay_r then 2
    --   when level3_distance<level3_overlay_r then 3
    -- end as overlay_id,
    -- case
    --   when level1_distance<level1_overlay_r then '一级防护区'
    --   when level2_distance<level2_overlay_r then '二级防护区'
    --   when level3_distance<level3_overlay_r then '三级防护区'
    -- end as overlay_name,
    cast(null as int) as overlay_id,
    cast(null as string) as overlay_name,
    proctime
from tmp_jy11_target_01;



-- 处理 ncs30电侦查数据
create view tmp_ncs30_target_01 as
select
    device_id,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as acquire_time,
    coalesce(`time`,t1.acquire_timestamp) as acquire_timestamp,
    device_name_join as device_name,
    -- ncs30
    auv_id, -- 干扰车名称
    from_unixtime(`time`/1000,'yyyy-MM-dd HH:mm:ss') as `time` , -- 发现时间
    track_batch_number, -- 目标批号
    frequence , -- 侦测中心频率
    band_width, -- 侦测中心带宽
    amplitude , -- 信号幅度
    `position` , -- 目标信号方位
    threat_level, -- 威胁等级
    signal , -- 上行/下行信号（枚举：0-未知，1-上行信号，2-下行型号）
    manufacturer_number, -- 目标无人机厂家
    target_model_number, -- 目标无人机型号
    sim,  -- 目标类型（枚举：1-真实，2-模拟）
    device_lng,
    device_lat,
    proctime
from tmp_source_kafka_02 t1
where product_key = 'ZIUO1Qmupsw'
  and `time`>1750521634;


-- jy11 与 ncs30 融合


create view tmp_merge_01 as
select
    'A' as dataType,
    device_id,
    device_lng,
    device_lat,
    cast(id as string) as target_id,
    longitude as target_lng,
    latitude as target_lat,
    yaw as target_azimuth,
    acquire_timestamp as timestamp_millis
from tmp_jy11_target_02
union all
select
    'B' as dataType,
    device_id,
    device_lng,
    device_lat,
    track_batch_number as target_id,
    cast(null as double) as target_lng,
    cast(null as double) as target_lat,
    `position` as target_azimuth,
    acquire_timestamp as timestamp_millis
from tmp_ncs30_target_01;



create view tmp_merge_02 as
select
    cast(datas.a_targetId as bigint) as id,
    datas.a_deviceId as jy11_device_id,
    datas.a_deviceLat as jy11_device_lat,
    datas.a_deviceLon as jy11_device_lng,
    datas.a_targetLat as jy11_target_lat,
    datas.a_targetLon as jy11_target_lng,
    datas.a_targetAzimuth as jy11_yaw,
    from_unixtime(datas.a_time/1000,'yyyy-MM-dd HH:mm:ss') as jy11_acquire_time,
    datas.b_targetId as ncs30_track_batch_number,
    datas.b_deviceId as ncs30_device_id,
    datas.b_deviceLat as ncs30_device_lat,
    datas.b_deviceLon as ncs30_device_lng,
    datas.b_targetAzimuth as ncs30_device_yaw,
    from_unixtime( datas.b_time/1000,'yyyy-MM-dd HH:mm:ss') as ncs30_acquire_time,
    datas.azimuthDiff as yaw_diff,
    datas.scoreAzimuth as score_yaw,
    datas.scoreTotal  as score_total
from (
         select
             merge_udf(
                     dataType,
                     device_id,
                     device_lat,
                     device_lng,
                     target_id,
                     target_lat,
                     target_lng,
                     target_azimuth,
                     timestamp_millis
                 ) as datas
         from merge_jy11_ncs30
     )
where datas.a_targetId is not null;







create view tmp_merge_03 as
select
    id,
    jy11_device_id,
    track_batch_number,
    ncs30_device_id,
    id%1000 as target_id,
  cnt
from (
  select
    id,
    jy11_device_id,
    ncs30_track_batch_number as track_batch_number,
    ncs30_device_id,
    count(*) as cnt
  from merge_jy11_ncs30_list
  group by
    id,
    jy11_device_id,
    ncs30_track_batch_number,
    ncs30_device_id
) a;

-- 处理 sy600光电数据
create view tmp_sy600_target_01 as
select
    id,
    device_id,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as  acquire_time,
    device_name_join as device_name,
    yaw,
    pitch,
    track_status,
    type,
    if(type in ('3','4'),'无人机',cast( null as string))type_name,
    confidence
from tmp_source_kafka_02 t1
where product_key = 'WnJL27UN3sc';


-- 算法检测
create view tmp_object_detection_01 as
select
    target_id as id,
    device_id,
    from_unixtime(t1.acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as  acquire_time,
    device_name_join as device_name,
    -- 垂起固定翼无人机、多旋翼无人机、直升机、蜂群、御 2
    case target_type when '多旋翼无人机' then 1 when '固定翼无人机' then 2 when '直升机' then 3  when '蜂群' then 5 end as target_type,
    target_type as target_type_name,
    source_image
from tmp_source_kafka_03 t1
where product_key = 'aby554pqic0'
  and `method` = 'event.trackerBindingResult.info';



-- 插入结果表
begin statement set;


insert into merge_jy11_ncs30
select * from tmp_merge_01;

insert into merge_jy11_ncs30_list
select * from tmp_merge_02;

insert into dwd_bhv_jy11_target_rt
select
    id,
    device_id,
    acquire_time,
    device_name,
    target_id,
    longitude,
    latitude,
    altitude,
    heading,
    yaw,
    pitch,
    speed,
    x_distance,
    y_distance,
    status,
    overlay_id,
    overlay_name,
    from_unixtime(unix_timestamp()) as update_time
from tmp_jy11_target_02;



insert into doris_dim_batch_id_info
select
    target_id as batch_id, -- 批号
    device_id, -- 设备编号
    1 as device_type, -- 设备类型
    seen_cnt, -- 出现次数
    acquire_time as last_seen_time, -- 最后一次出现时间
    from_unixtime(unix_timestamp()) as upate_time  -- 更新时间
from tmp_jy11_target_02;

insert into dwd_bhv_ncs30_target_rt
select
    track_batch_number,
    device_id,
    coalesce(acquire_time,`time`) as acquire_time,
    device_name,
    auv_id,
    `position`,
    amplitude,
    signal,
    threat_level,
    manufacturer_number,
    target_model_number,
    '无人机' as target_type,
    frequence,
    band_width,
    sim,
    from_unixtime(unix_timestamp()) as update_time
from tmp_ncs30_target_01;


insert into dws_rl_jy11_ncs30_list
select
    ncs30_track_batch_number,
    ncs30_device_id,
    ncs30_acquire_time,
    ncs30_device_lat,
    ncs30_device_lng,
    ncs30_device_yaw,
    id,
    jy11_device_id,
    jy11_device_lat,
    jy11_device_lng,
    jy11_target_lat,
    jy11_target_lng,
    jy11_yaw,
    jy11_acquire_time,
    yaw_diff,
    score_yaw,
    score_total,
    from_unixtime(unix_timestamp()) as update_time
from merge_jy11_ncs30_list;

insert into dim_rl_jy11_ncs30_id
select
    id,
    jy11_device_id,
    track_batch_number,
    ncs30_device_id,
    target_id,
    cnt,
    from_unixtime(unix_timestamp()) as update_time
from tmp_merge_03;

insert into dwd_bhv_sy600_track_target_rt
select
    id,
    device_id,
    acquire_time,
    device_name,
    yaw,
    pitch,
    cast(track_status as int) as track_status,
    cast(type as int) as type,
    type_name,
    confidence,
    from_unixtime(unix_timestamp()) as update_time
from tmp_sy600_target_01;


insert into dwd_bhv_object_detection
select
    id,
    device_id,
    acquire_time,
    device_name,
    target_type,
    target_type_name,
    source_image,
    from_unixtime(unix_timestamp()) as update_time
from tmp_object_detection_01;



insert into uav_source
select
    cast(id as string) as  id                    , -- id
    '无人机' as name                  , -- 名称
    cast(null as string) as type                  , -- 类型
    cast(null as string) as manufacturer          , -- 厂商
    cast(null as string) as model                 , -- 型号
    cast(null as double) as ew                    , -- 航迹角
    altitude as height                , -- 无人机距地高度
    altitude as locationAlit          , -- 气压高度
    cast(null as double) as speedV                , -- 垂直速度
    speed as speedH                , -- 无人机地速,水平速度
    cast(null as string) recvtype              , -- 无人机数据类型
    cast(null as double) as rssi                  , -- 信号强度
    longitude as lng                   , -- 融合的经度 - 给大武规则
    latitude as lat                   , -- 维度的纬度 - 给大武规则
    longitude             , -- 原始经度 - 给抓捕
    latitude              , -- 原始纬度 - 给抓捕
    acquire_time as acquireTime           , -- 采集时间
    'UAV' as targetType            , -- 实体类型 固定值 UAV
    from_unixtime(unix_timestamp()) as updateTime             -- flink处理时间
from tmp_jy11_target_02;


end;









