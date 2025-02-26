--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/07/30 16:48:50
-- description: 飞机数据源合之后的数据全部写入doris
-- version: ja-rid-data-rt-v250215-dev
--********************************************************************--

set 'pipeline.name' = 'ja-rid-data-rt';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '3';

-- SET 'execution.checkpointing.interval' = '300000';
-- SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-rid-data-rt';

-- set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';

 -----------------------

 -- 数据结构

 -----------------------


-- 计算距离
create function distance_utf as 'com.jingan.udf.geohash.DistanceUdf';



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
                                             operator_info row<>
                                                 >
                                                 >

) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = '172.21.30.105:30090',
      -- 'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'iot-rid-data1',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1740153629000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- rid数据入库doris
create table dws_bhv_rid_rt (
                                rid_device_id              string  comment 'RID的设备id-牍术接入的',
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
    -- operator_id                string  comment '无用-可为空',
                                sys_timestamp              string  comment '无人机自己上报的自己的时间-可能不对的',
                                filter_col                 string  comment '动态筛选字段拼接',
                                update_time                string  comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.105:30030',
      'table.identifier' = 'sa.dws_bhv_rid_rt',
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
      'fenodes' = '172.21.30.105:30030',
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
      'fenodes' = '172.21.30.105:30030',
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



-- rid设备属性心跳
create table `dws_bhv_rid_heartbeat_rt` (
                                            rid_device_id   		string  comment 'RID的设备id-牍术接入的',
                                            acquire_time    		string  comment '采集时间',
                                            longitude				double  comment '经度',
                                            latitude				double  comment '经度',
                                            open_alarm_code		bigint  comment '是否开箱代码0:否，1:开箱（可能被偷）',
                                            open_alarm			string  comment '是否开箱，告警，正常',
                                            outpower_alarm_code	bigint  comment '电量是否告警 1:告警，0:正常',
                                            outpower_alarm		string  comment '电量是否告警，告警，正常',
                                            update_time         	string  comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.105:30030',
      'table.identifier' = 'sa.dws_bhv_rid_heartbeat_rt',
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
                                     rid_device_id   	   string  comment 'RID的设备id-牍术接入的',
                                     uav_id               string  comment '无人机的id-sn号',
                                     control_station_id   string  comment '控制站id',
                                     rid_devid       	   string  comment 'rid设备的id-飞机上报的',
                                     acquire_time         string  comment '采集时间',
                                     update_time          string  comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.105:30030',
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
      'url' = 'jdbc:mysql://172.21.30.105:31306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
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
                           updateTime            string -- flink处理时间
) with (
      'connector' = 'kafka',
      'topic' = 'uav_source',
      'properties.bootstrap.servers' = '172.21.30.105:30090',
      -- 'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'uav_source1',
      'key.format' = 'json',
      'key.fields' = 'id',
      'format' = 'json'
      );



-----------------------

-- 数据处理

-----------------------

-- 数据字段处理
create view temp01 as
select
    productKey                                          as product_key,
    deviceId                                            as rid_device_id,
    from_unixtime(message.`timestamp`/1000,'yyyy-MM-dd HH:mm:ss') acquire_time,
    -- message.`timestamp`                                 as acquire_timestamp,
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

    message.`data`.location_info.location_lon           as uav_longitude,         -- 无人机经度
    message.`data`.location_info.location_lat           as uav_latitude,         -- 无人机纬度
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
    message.`data`.system_info.sys_lon                  as control_station_longitude,
    message.`data`.system_info.sys_lat                  as control_station_latitude,
    message.`data`.system_info.sys_location_type        as sys_location_type,     -- 保留字
    message.`data`.system_info.sys_area_rad             as sys_area_rad,          -- 运行区域半径

    message.`data`.system_info.sys_coordinate_type      as sys_coordinate_type,
    message.`data`.system_info.sys_area_ceil            as sys_area_ceil,         -- 运行区域高度上限
    message.`data`.system_info.sys_area_floor           as sys_area_floor,        -- 运行区域高度下限
    message.`data`.system_info.sys_class                as sys_class,             -- 保留字
    message.`data`.system_info.sys_timestamp            as sys_timestamp,         -- 探测到无人机时间戳
    message.`data`.system_info.operator_type            as operator_type,         -- 描述类型 保留字
    message.`data`.system_info.operator_id              as operator_id,            -- 无用-可为空
    PROCTIME() as proctime
from iot_device_message_kafka_01
where message.`timestamp` is not null
  and productKey = 'iiCC6Y7Qhmz';



create view temp01_01 as
select
    t1.*
from (
         select
             *
         from temp01
         where `method` = 'event.ridMessage.info'
           and uav_id is not null
           and uav_id <> ''
           and uav_longitude > -180
           and uav_longitude < 180
           and uav_longitude <> 0
           and uav_latitude <> 0
     ) as t1
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 设备表 关联rid设备位置
                   on t1.rid_device_id = t2.device_id
where distance_utf(t1.uav_latitude,t1.uav_longitude,t2.latitude,t2.longitude) <= 15;




-- 筛选rid设备事件数据处理
create view temp02 as
select
    t1.product_key,
    t1.rid_device_id,
    t1.acquire_time,
    t1.`method`,

    t1.uav_longitude  as longitude,
    t1.uav_latitude  as latitude,
    t1.open_alarm,
    t1.outpower_alarm,
    t1.rid_devid,
    t1.msgtype,
    t1.recvtype,
    t1.recvmac,
    t1.mac,
    t1.rssi,
    REPLACE(REPLACE(t1.uav_id,'\t',''),'\n','') as uav_id,
    t1.basic_idtype,
    t1.basic_uatype,
    t1.speed_v,              -- 垂直速度
    t1.hori_acc,             -- 水平精度
    t1.location_direc,       -- 保留字
    t1.vert_acc,             -- 垂直精度
    t1.location_speed_multi, -- 保留字
    t1.height_type,          -- 高度类型
    t1.ew,                   -- 航迹角
    t1.height,               -- 无人机距地高度
    t1.location_status,      -- 保留字
    t1.speed_acc,            -- 速度精度
    t1.speed_h,              -- 无人机地速（以实际为准）
    t1.location_alit,        -- 气压高度

    t1.sys_classification,   -- 保留字
    t1.sys_area_count,       -- 运行区域计数
    t1.sys_category,         -- 保留字
    t1.control_station_longitude,
    t1.control_station_latitude,
    t1.control_station_height,
    t1.sys_location_type,     -- 保留字
    t1.sys_area_rad,          -- 运行区域半径
    t1.sys_coordinate_type,
    t1.sys_area_ceil,         -- 运行区域高度上限
    t1.sys_area_floor,        -- 运行区域高度下限
    t1.sys_class,             -- 保留字
    if(t1.sys_timestamp = '',cast(null as varchar),t1.sys_timestamp) as sys_timestamp, -- 探测到无人机时间戳
    t1.operator_type,         -- 描述类型 保留字
    t1.operator_id,            -- 无用-可为空

    t2.device_id              as join_dushu_uav_device_id,
    t2.name                   as join_dushu_uav_name,
    t2.manufacturer           as join_dushu_uav_manufacturer,
    t2.model                  as join_dushu_uav_model,
    t2.owner                  as join_dushu_uav_owner,
    t2.type                   as join_dushu_uav_type,
    t2.status                 as join_dushu_uav_status,
    concat('cs',t1.uav_id)    as control_station_id,
    'RID'                     as source
from temp01_01 as t1
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 设备表 关联无人机
                   on t1.uav_id = t2.sn and 'UAV' = t2.type;



-- 筛选rid设备心跳属性数据
create view temp03 as
select
    rid_device_id,
    latitude,
    longitude,
    open_alarm,
    outpower_alarm,
    acquire_time
from temp01
where `method` = 'event.property.post';



-----------------------

-- 数据写入

-----------------------


begin statement set;

-- ri、控制站、无人机关系表
insert into dws_rl_rid_uav_rt
select
    rid_device_id,
    uav_id,
    control_station_id,
    rid_devid,
    acquire_time,
    from_unixtime(unix_timestamp()) as update_time
from temp02;


-- 控制站实体表
insert into dws_et_control_station_info
select
    control_station_id     as id,
    acquire_time,
    uav_id                 as name, -- 无人机id-sn号
    uav_id                 as register_uav,
    source,
    uav_id                 as search_content,
    from_unixtime(unix_timestamp()) as update_time
from temp02;


-- 无人机实体表
insert into dws_et_uav_info
select
    uav_id                       as id,
    join_dushu_uav_device_id     as device_id,
    uav_id                       as sn,
    join_dushu_uav_name          as name,
    recvmac,
    join_dushu_uav_manufacturer  as manufacturer,
    join_dushu_uav_model         as model,
    join_dushu_uav_owner         as owner,
    join_dushu_uav_type          as type,
    source,
    -- cast(null as varchar)  as category,
    -- cast(null as varchar)  as phone,
    -- cast(null as varchar)  as type
    -- cast(null as varchar)  as empty_weight,
    -- cast(null as varchar)  as maximum_takeoff_weight,
    -- cast(null as varchar)  as purpose,
    concat(
            ifnull(join_dushu_uav_name,''),' ',
            ifnull(uav_id,'')
        )         as search_content,
    from_unixtime(unix_timestamp()) as update_time
from temp02;






-- rid设备的行为数据
insert into dws_bhv_rid_rt
select
    rid_device_id,
    uav_id,
    control_station_id,
    acquire_time,
    join_dushu_uav_device_id as uav_device_id,
    rid_devid,
    msgtype,
    recvtype ,
    mac,
    rssi,
    basic_uatype,
    basic_idtype,
    longitude,
    latitude,
    location_alit,
    ew,
    speed_h,
    speed_v,
    height,
    height_type,
    hori_acc,
    vert_acc,
    speed_acc,
    control_station_longitude,
    control_station_latitude,
    control_station_height,
    sys_area_count,
    sys_area_rad,
    sys_area_ceil,
    sys_area_floor,
    sys_classification,
    sys_category,
    sys_class,
    sys_location_type,
    sys_coordinate_type,
    location_direc,
    location_status,
    location_speed_multi,
    operator_type,
    -- operator_id,
    sys_timestamp,
    concat(
            ifnull(cast(longitude as varchar),''),'¥',
            ifnull(cast(latitude as varchar),''),'¥',
            ifnull(cast(location_alit as varchar),''),'¥',
            ifnull(cast(ew as varchar),''),'¥',
            ifnull(cast(height as varchar),''),'¥',
            ifnull(cast(speed_v as varchar),''),'¥',
            ifnull(cast(speed_h as varchar),''),'¥',
            ifnull(recvtype,''),'¥',
            ifnull(cast(rssi as varchar),''),'¥',
            ifnull(join_dushu_uav_status,''),'¥',
            ifnull(cast(control_station_longitude as varchar),''),'¥',
            ifnull(cast(control_station_latitude as varchar),''),'¥',
            ifnull(rid_devid,'')
        ) as filter_col,
    from_unixtime(unix_timestamp()) as update_time
from temp02;


-- rid设备属性心跳
insert into dws_bhv_rid_heartbeat_rt
select
    rid_device_id,
    acquire_time,
    longitude,
    latitude,
    open_alarm                      as open_alarm_code,
    if(open_alarm = 1,'告警','正常') as open_alarm,
    outpower_alarm                  as outpower_alarm_code,
    if(open_alarm = 1,'告警','正常') as outpower_alarm,
    from_unixtime(unix_timestamp()) as update_time
from temp03;


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
from temp02;


end;



