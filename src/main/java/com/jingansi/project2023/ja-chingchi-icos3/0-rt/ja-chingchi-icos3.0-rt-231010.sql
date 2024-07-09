--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2023/3/21 14:06:19
-- description: 旌旗3.0设备接入日志、设备检测数据状态、轨迹等
--********************************************************************--

set 'pipeline.name' = 'ja-chingchi-icos3.0-rt';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '6';
-- SET 'execution.checkpointing.interval' = '600000';
-- SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-chingchi-icos3.0-rt-checkpoint' ;


-- 设备检测数据上报（Source：kafka）
drop table if exists iot_device_message_kafka;
create table iot_device_message_kafka (
                                          productKey    string     comment '产品编码',
                                          deviceId      string     comment '设备id',
                                          type          string     comment '设备检测数据-类型',
                                          version       string     comment '拍照数据上报-版本',
                                          `timestamp`   bigint     comment '拍照数据上报-时间戳毫秒',
                                          tid           string     comment '拍照数据上报',
                                          bid           string     comment '拍照数据上报-业务id',
                                          `method`      string     comment '拍照数据上报',
                                          `data`  row(
                                              pictureUrl       string   ,-- '拍照数据上报-图片url
                                              width            int      ,-- '拍照数据上报-宽度
                                              height           int       -- '拍照数据上报-高度
                                              ),
                                          message  row(
                                              tid                     string    , -- 机库子设备-无人机/设备检测目标数据-当前请求的事务唯一ID
                                              bid                     string    , -- 机库子设备-无人机/设备检测目标数据-长连接整个业务的ID
                                              version                 string    , -- 设备检测数据-版本
                                              `timestamp`             bigint    , -- 机库子设备-无人机/设备检测目标数据-时间戳
                                              `method`                string    , -- 机库子设备-无人机/设备检测目标数据-服务&事件标识
                                              productKey              string    , -- 设备检测目标数据-产品编码
                                              deviceId                string    , -- 设备检测目标数据-设备编码
                                              `data` row(
                                              flightId            string    , -- 媒体无人机数据
                                              fileName            string    , -- 媒体无人机数据
                                              objectKey           string    , -- 媒体无人机数据
                                              bucketName          string    , -- 媒体无人机数据
                                              fileType            bigint    , -- 媒体无人机数据
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
                                              targets array<
                                              row(
                                              targetId         bigint   ,-- 雷达检测数据上报-目标id
                                              xDistance        double   ,-- 雷达检测数据上报-x距离
                                              yDistance        double   ,-- 雷达检测数据上报-y距离
                                              speed            double   ,-- 雷达检测数据上报-速度
                                              targetPitch      double   ,-- 雷达检测数据上报-俯仰角
                                              targetAltitude   double   ,-- 雷达检测数据上报-目标海拔高度
                                              status           string   ,-- 雷达检测数据上报-0 目标跟踪 1 目标丢失 2 跟踪终止
                                              targetLongitude  string   ,-- 雷达检测数据上报-目标经度
                                              targetLatitude   string    -- 雷达检测数据上报-目标纬度
                                              -- `action`         string   ,-- 设备日志
                                              -- `system:status`  string   ,-- 设备日志-枚举
                                              -- `system:progress`  row(
                                              --     `system:percent`        double   ,-- 设备日志-执行进度 ,单位%
                                              --     `system:stepKey`        string   ,-- 设备日志-当前步骤 -枚举
                                              --     `system:stepResult`     string   ,-- 设备日志-当前步骤结果
                                              --     `system:message`        string    -- 设备日志-当前执行步骤信息
                                              -- )
                                              )
                                              >
                                              )
                                              )
) WITH (
      'connector' = 'kafka',
      'topic' = 'iot-device-message',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'iot-device-message-rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1682938851000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 媒体拍照数据入库（Sink：mysql）
drop table if exists device_media_datasource_mysql;
create table device_media_datasource_mysql (
                                               device_id                      string        comment '设备编码',
                                               source_id                      int           comment '来源id/应用id',
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
                                               PRIMARY KEY (tid,device_id,start_time) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device_media_datasource',
      'sink.buffer-flush.interval' = '1s',
      'sink.buffer-flush.max-rows' = '20'
      );


-- 设备检测目标全量数据入库（Sink：doris）
drop table  if exists device_inspection_target_all_rt_doris;
create table device_inspection_target_all_rt_doris(
                                                      device_id          string     comment '设备id',
                                                      target_id          bigint     comment '目标id',
                                                      timestamp_format   string     comment '时间戳格式化',
                                                      tid                string     comment '',
                                                      bid                string     comment '',
                                                      acquire_timestamp  bigint     comment '时间戳',
                                                      x_distance         double     comment 'x距离',
                                                      y_distance         double     comment 'y距离',
                                                      speed              double     comment '目标速度',
                                                      status             string     comment '0 目标跟踪 1 目标丢失 2 跟踪终止',
                                                      target_altitude    double     comment '目标海拔高度',
                                                      target_longitude   string     comment '目标经度',
                                                      target_latitude    string     comment '目标维度',
                                                      target_pitch       double     comment '俯仰角',
                                                      `method`           string     comment '',
                                                      product_key        string     comment '',
                                                      update_time        string     comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.21.30.105:30030',
     'table.identifier' = 'dushu.device_inspection_target_all_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='5000',
     'sink.batch.interval'='10s'
-- 'sink.properties.escape_delimiters' = 'false'，
-- 'sink.properties.column_separator' = '\x01',	 -- 列分隔符
-- 'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
-- 'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 设备检测目标状态数据入库（Sink：doris）
drop table  if exists device_inspection_target_status_rt_doris;
create table device_inspection_target_status_rt_doris(
                                                         device_id          string     comment '设备id',
                                                         target_id          bigint     comment '目标id',
                                                         timestamp_format   string     comment '时间戳格式化',
                                                         tid                string     comment '',
                                                         bid                string     comment '',
                                                         acquire_timestamp  bigint     comment '时间戳',
                                                         x_distance         double     comment 'x距离',
                                                         y_distance         double     comment 'y距离',
                                                         speed              double     comment '目标速度',
                                                         status             string     comment '0 目标跟踪 1 目标丢失 2 跟踪终止',
                                                         target_altitude    double     comment '目标海拔高度',
                                                         target_longitude   string     comment '目标经度',
                                                         target_latitude    string     comment '目标维度',
                                                         target_pitch       double     comment '俯仰角',
                                                         `method`           string     comment '',
                                                         product_key        string     comment '',
                                                         update_time        string     comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.21.30.105:30030',
     'table.identifier' = 'dushu.device_inspection_target_status_rt',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='5000',
     'sink.batch.interval'='5s'
     );


-- 设备状态上报数据入库（无人机子设备、无人机、执法仪）（Sink：mysl）
drop table if exists device_properties_history_record;
create table device_properties_history_record (
                                                  product_key                    string        comment '产品编码',
                                                  device_id                      string        comment '设备编码',
                                                  tid                            string        comment '当前请求的事务唯一ID',
                                                  bid                            string        comment '长连接整个业务的ID',
                                                  `key`                          string        comment 'key枚举值',
                                                  `value`                         string        comment 'value值',
                                                  gmt_create                     string        comment '创建时间',
                                                  gmt_create_by                  string        comment '创建人',
                                                  gmt_modified_by                string        comment '更新人',
                                                  PRIMARY KEY (tid,`key`) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device_properties_history_record',
      'sink.buffer-flush.max-rows'='5000',
      'sink.buffer-flush.interval'='5s'
      );


-----------------------

-- 数据处理

-----------------------

-- kafka来源的所有数据解析
drop table if exists tmp_source_kafka_01;
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
    `data`.pictureUrl as picture_url,
    `data`.width  as width,
    `data`.height as height,
    message.tid as message_tid,
    message.bid as message_bid,
    message.version as message_version,
    message.`timestamp` as message_acquire_timestamp,
    message.productKey as message_product_key,
    message.deviceId as message_device_id,
    message.`method` as message_method,
    split_index(message.`method`,'.',0) as method0,
    split_index(message.`method`,'.',1) as method1,
    split_index(message.`method`,'.',2) as method2,
    message.`data`.targets     as targets,
    message.`data`.flightId    as flight_id,
    message.`data`.fileName    as file_name,
    message.`data`.objectKey   as object_key,
    message.`data`.bucketName  as bucket_name,
    message.`data`.fileType    as file_type,
    message.`data`.longitude   as longitude,
    message.`data`.latitude    as latitude,
    message.`data`.height      as height,
    message.`data`.attitudeHead               as attitude_head,
    message.`data`.verticalSpeed              as vertical_speed,
    message.`data`.horizontalSpeed            as horizontal_speed,
    message.`data`.battery.capacityPercent    as capacity_percent,
    message.`data`.electricity                as electricity,
    message.`data`.gimbalHead                 as gimbal_head
    -- message.`data`.targetId as target_id,
    -- message.`data`.xDistance as x_distance,
    -- message.`data`.yDistance as y_distance,
    -- message.`data`.speed as speed,
    -- message.`data`.status as status,
    -- message.`data`.targetAltitude as target_altitude,
    -- message.`data`.targetLongitude as target_longitude,
    -- message.`data`.targetLatitude as target_latitude,
    -- message.`data`.targetPitch as target_pitch,
    -- message.`data`.`action` as `action`,
    -- message.`data`.`system:status` as system_status,
    -- message.`data`.`system:progress`.`system:percent` as system_percent,
    -- message.`data`.`system:progress`.`system:stepKey` as system_step_key,
    -- message.`data`.`system:progress`.`system:stepResult` as system_step_result,
    -- message.`data`.`system:progress`.`system:message` as system_message
from iot_device_message_kafka;


-- 设备(雷达)检测数据筛选处理
drop table if exists tmp_source_kafka_02;
create view tmp_source_kafka_02 as
select
    message_tid,
    message_bid,
    message_method,
    message_product_key,
    message_device_id,
    message_acquire_timestamp,
    targets
from tmp_source_kafka_01
where message_acquire_timestamp is not null
  and message_method = 'event.targetInfo.info'
  and message_product_key = 'k8dNIRut1q3';


-- 设备检测数据（雷达）数据进一步解析数组
drop table if exists tmp_source_kafka_03;
create view tmp_source_kafka_03 as
select
    t1.message_tid,
    t1.message_bid,
    t1.message_method,
    t1.message_product_key,
    t1.message_device_id,
    t1.message_acquire_timestamp,
    t2.targetId          as target_id,
    t2.xDistance         as x_distance,
    t2.yDistance         as y_distance,
    t2.speed             as speed,
    t2.targetPitch       as target_pitch,
    t2.targetAltitude    as target_altitude,
    t2.status            as status,
    t2.targetLongitude   as target_longitude,
    t2.targetLatitude    as target_latitude
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
                                            targetLatitude
    );


-- 机库子设备/无人机/执法仪-数据筛选处理
drop table if exists tmp_source_kafka_04;
create view tmp_source_kafka_04 as
select
    product_key,
    device_id,
    message_tid,
    message_bid,
    message_product_key,
    message_device_id,
    message_acquire_timestamp,
    longitude,
    latitude,
    map [
        'longitude'          , cast(longitude as varchar),
    'latitude'           , cast(latitude as varchar),
    'height'             , cast(height as varchar),
    'electricity'        , cast(if(capacity_percent is not null,capacity_percent,electricity) as varchar) ,
    'attitudeHead'       , cast(attitude_head as varchar),
    'verticalSpeed'      , cast(vertical_speed as varchar),
    'horizontalSpeed'    , cast(horizontal_speed as varchar),
    'gimbalHead'         , cast(gimbal_head as varchar)
    ] as index_key_value
from tmp_source_kafka_01
  where message_method = 'properties.state'
    and product_key in('00000000002','zyrFih3kept','QptZJHOd1KD');


-----------------------

-- 数据插入

-----------------------


begin statement set;

-- 媒体数据手动拍照数据入库
insert into device_media_datasource_mysql
select
    device_id            as device_id,
    -1                   as source_id,
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
    'ja-flink-control-center' as gmt_create_by,
    'ja-flink-control-center' as gmt_modified_by
from tmp_source_kafka_01
where acquire_timestamp is not null
  and `method` = 'platform.capture.post';
-- and product_key = '00000000001';


-- 媒体无人机数据数据入库
insert into device_media_datasource_mysql
select
    message_device_id            as device_id,
    cast(null as int)            as source_id,
    cast(null as string)         as source_name,
    if(file_type = 2,'HISTORY_VIDEO','PICTURE')  as type,
    from_unixtime(message_acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as start_time,
    from_unixtime(message_acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as end_time,
    concat(bucket_name,file_name)          as url,
    0 as width,
    0 as height,
    flight_id as bid,
    message_tid as tid,
    'WAYLINE_TASK' as b_type,
    '{}'                  as extends,
    from_unixtime(unix_timestamp()) as gmt_create,
    'ja-flink-control-center' as gmt_create_by,
    'ja-flink-control-center' as gmt_modified_by
from tmp_source_kafka_01
where acquire_timestamp is not null
  and message_method = 'event.mediaFileUpload.info'
  and message_product_key = '00000000001';


-- 设备(雷达)检测全量数据入库doris
insert into device_inspection_target_all_rt_doris
select
    message_device_id as device_id,
    target_id,
    from_unixtime(message_acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as timestamp_format,
    message_tid as tid,
    message_bid as bid,
    message_acquire_timestamp as acquire_timestamp,
    x_distance,
    y_distance,
    speed,
    status,
    target_altitude,
    target_longitude,
    target_latitude,
    target_pitch,
    message_method as `method`,
    message_product_key as product_key,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_03;


-- 设备(雷达)检测状态数据入库doris
insert into device_inspection_target_status_rt_doris
select
    message_device_id as device_id,
    target_id,
    from_unixtime(message_acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as timestamp_format,
    message_tid as tid,
    message_bid as bid,
    message_acquire_timestamp as acquire_timestamp,
    x_distance,
    y_distance,
    speed,
    status,
    target_altitude,
    target_longitude,
    target_latitude,
    target_pitch,
    message_method as `method`,
    message_product_key as product_key,
    from_unixtime(unix_timestamp()) as update_time
from tmp_source_kafka_03;


-- 设备状态数据上报入库mysql（机库子设备、无人机）
insert into device_properties_history_record
select
    t1.product_key,
    t1.device_id,
    t1.message_tid as tid,
    t1.message_bid as bid,
    t2.`key`,
    t2.`value`,
    from_unixtime(message_acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') gmt_create,
    'ja-flink-control-center' as gmt_create_by,
    'ja-flink-control-center' as gmt_modified_by
from tmp_source_kafka_04 as t1
         cross join unnest (index_key_value) as t2 (
                                                    `key`,
                                                    `value`
    )
where t2.`value` is not null;

end;



-- -- 设备日志数据入库（Sink：mysl）
-- drop table if exists iot_device_operation_log_mysql;
-- create table iot_device_operation_log_mysql (
--     timestamp_format               string        comment '时间戳格式化',
--     product_key                    string        comment '产品编码',
--     device_id                      string        comment '设备编码',
--     tid                            string        comment '当前请求的事务唯一ID',
--     bid                            string        comment '长连接整个业务的ID',
--     `method`                       string        comment '服务&事件标识',
--     -- log_type                       string        comment '日志类型:properties、events、services',
--     content                        string        comment '日志内容',
--     log_extend                     string        comment '拓展字段',
--     `action` 					   string		 comment '开关',
--   	system_status				   string		 comment '执行状态',
--   	system_percent			       double        comment '执行进度',
-- 	system_step_key			       string  		 comment '当前步骤',
-- 	system_step_result		       string		 comment '当前步骤结果',
-- 	system_message			       string		 comment '当前执行步骤信息',
--     gmt_create                     string        comment '创建时间',
--     gmt_create_by                  string        comment '创建人',
--     gmt_modified_by                string        comment '更新人',
--     PRIMARY KEY (tid) NOT ENFORCED
-- ) with (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
--     'driver' = 'com.mysql.cj.jdbc.Driver',
--     'username' = 'root',
--     'password' = 'jingansi110',
--     'table-name' = 'iot_device_operation_log',
--     'sink.buffer-flush.max-rows'='5000',
--     'sink.buffer-flush.interval'='5s'
-- );


-- -- 设备日志信息关联获取对应枚举值（Source：mysql）
-- drop table if exists iot_function_param;
-- create table iot_function_param (
--     id                             bigint        comment '自增主键',
--     product_id                     bigint        comment '产品ID',
--     module_id                      bigint        comment '模块ID',
--     function_id                    bigint        comment '功能ID',
--     param_name                     string        comment '参数名称',
--     param_identifier               string        comment '参数标识',
--     param_type                     string        comment '参数类型：属性，input，output',
--     data_type                      string        comment '数据类型：boolen,int，float，double，string,arr,enum,json',
--     specs                          string        comment '参数值',
--     required                       int           comment '必填参数 0 非必填 1必填',
--     PRIMARY KEY (id) NOT ENFORCED
-- ) with (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
--     'driver' = 'com.mysql.cj.jdbc.Driver',
--     'username' = 'root',
--     'password' = 'jingansi110',
--     'table-name' = 'iot_function_param',
--     'lookup.cache.max-rows' = '5000',
--     'lookup.cache.ttl' = '3600s',
--     'lookup.max-retries' = '3'
-- );


-- -- 设备日志信息关联获取对应模版id（Source：mysql）
-- drop table if exists iot_function;
-- create table iot_function (
--     id                             bigint        comment '',
--     product_id                     bigint        comment '产品ID',
--     function_name                  string        comment '功能名称',
--     function_identifier            string        comment '功能标识',
--     PRIMARY KEY (id) NOT ENFORCED
-- ) with (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
--     'driver' = 'com.mysql.cj.jdbc.Driver',
--     'username' = 'root',
--     'password' = 'jingansi110',
--     'table-name' = 'iot_function',
--     'lookup.cache.max-rows' = '5000',
--     'lookup.cache.ttl' = '3600s',
--     'lookup.max-retries' = '3'
-- );



-- -- 筛选日志数据
-- drop table if exists tmp_device_log_01;
-- create view tmp_device_log_01 as
-- select
--   message_device_id                           ,
--   message_product_key                         ,
--   message_acquire_timestamp                   ,
--   message_bid                                 ,
--   message_tid                                 ,
--   message_method                              ,
--   method1                                     ,
--   `action`                                    ,
--   system_status                               ,
--   system_percent                              ,
--   system_step_key                             ,
--   system_step_result                          ,
--   system_message                              ,
--   PROCTIME()  as proctime          -- 维表关联的时间函数
-- from tmp_source_kafka_01
--  where message_acquire_timestamp is not null
--    and method0 = 'service'
--    and method2 = 'progress';


-- -- 日志数据进行关联模版表取出模版id
-- drop table if exists tmp_device_log_02;
-- create view tmp_device_log_02 as
-- select
--   t1.message_device_id   , -- 设备id
--   t1.message_product_key ,
--   t1.message_acquire_timestamp    , -- 时间戳格式化
--   t1.message_bid                  , -- 业务id
--   t1.message_tid                  ,
--   t1.message_method               ,
--   t1.`action`            , -- 开关
--   t1.system_status       , -- 执行状态
--   t1.system_percent      , -- 执行进度
--   t1.system_step_key     , -- 当前步骤
--   t1.system_step_result  , -- 当前步骤结果
--   t1.system_message      , -- 当前执行步骤信息
--   t1.proctime            , -- 维表关联的时间
--   t2.id                    -- 模版id
-- from tmp_device_log_01 as t1
-- left join iot_function
-- FOR SYSTEM_TIME AS OF t1.proctime as t2
-- on method1 = t2.function_identifier
-- and t1.message_product_key = cast(t2.product_id as varchar);


-- -- 日志数据拿到模版id之后关联枚举表取值
-- drop table if exists tmp_device_log_03;
-- create view tmp_device_log_03 as
-- select
--   t1.message_device_id                      , -- 设备id
--   t1.message_product_key                    , -- 产品编码
--   t1.message_acquire_timestamp              , -- 时间戳格式化
--   t1.message_bid                            , -- 长连接整个业务的ID
--   t1.message_tid                            , -- 当前请求的事务唯一ID
--   t1.`action`                               , -- 开关
--   t1.message_method                         , -- 服务&事件标识
--   t1.system_status                          , -- 执行状态
--   t1.system_percent                         , -- 执行进度
--   t1.system_step_key                        , -- 当前步骤
--   t1.system_step_result                     , -- 当前步骤结果
--   t1.system_message                         , -- 当前执行步骤信息
--   t1.proctime                               , -- 维表关联的时间函数
--   t1.id                                     , -- 模版id
--   t2.specs as status_specs                  , -- 执行状态-枚举值
--   t2.param_name as status_param_name        , -- 执行状态-name
--   t3.specs as stepKey_specs                 , -- 当前步骤-枚举值
--   t3.param_name as stepKey_param_name       , -- 当前步骤-name
--   t4.specs as stepResult_specs              , -- 当前步骤结果-枚举值
--   t4.param_name as stepResult_param_name     -- 当前步骤结果-name
-- from tmp_device_log_02 as t1
-- left join iot_function_param
--   FOR SYSTEM_TIME AS OF t1.proctime as t2
--   on t1.id = t2.function_id    -- 关联取出system:status执行状态的枚举值
--   and 'system:status' = t2.param_identifier
-- left join iot_function_param
--   FOR SYSTEM_TIME AS OF t1.proctime as t3
--   on t1.id = t3.function_id    -- 关联取出system:stepKey当前步骤的枚举值
--   and 'system:stepKey' = t3.param_identifier
-- left join iot_function_param
--   FOR SYSTEM_TIME AS OF t1.proctime as t4
--   on t1.id = t4.function_id    -- 关联取出system:stepResult当前步骤结果的枚举值
--   and 'system:stepResult' = t4.param_identifier;


-- -- 日志数据拿到枚举值get中文
-- drop table if exists tmp_device_log_04;
-- create view tmp_device_log_04 as
-- select
--   message_device_id                      , -- 设备id
--   message_product_key                    ,
--   message_acquire_timestamp              , -- 时间戳格式化
--   message_bid                            , -- 业务id
--   message_tid                            ,
--   message_method                         ,
--   `action`                               , -- 开关
--   system_status                          , -- 执行状态
--   system_percent                         , -- 执行进度
--   system_step_key                        , -- 当前步骤
--   system_step_result                     , -- 当前步骤结果
--   system_message                         , -- 当前执行步骤信息
--   proctime                               , -- 维表关联的时间函数
--   id                                     , -- 模版id

--   status_specs                           , -- 执行状态-枚举值
--   status_param_name                      , -- 执行状态-name
--   json_value(status_specs,concat('$.',system_status)) as status_commant_chinese,   -- 执行状态-对应的指令

--   stepKey_specs                          , -- 当前步骤-枚举值
--   stepKey_param_name                     , -- 当前步骤-name
--   json_value(stepKey_specs,concat('$.',system_step_key)) as stepKey_commant_chinese, -- 当前步骤-对应的指令

--   stepResult_specs                       , -- 当前步骤结果-枚举值
--   stepResult_param_name                  , -- 当前步骤结果-name
--   json_value(stepResult_specs,concat('$.',system_step_result)) as stepResult_commant_chinese -- 当前步骤-对应的指令
-- from tmp_device_log_03;




-- -- 模型检测数据入拍照数据表
-- insert into device_media_datasource_mysql
-- select
--     id as device_id,
--     cast(source_id as int) as source_id,
--     -- source_name,
--     'PICTURE' as type,
--     ntp_timestamp_format as start_time,
--     ntp_timestamp_format as end_time,
--     originPictureUrl as url,
--     bbox_width as width,
--     bbox_height as height,
--     cast(null as varchar) as bid,
--     cast(null as varchar) as b_type,
--     row(
--         type,
--         originWidth,
--         originHeight,
--         targetOfOriginLeft,
--         targetOfOriginTop,
--         originPictureUrl
--     ) as extends,
--     from_unixtime(unix_timestamp()) as gmt_create,
--     'ja-flink-control-center' as gmt_create_by,
--     'ja-flink-control-center' as gmt_modified_by
-- from tmp_frame_infer_data_external_01;



-- 日志数据入库mysql
-- insert into iot_device_operation_log_mysql
-- select
--   from_unixtime(message_acquire_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as timestamp_format                       , -- 时间戳格式化
--   message_product_key as product_key          , -- 产品编码
--   message_device_id as  device_id             , -- 设备id
--   message_bid                                    , -- 业务id
--   message_tid                                    ,
--   message_method as `method`                     ,
--    concat(system_message,
--       ':执行状态->', if(status_commant_chinese is not null,status_commant_chinese,system_status),
--       ',执行进度->',cast(system_percent as varchar),
--       '%,当前步骤->',if(stepKey_commant_chinese is not null,stepKey_commant_chinese,system_step_key),
--       ',当前步骤结果->',if(stepResult_commant_chinese is not null,stepResult_commant_chinese,system_step_result)
--     ) as content,
--   cast(null as varchar) as log_content,
--   `action`                               , -- 开关
--   system_status                          , -- 执行状态
--   system_percent                         , -- 执行进度
--   system_step_key                        , -- 当前步骤
--   system_step_result                     , -- 当前步骤结果
--   system_message                         , -- 当前执行步骤信息
--   from_unixtime(unix_timestamp()) as gmt_create,
--   'ja-flink-contril-center' as gmt_create_by,
--   'ja-flink-contril-center' as gmt_modified_by
-- from tmp_device_log_04;

-- -- 模型检测的数据处理进拍照信息表，数据展开kafka-topic
-- drop view if exists tmp_frame_infer_data_external_01;
-- create view tmp_frame_infer_data_external_01 as
-- select
--     a.batch_id,
--     a.frame_num,
--     a.pts,
--     a.ntp_timestamp,
--     from_unixtime(a.ntp_timestamp/1000,'yyyy-MM-dd HH:mm:ss') as ntp_timestamp_format, -- 时间戳格式化
--     a.source_id,
--     a.source_frame_width as originWidth,
--     a.source_frame_height as originHeight,
--     a.infer_done,
--     a.image_path  as originPictureUrl,
--     a.frame_tensor_list,
--     a.user_meta.`dateTime`,
--     a.user_meta.id,
--     a.user_meta.imageUrl,
--     a.user_meta.`name`,
--     t.object_id,
--     t.object_label,
--     t.infer_id,
--     t.class_id,
--     t.bbox_left as targetOfOriginLeft,
--     t.bbox_top as targetOfOriginTop,
--     t.bbox_width,
--     t.bbox_height,
--     t.confidence,
--     t.image_path,
--    'IMAGE_MATTING' as type
-- from  frame_infer_data_external_test a
-- cross join unnest (object_list) as t (
--     object_id,
--     object_label,
--     infer_id,
--     class_id,
--     bbox_left,
--     bbox_top,
--     bbox_width,
--     bbox_height,
--     confidence,
--     image_path
-- )
-- where a.ntp_timestamp is not null;



-- -- kafka来源的模型数据（Source：kafka）
-- drop table if exists frame_infer_data_external_test;
-- create table frame_infer_data_external_test (
--     batch_id                bigint,                        -- 批处理ID
--     frame_num               int,                           -- 帧编号
--     pts                     bigint,                        -- pts 值
--     ntp_timestamp           bigint,                        -- 时间戳
--     source_id               string,                        -- 数据源ID
--     source_frame_width      int,                           -- 原始帧宽度
--     source_frame_height     int,                           -- 原始帧高度
--     infer_done              boolean,
--     image_path              string,                        -- 大图图片存储路径
--     frame_tensor_list       string,                        -- 输出基于帧的特征向量
--     object_list             array<
--         row(
--                 object_id           bigint,                -- 目标ID
--                 object_label        string,                -- 目标类型(cat,dog,mouse,smoke,naked)
--                 infer_id            int,                   -- 推理算子ID
--                 class_id            int,
--                 bbox_left           int,                   -- 左上角坐标
--                 bbox_top            int,                   -- 左上角坐标
--                 bbox_width          int,                   -- 目标宽度
--                 bbox_height         int,                   -- 目标高度
--                 confidence          decimal(20,18),        -- 目标置信度
--                 image_path          string                 -- 检测目标截取图片存储地址
--             )
--     >,
--      user_meta
--           row(
--               dateTime             string,
--               id                   string,
--               imageUrl             string,
--               name                 string
--           )
-- ) WITH (
--   'connector' = 'kafka',
--   'topic' =  'test_infer_result',
--   'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
--   'properties.group.id' = 'test-infer-result-rt',
--     -- 'scan.startup.mode' = 'latest-offset',
--   'scan.startup.mode' = 'timestamp',
--   'scan.startup.timestamp-millis' = '0',
--   'format' = 'json',
--   'json.fail-on-missing-field' = 'false',
--   'json.ignore-parse-errors' = 'true'
--   -- 'flink.kafka.poll-timeout' = '600000',
--   -- 'flink.kafka.consumer.properties.session.timeout.ms' = '600000'
-- );


-- -- 媒体数据手动拍照数据筛选处理
-- drop table if exists tmp_device_01;
-- create view tmp_device_01 as
-- select
--   in_device_id              as device_id,
--   -1                        as source_id,
--   cast(null as string)      as source_name,
--   'PICTURE'                 as type,
--   tid                       as tid,
--   from_unixtime(`timestamp`/1000,'yyyy-MM-dd HH:mm:ss') as timestamp_format,
--   data.pictureUrl           as url,
--   data.width                as width,
--   data.height               as height,
--   data.bid                  as bid,
--   data.bType                as b_type,
--   '{}'                      as extends,
--   from_unixtime(unix_timestamp()) as gmt_create,
--   'ja-flink-control-center' as gmt_create_by,
--   'ja-flink-control-center' as gmt_modified_by
-- from tmp_source_kafka_01
-- where `method` = 'platform.capture.post'
--   and in_product_key = '00000000001';





