CREATE TABLE `dwd_radar_target_all_rt` (
                                           `device_id` VARCHAR(200) NULL COMMENT '设备id:雷达ID,可见光红外id,震动器id',
                                           `target_id` VARCHAR(200) NULL COMMENT '目标id',
                                           `parent_id` VARCHAR(300) NULL COMMENT '父设备的id,也就是望楼id',
                                           `acquire_timestamp_format` DATETIME NULL COMMENT '上游程序上报时间戳-时间戳格式化',
                                           `acquire_timestamp` BIGINT NULL COMMENT '采集时间戳毫秒级别，上游程序上报时间戳',
                                           `source_type` VARCHAR(300) NULL COMMENT '数据设备来源名称',
                                           source_type_name VARCHAR(300) NULL COMMENT '数据设备来源名称,就是设备类型,使用product_key区分的',
                                           `device_name` VARCHAR(300) NULL COMMENT '设备名称',
                                           `device_info` VARCHAR(10000) NULL COMMENT '数据检测的来源[{deviceName,targetId,type}]',
                                           `object_label` VARCHAR(300) NULL COMMENT '目标类型',
                                           `x_distance` DOUBLE NULL COMMENT 'x距离',
                                           `y_distance` DOUBLE NULL COMMENT 'y距离',
                                           `speed` DOUBLE NULL COMMENT '目标速度',
                                           `status` VARCHAR(200) NULL COMMENT '0 目标跟踪 1 目标丢失 2 跟踪终止',
                                           `target_altitude` DOUBLE NULL COMMENT '目标海拔高度',
                                           `longitude` DOUBLE NULL COMMENT '目标经度',
                                           `latitude` DOUBLE NULL COMMENT '目标维度',
                                           `target_pitch` DOUBLE NULL COMMENT '俯仰角',
                                           `target_yaw` DOUBLE NULL COMMENT '水平角,新版本加入',
                                           `distance` DOUBLE NULL COMMENT '距离，新雷达的距离，没有了x距离和y距离 m',
                                           `utc_time` BIGINT NULL COMMENT '雷达上报的时间',
                                           `tracked_times` DOUBLE NULL COMMENT '已跟踪次数',
                                           `loss_times` DOUBLE NULL COMMENT '连续丢失次数',
                                           `target_credibility` DOUBLE NULL COMMENT '振动仪的字段',
                                           `time1` DATETIME NULL COMMENT '振动仪的字段',
                                           `tid` VARCHAR(200) NULL COMMENT '当前请求的事务唯一ID',
                                           `bid` VARCHAR(200) NULL COMMENT '长连接整个业务的ID',
                                           `method` VARCHAR(200) NULL COMMENT '服务&事件标识',
                                           `product_key` VARCHAR(200) NULL COMMENT '产品编码',
                                           `version` VARCHAR(100) NULL COMMENT '版本',
                                           `update_time` DATETIME NULL COMMENT '数据入库时间'
) ENGINE=OLAP
UNIQUE KEY(`device_id`, `target_id`, `parent_id`, `acquire_timestamp_format`)
COMMENT '雷达-振动仪设备检测数据全部入库'
PARTITION BY RANGE(`acquire_timestamp_format`)()
DISTRIBUTED BY HASH(`device_id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.time_zone" = "Etc/UTC",
"dynamic_partition.start" = "-1000",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.replication_allocation" = "tag.location.default: 3",
"dynamic_partition.buckets" = "2",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "400",
"dynamic_partition.hot_partition_num" = "0"
);




alter table dwd_radar_target_all_rt add column
    source_type_name VARCHAR(300) COMMENT '数据设备来源名称,就是设备类型,使用product_key区分的'
   after source_type;

update dwd_radar_target_all_rt
set source_type_name = device_name
where 1 = 1;

alter table dwd_radar_target_all_rt add column
    `device_info` VARCHAR(10000) NULL COMMENT '数据检测的来源[{deviceName,targetId,type}]',
   after source;

update dwd_radar_target_all_rt
set device_info = source
where 1 =1;

alter table dwd_radar_target_all_rt drop column create_by;


alter table dwd_radar_target_all_rt add column
    source_type_name VARCHAR(300) COMMENT '数据设备来源名称,就是设备类型,使用product_key区分的'
   after source_type;

update dwd_radar_target_all_rt
set source_type_name = device_name
where 1 = 1;

alter table dwd_radar_target_all_rt add column
    `device_info` VARCHAR(10000) NULL COMMENT '数据检测的来源[{deviceName,targetId,type}]',
   after source;

update dwd_radar_target_all_rt
set device_info = source
where 1 =1;

alter table dwd_radar_target_all_rt drop column create_by;