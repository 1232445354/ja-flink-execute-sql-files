----------------------
-- database：doris
-- remark：所有设备的轨迹表
----------------------

CREATE TABLE `dwd_device_track_rt` (
                                       `device_id` varchar(50) NULL COMMENT '上报的设备id',
                                       `acquire_timestamp_format` datetime NULL COMMENT '格式化时间',
                                       `device_type` varchar(50) NULL COMMENT '上报的设备类型',
                                       `acquire_timestamp` bigint(20) NULL COMMENT '上报时间戳',
                                       `attitude_head` decimalv3(30, 18) NULL COMMENT '无人机机头朝向',
                                       `gimbal_head` decimalv3(30, 18) NULL COMMENT '无人机云台朝向',
                                       `lng_02` decimalv3(30, 18) NULL COMMENT '经度—高德坐标系、火星坐标系',
                                       `lat_02` decimalv3(30, 18) NULL COMMENT '纬度—高德坐标系、火星坐标系',
                                       `lng_84` decimalv3(30, 18) NULL COMMENT '经度—84坐标系',
                                       `lat_84` decimalv3(30, 18) NULL COMMENT '纬度—84坐标系',
                                       `rectify_lng_lat` varchar(50) NULL COMMENT '高德坐标算法纠偏后经纬度',
                                       `username` varchar(50) NULL COMMENT '设备用户',
                                       `group_id` varchar(255) NULL COMMENT '组织id',
                                       `product_key` varchar(300) NULL COMMENT '产品key',
                                       `tid` varchar(300) NULL COMMENT 'tid',
                                       `bid` varchar(300) NULL COMMENT 'bid',
                                       `device_name` varchar(300) NULL COMMENT '设备名称',
                                       `start_time` datetime NULL COMMENT '开始时间（开窗）',
                                       `end_time` datetime NULL COMMENT '结束时间（开窗）',
                                       `type` varchar(50) NULL COMMENT 'POSITION 位置',
                                       `location_type` varchar(100) NULL COMMENT '位置类型',
                                       `record_id` varchar(50) NULL COMMENT '行为id',
                                       `create_by` varchar(300) NULL COMMENT '创建人',
                                       `update_time` datetime NULL COMMENT '更新插入时间（数据入库时间'
) ENGINE = OLAP UNIQUE KEY(`device_id`, `acquire_timestamp_format`) COMMENT '执法仪、tak的轨迹数据' PARTITION BY RANGE(`acquire_timestamp_format`) () DISTRIBUTED BY HASH(`device_id`) BUCKETS 10 PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.time_zone" = "Etc/UTC",
  "dynamic_partition.start" = "-360",
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.replication_allocation" = "tag.location.default: 3",
  "dynamic_partition.buckets" = "10",
  "dynamic_partition.create_history_partition" = "true",
  "dynamic_partition.history_partition_num" = "30",
  "dynamic_partition.hot_partition_num" = "0",
  "dynamic_partition.reserved_history_periods" = "NULL",
  "dynamic_partition.storage_policy" = "",
  "dynamic_partition.storage_medium" = "HDD",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);