----------------------
-- database：doris
-- remark：无人机的飞行轨迹
----------------------

CREATE TABLE `dwd_device_index_all_rt` (
                                           `device_id` VARCHAR(50) NULL COMMENT '上报的设备id',
                                           `acquire_timestamp_format` DATETIME NULL COMMENT '时间戳格式化',
                                           `record_id` VARCHAR(50) NULL COMMENT '行为id or 事件id',
                                           `acquire_timestamp` BIGINT NULL COMMENT '上报时间戳',
                                           `type` VARCHAR(50) NULL COMMENT 'ELECTRIC 电量 POSITION 位置 AGL_ALTITUDE 高度',
                                           `value` VARCHAR(50) NULL COMMENT '值',
                                           `location_type` VARCHAR(50) NULL COMMENT '位置类型',
                                           `device_type` VARCHAR(50) NULL COMMENT '上报的设备类型',
                                           `username` VARCHAR(50) NULL COMMENT '设备用户',
                                           `group_id` VARCHAR(50) NULL COMMENT '组织id',
                                           `create_by` VARCHAR(300) NULL COMMENT '创建人',
                                           `update_time` DATETIME NULL COMMENT '数据入库时间'
) ENGINE=OLAP
UNIQUE KEY(`device_id`, `acquire_timestamp_format`, `record_id`)
COMMENT '运行记录存储全量数据'
PARTITION BY RANGE(`acquire_timestamp_format`)()
DISTRIBUTED BY HASH(`device_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.time_zone" = "Etc/UTC",
"dynamic_partition.start" = "-1000",
"dynamic_partition.end" = "1",
"dynamic_partition.prefix" = "p",
"dynamic_partition.replication_allocation" = "tag.location.default: 1",
"dynamic_partition.buckets" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "5",
"dynamic_partition.hot_partition_num" = "0",
"dynamic_partition.reserved_history_periods" = "NULL",
"dynamic_partition.storage_policy" = "",
"dynamic_partition.storage_medium" = "HDD",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V1",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);