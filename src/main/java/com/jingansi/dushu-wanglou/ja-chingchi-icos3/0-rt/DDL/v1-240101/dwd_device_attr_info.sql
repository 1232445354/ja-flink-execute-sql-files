----------------------
-- database：doris
-- remark：设备属性存储表
----------------------

create table `dwd_device_attr_info` (
                                        `device_id` varchar(200) NULL COMMENT '设备id:望楼id,雷达ID,可见光红外id,震动器id',
                                        `parent_id` varchar(300) NULL COMMENT '父设备id',
                                        `acquire_timestamp_format` datetime NULL COMMENT '时间戳格式化',
                                        `acquire_timestamp` bigint(20) NULL COMMENT '采集时间戳毫秒级别',
                                        `device_type` varchar(300) NULL COMMENT '设备类型(根据product_key赋值的)值就是product_key',
                                        `source_type` varchar(300) NULL COMMENT '类型,从mysql中关联取出的',
                                        `properties` varchar(40000) NULL COMMENT '设备属性json',
                                        `tid` varchar(200) NULL COMMENT '当前请求的事务唯一ID',
                                        `bid` varchar(200) NULL COMMENT '长连接整个业务的ID',
                                        `method` varchar(200) NULL COMMENT '服务&事件标识',
                                        `product_key` varchar(200) NULL COMMENT '产品编码',
                                        `version` varchar(100) NULL COMMENT '版本',
                                        `type` varchar(200) NULL COMMENT '类型',
                                        `update_time` datetime NULL COMMENT '数据入库时间'
) ENGINE=OLAP
UNIQUE KEY(`device_id`, `parent_id`, `acquire_timestamp_format`)
COMMENT '设备的属性存储'
PARTITION BY RANGE(`acquire_timestamp_format`)()
DISTRIBUTED BY HASH(`device_id`) BUCKETS 3
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.time_zone" = "Etc/UTC",
-- "dynamic_partition.start" = "-1000",
"dynamic_partition.end" = "4",
"dynamic_partition.prefix" = "p",
"dynamic_partition.replication_allocation" = "tag.location.default: 3",
"dynamic_partition.buckets" = "3",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "30",
"dynamic_partition.hot_partition_num" = "0",
"dynamic_partition.reserved_history_periods" = "NULL",
"dynamic_partition.storage_policy" = "",
"dynamic_partition.storage_medium" = "HDD",
"in_memory" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false"
);