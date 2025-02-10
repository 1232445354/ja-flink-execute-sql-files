----------------------
-- database：doris
-- remark：可见光红外全量表-原始表
----------------------

drop table if exists dwd_photoelectric_target_all_rt_source;
CREATE TABLE `dwd_photoelectric_target_all_rt_source` (
                                                          `device_id` varchar(200) NULL COMMENT '可见光、红外的设备ID-source_id',
                                                          `target_id` varchar(200) NULL COMMENT '可见光、红外检测到的目标的id',
                                                          parent_id   varchar(300) comment '父设备的id',
                                                          `acquire_timestamp_format` datetime NULL COMMENT '上游程序上报时间戳-时间戳格式化',
                                                          `acquire_timestamp` bigint(20) NULL COMMENT '采集时间戳毫秒级别，上游程序上报时间戳',
                                                          source_type  varchar(300) comment 'VISUAL:可见光,INFRARED:红外,FUSION:	融合',
                                                          device_name varchar(300) comment '设备名称',
                                                          radar_id 	varchar(300) comment '雷达id',
                                                          radar_target_id  double comment '雷达检测的目标id',
                                                          radar_device_name varchar(400) comment '雷达设备的名称',
                                                          source     varchar(10000) comment '数据检测的来源拼接 示例：雷达（11）、可见光（22）',
                                                          record_path	varchar(10000) comment '可见光、红外告警视频地址',
                                                          bbox_height double comment '长度',
                                                          bbox_left	double comment '左',
                                                          bbox_top	double comment '上',
                                                          bbox_width	double comment '宽度',
                                                          source_frame_height bigint comment '原视频高度',
                                                          source_frame_width bigint comment '原视频宽度',
                                                          longitude double comment '目标经度',
                                                          latitude double 	comment '目标纬度',
                                                          altitude double comment '高度',
                                                          big_image_path varchar(1000) comment '大图',
                                                          small_image_path varchar(1000) comment '小图',
                                                          image_source varchar(200) comment '图片来源设备',
                                                          class_id double comment '',
                                                          confidence varchar(200) comment '置信度',
                                                          infer_id double comment '',
                                                          object_label varchar(200) comment '目标的类型，人，车...',
                                                          object_sub_label varchar(200) comment '目标的类型子类型',
                                                          `speed` double NULL COMMENT '目标速度 m/s',
                                                          `distance` double NULL COMMENT '距离 m',
                                                          `yaw` double NULL COMMENT '目标方位',
                                                          `update_time` datetime NULL COMMENT '数据入库时间'
) ENGINE=OLAP
UNIQUE KEY(`device_id`, `target_id`, parent_id,`acquire_timestamp_format`)
COMMENT '可见光红外检测数据全部入库'
PARTITION BY RANGE(`acquire_timestamp_format`)()
DISTRIBUTED BY HASH(`device_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.time_zone" = "Etc/UTC",
-- "dynamic_partition.start" = "-1000",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.replication_allocation" = "tag.location.default: 1",
"dynamic_partition.buckets" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "10",
"dynamic_partition.hot_partition_num" = "0",
"dynamic_partition.reserved_history_periods" = "NULL",
"dynamic_partition.storage_policy" = "",
"dynamic_partition.storage_medium" = "HDD",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false"
)