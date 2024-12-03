----------------------
-- database：doris
-- remark：可见光红外全量表-目标可删除可更改(主要是望楼版本)
----------------------

CREATE TABLE `dws_photoelectric_target_status_rt` (
                                                      `device_id` VARCHAR(200) NULL COMMENT '可见光、红外的设备ID-source_id',
                                                      `target_id` VARCHAR(200) NULL COMMENT '可见光、红外检测到的目标的id',
                                                      `parent_id` VARCHAR(300) NULL COMMENT '父设备的id',
                                                      `acquire_timestamp_format` DATETIME NULL COMMENT '上游程序上报时间戳-时间戳格式化',
                                                      `acquire_timestamp` BIGINT NULL COMMENT '采集时间戳毫秒级别，上游程序上报时间戳',
                                                      `source_type` VARCHAR(300) NULL COMMENT 'VISUAL:可见光,INFRARED:红外,FUSION:	融合目标',
                                                      `device_name` VARCHAR(300) NULL COMMENT '设备名称',
                                                      `radar_id` VARCHAR(300) NULL COMMENT '雷达id',
                                                      `radar_target_id` DOUBLE NULL COMMENT '雷达检测的目标id',
                                                      `radar_device_name` VARCHAR(400) NULL COMMENT '雷达设备的名称',
                                                      `source` VARCHAR(10000) NULL COMMENT '数据检测的来源拼接 示例：雷达（11）、可见光（22）',
                                                      `record_path` VARCHAR(10000) NULL COMMENT '可见光、红外告警视频地址',
                                                      `bbox_height` DOUBLE NULL COMMENT '长度',
                                                      `bbox_left` DOUBLE NULL COMMENT '左',
                                                      `bbox_top` DOUBLE NULL COMMENT '上',
                                                      `bbox_width` DOUBLE NULL COMMENT '宽度',
                                                      `source_frame_height` BIGINT NULL COMMENT '原视频高度',
                                                      `source_frame_width` BIGINT NULL COMMENT '原视频宽度',
                                                      `longitude` DOUBLE NULL COMMENT '目标经度',
                                                      `latitude` DOUBLE NULL COMMENT '目标纬度',
                                                      `altitude` DOUBLE NULL COMMENT '高度',
                                                      `big_image_path` VARCHAR(1000) NULL COMMENT '大图',
                                                      `small_image_path` VARCHAR(1000) NULL COMMENT '小图',
                                                      `image_source` VARCHAR(200) NULL COMMENT '图片来源设备',
                                                      `class_id` DOUBLE NULL,
                                                      `confidence` VARCHAR(200) NULL COMMENT '置信度',
                                                      `infer_id` DOUBLE NULL,
                                                      `object_label` VARCHAR(200) NULL COMMENT '目标的类型，人，车...',
                                                      `object_sub_label` VARCHAR(200) NULL COMMENT '目标的类型子类型',
                                                      `speed` DOUBLE NULL COMMENT '目标速度 m/s',
                                                      `distance` DOUBLE NULL COMMENT '距离 m',
                                                      `yaw` DOUBLE NULL COMMENT '目标方位',
                                                      `flag` VARCHAR(300) NULL COMMENT '是否修改属性-修改、插入',
                                                      `update_time` DATETIME NULL COMMENT '数据入库时间'
) ENGINE=OLAP
UNIQUE KEY(`device_id`, `target_id`, `parent_id`)
COMMENT '设备检测数据全部入库'
DISTRIBUTED BY HASH(`device_id`) BUCKETS 3
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V1",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728",
"enable_mow_light_delete" = "false"
);