CREATE TABLE `dim_country_code_name_info` (
                                              `id` VARCHAR(300) NULL COMMENT '国家ID，可以使用country英文名称',
                                              `source` VARCHAR(200) NULL COMMENT '数据所属的数据来源',
                                              `acquire_timestamp_format` DATETIME NULL COMMENT '数据采集时间戳格式化',
                                              `acquire_timestamp` BIGINT NULL COMMENT '数据采集时间戳',
                                              `e_name` VARCHAR(300) NULL COMMENT '国家名称英文',
                                              `c_name` VARCHAR(100) NULL COMMENT '国家名称中文',
                                              `country_code2` VARCHAR(100) NULL COMMENT '国家代码2字',
                                              `country_code3` VARCHAR(100) NULL COMMENT '国家代码3字',
                                              `acquire_flag_url` VARCHAR(200) NULL COMMENT '国旗图片获取地址',
                                              `flag_url` VARCHAR(200) NULL COMMENT '国旗图片存放地址',
                                              `remark` VARCHAR(200) NULL COMMENT '备注',
                                              `create_by` VARCHAR(200) NULL COMMENT '创建人',
                                              `update_time` DATETIME NULL COMMENT '数据入库时间'
) ENGINE=OLAP
UNIQUE KEY(`id`, `source`)
COMMENT '国家信息表'
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_format" = "V2",
"inverted_index_storage_format" = "DEFAULT",
"enable_unique_key_merge_on_write" = "false",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);