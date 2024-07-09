-- truncate table dim_track_table_per_day_cnt;
-- drop table if exists dim_track_table_per_day_cnt;
CREATE TABLE `dim_table_per_day_cnt` (
                                         table_name 	varchar(200) 	comment '表名',
                                         time1     	datetime 			comment '时间',
                                         cnt      		bigint 				comment '数据量'
) ENGINE=OLAP
UNIQUE KEY(`table_name`, `time1`)
COMMENT '轨迹表每天数据量'
DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 2",
"min_load_replica_num" = "-1",
"storage_format" = "V2"
);