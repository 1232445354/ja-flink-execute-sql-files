
-- doris中创建临时表temp_radarbox_aircraft_01
CREATE TABLE `temp_radarbox_aircraft_01` (
                  `flight_id` varchar(20) NULL COMMENT '飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码',
                  `acquire_time` datetime NULL COMMENT '采集时间',
                  `src_code` int(11) NULL COMMENT '来源网站标识 1. radarbox 2. adsbexchange',
                  `icao_code` varchar(10) NULL COMMENT '24位 icao编码',
                  `registration` varchar(20) NULL COMMENT '注册号',
                  `flight_no` varchar(20) NULL COMMENT '航班号',
                  `callsign` varchar(20) NULL COMMENT '呼号',
                  `flight_type` varchar(200) NULL COMMENT '飞机型号',
                  `is_military` int(11) NULL COMMENT '是否军用飞机 0 非军用 1 军用',
                  `pk_type` varchar(10) NULL COMMENT 'flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex',
                  `src_pk` varchar(20) NULL COMMENT '源网站主键',
                  `flight_category` varchar(10) NULL COMMENT '飞机类型',
                  `flight_category_name` varchar(50) NULL COMMENT '飞机类型名称',
                  `lng` double NULL COMMENT '经度',
                  `lat` double NULL COMMENT '纬度',
                  `speed` double NULL COMMENT '飞行当时的速度（单位：节）',
                  `speed_km` double NULL COMMENT '速度单位 km/h',
                  `altitude_baro` double NULL COMMENT '气压高度 海拔 航班当前高度，单位为（ft）',
                  `altitude_baro_m` double NULL COMMENT '气压高度 海拔 单位米',
                  `altitude_geom` double NULL COMMENT '海拔高度 海拔 航班当前高度，单位为（ft）',
                  `altitude_geom_m` double NULL COMMENT '海拔高度 海拔 单位米',
                  `heading` double NULL COMMENT '方向  正北为0 ',
                  `squawk_code` varchar(10) NULL COMMENT '当前应答机代码',
                  `flight_status` varchar(20) NULL COMMENT '飞机状态： 已启程',
                  `special` int(11) NULL COMMENT '是否有特殊情况',
                  `origin_airport3_code` varchar(10) NULL COMMENT '起飞机场的iata代码',
                  `origin_airport_e_name` varchar(500) NULL COMMENT '来源机场英文',
                  `origin_airport_c_name` varchar(500) NULL COMMENT '来源机场中文',
                  `origin_lng` double NULL COMMENT '来源机场经度',
                  `origin_lat` double NULL COMMENT '来源机场纬度',
                  `dest_airport3_code` varchar(10) NULL COMMENT '目标机场的 iata 代码',
                  `dest_airport_e_name` varchar(500) NULL COMMENT '目的机场英文',
                  `dest_airport_c_name` varchar(500) NULL COMMENT '目的机场中文',
                  `dest_lng` double NULL COMMENT '目的地坐标经度',
                  `dest_lat` double NULL COMMENT '目的地坐标纬度',
                  `flight_photo` varchar(200) NULL COMMENT '飞机的图片',
                  `flight_departure_time` datetime NULL COMMENT '航班起飞时间',
                  `expected_landing_time` datetime NULL COMMENT '预计降落时间',
                  `to_destination_distance` double NULL COMMENT '目的地距离',
                  `estimated_landing_duration` varchar(300) NULL COMMENT '预计还要多久着陆',
                  `airlines_icao` varchar(10) NULL COMMENT '航空公司的icao代码',
                  `airlines_e_name` varchar(500) NULL COMMENT '航空公司英文',
                  `airlines_c_name` varchar(100) NULL COMMENT '航空公司中文',
                  `country_code` varchar(10) NULL COMMENT '飞机所属国家代码',
                  `country_name` varchar(50) NULL COMMENT '国家中文',
                  `data_source` varchar(20) NULL COMMENT '数据来源的系统',
                  `source` varchar(20) NULL COMMENT '来源',
                  `position_country_code2` varchar(2) NULL COMMENT '位置所在国家简称',
                  `position_country_name` varchar(50) NULL COMMENT '位置所在国家名称',
                  `friend_foe` varchar(20) NULL COMMENT '敌我',
                  `sea_id` varchar(3) NULL COMMENT '海域id',
                  `sea_name` varchar(100) NULL COMMENT '海域名字',
                  `h3_code` varchar(20) NULL COMMENT '位置h3编码',
                  `extend_info` text NULL COMMENT '扩展信息 json 串',
                  `update_time` datetime NULL COMMENT '更新时间'
) ENGINE=OLAP
UNIQUE KEY(`flight_id`, `acquire_time`, `src_code`)
COMMENT 'radarbox飞机数据s模式为null，并且存在可以换的追踪id数据表'
DISTRIBUTED BY HASH(`flight_id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "default.location.default: 3",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);



-- doris中创建临时表temp_radarbox_aircraft_01
drop table if exists temp_radarbox_aircraft_02;
CREATE TABLE `temp_radarbox_aircraft_02` (
        icao_code 	    varchar(20) ,
        src_pk 			int(11) ,
        registration 	varchar(300),
        is_military 	varchar(300),
        flight_photo 	varchar(300),
        country_code 	varchar(300),
        country_name 	varchar(300)
) ENGINE=OLAP
UNIQUE KEY(`icao_code`, `src_pk`)
COMMENT 'radarbox飞机数据s模式和追踪id对应关系表'
DISTRIBUTED BY HASH(`icao_code`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);



-- mysql中创建临时表temp_radarbox_aircraft_mysql_01
CREATE TABLE `temp_radarbox_aircraft_mysql_01` (
     icao_code varchar(255) COMMENT 's模式代码',
     src_pk varchar(255)  COMMENT '追踪id',
     UNIQUE KEY `idx_union` (`icao_code`,`src_pk`) USING BTREE COMMENT '联合唯一索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='radarbox飞机数据s模式和追踪id对应关系表'


-- mysql中创建临时表temp_radarbox_aircraft_mysql_02
drop table temp_radarbox_aircraft_mysql_02;
CREATE TABLE `temp_radarbox_aircraft_mysql_02` (
       `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
       `situation_id` bigint DEFAULT NULL COMMENT '态势id',
       `target_id` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '目标id',
       `target_type` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '目标类型,AIRCRAFT:飞机,SHIP:船舶,SATELLITE:卫星,AIRPORT:机场',
       `type` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '敌我类型,ENEMY:敌方;OUR_SIDE:我方;FRIENDLY_SIDE:友方;NEUTRALITY:中立;SUSPECTED_ENEMY:疑似敌方;SUSPECTED_OUR_SIDE:疑似我方;SUSPECTED_FRIENDLY_SIDE:疑似友方;SUSPECTED_NEUTRALITY:疑似中立',
       `username` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '用户名',
       `nickname` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '目标昵称',
       `remark` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '备注',
       `gmt_create` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
       `gmt_modified` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
       `deleted` tinyint(1) DEFAULT '0' COMMENT '是否删除,0:未删除,1:已删除',
       `gmt_create_by` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '创建人',
       `gmt_modified_by` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '更新人',
       PRIMARY KEY (`id`),
       UNIQUE KEY `idx_union` (`target_id`,`target_type`,`username`) USING BTREE COMMENT '联合唯一索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='一个用户多个飞机昵称需要删除的id表'


-- mysql中创建临时表temp_radarbox_aircraft_mysql_03
drop table temp_radarbox_aircraft_mysql_03;
CREATE TABLE `temp_radarbox_aircraft_mysql_03` (
        `id` bigint NOT NULL,
        `target_id` varchar(128) COLLATE utf8mb4_bin NOT NULL COMMENT '关注目标id',
        `target_name` varchar(128) COLLATE utf8mb4_bin NOT NULL COMMENT '关注目标名',
        `target_type` varchar(128) COLLATE utf8mb4_bin NOT NULL COMMENT '关注目标类型,AIRCRAFT:飞机,SHIP:船舶,SATELLITE:卫星,AIRPORT:机场',
        `username` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '关注用户id',
        `gmt_create` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `gmt_modified` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        `gmt_create_by` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '创建者id',
        `gmt_create_by_name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '创建者名称',
        `gmt_modified_by` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '更新者id',
        `gmt_modified_by_name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '更新者名称',
        `follow_group_id` bigint DEFAULT NULL COMMENT '关注分组id',
        PRIMARY KEY (`id`),
        UNIQUE KEY `idx_union` (`target_id`,`target_type`,`username`) USING BTREE COMMENT '联合索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='一个用户多个飞机关注需要删除的id表'






