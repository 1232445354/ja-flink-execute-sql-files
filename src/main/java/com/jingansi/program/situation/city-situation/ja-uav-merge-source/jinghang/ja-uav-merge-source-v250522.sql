--********************************************************************--
-- author:      write your name here
-- create time: 2025/5/11 17:38:47
-- description: rid、aoa、雷达融合数据入库
-- version: ja-uav-merge-source-v250522 新增无人机设备id
--********************************************************************--
set 'pipeline.name' = 'ja-uav-merge-source';


SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '60000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'parallelism.default' = '4';
-- set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';

SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-uav-merge-source';


 -----------------------

 -- 数据结构来源

 -----------------------

-- 设备检测数据上报 （Source：kafka）
create table uav_merge_target_kafka (
                                        id                            string  comment 'id',
                                        device_id                     string  comment '数据来源的设备id',
                                        acquire_time          		string  comment '采集时间',
                                        src_code              		string  comment '自己本身数据类型 RID、AOA、RADAR',
                                        src_pk                        string  comment '自己网站的目标id',
                                        device_name                   string  comment '设备名称',
                                        rid_devid             		string  comment 'rid设备的id-飞机上报的',
                                        msgtype               		bigint  comment '消息类型',
                                        recvtype              		string  comment '无人机数据类型,示例:2.4G',
                                        recvmac                       string  comment '无人机的mac地址',
                                        mac                   		string  comment 'rid设备MAC地址',
                                        rssi                  		bigint  comment '信号强度',
                                        longitude             		double  comment '探测到的无人机经度',
                                        latitude              		double  comment '探测到的无人机纬度',
                                        location_alit         		double  comment '气压高度',
                                        ew                    		double  comment 'rid航迹角,aoa监测站识别的目标方向角',
                                        speed_h               		double  comment '水平速度',
                                        speed_v               		double  comment '垂直速度',
                                        height                		double  comment '距地高度',
                                        height_type           		double  comment '高度类型',
                                        control_station_longitude  	double  comment '控制无人机人员经度',
                                        control_station_latitude   	double  comment '控制无人机人员纬度',
                                        control_station_height 	   	double 	comment '控制站高度',
                                        target_name             		string  comment 'aoa-目标名称',
                                        altitude                		double  comment 'aoa-无人机所在海拔高度',
                                        distance_from_station   		double  comment 'aoa-无人机距离监测站的距离',
                                        speed_ms                  	double  comment 'aoa-无人机飞行速度 (m/s)',
                                        target_frequency_khz    		double  comment 'aoa-目标使用频率 (k_hz)',
                                        target_bandwidth_khz		    double  comment 'aoa-目标带宽 (k_hz)',
                                        target_signal_strength_db		double  comment 'aoa-目标信号强度 (d_b)',
                                        target_type bigint,
                                        `target_list_status` int NULL COMMENT '名单状态1是白名单 2是黑名单 4是民众注册无人机',
                                        `target_list_type` int NULL COMMENT '名单类型3是政务无人机 2是低空经济无人机 5是重点关注',
                                        `user_company_name` varchar(50) NULL COMMENT '持有单位',
                                        `user_full_name` varchar(50) NULL COMMENT '持有者姓名',
                                        `target_direction` double NULL COMMENT '无人机方位（对于发现设备）',
                                        `target_area_code` varchar(20) NULL COMMENT '无人机飞行地区行政编码',
                                        `target_registered` int NULL COMMENT '无人机是否报备1是已报备',
                                        `target_fly_report_status` int NULL COMMENT '无人机报备状态1是未报备 2 是已报备 3 超出报备区域范围 4 是不符合报备飞行时间5 是超出报备区域范围 6是 飞行海拔高度超过120米',
                                        `home_longitude` double NULL COMMENT '无人机返航点经度',
                                        `home_latitude` double NULL COMMENT '无人机返航点纬度',
                                        `no_fly_zone_id` int NULL COMMENT '无人机是否飞入禁飞区0是未飞入',
                                        `user_phone` varchar(20) NULL COMMENT '持有者手机号'
) WITH (
      'connector' = 'kafka',
      'topic' = 'uav_merge_target',
      -- 'properties.bootstrap.servers' = '135.100.11.110:30090',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.group.id' = 'uav_merge_target3',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1761237002000',  -- 1745564415000
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );




-- ********************************* doris写入的表 ********************************************

-- 融合全量表
create table dwd_bhv_merge_target_rt (
                                         uav_id                         string  comment '无人机的id-sn号',
                                         control_station_id             string  comment '控制站的id',
                                         device_id                      string  comment 'RID、AOA设备id',
                                         acquire_time                   string  comment '采集时间',
                                         src_code                       string  comment '自己本身数据类型 RID、AOA RADAR',
                                         src_pk                	     string  comment '数据自身的id',
                                         merge_type                     string  comment '融合的设备类型,逗号分隔,示例：RID,AOA,RADAR',
                                         merge_cnt                      bigint  comment '融合的设备类型数量,示例：1',
                                         merge_target_cnt               bigint  comment '融合的目标id数量,示例，100个RID目标融合为100',
                                         uav_device_id                  string  comment '牍术无人机id',
                                         device_name                    string  comment '设备名称',
                                         rid_devid                      string  comment 'rid设备的id-飞机上报的',
                                         msgtype                        bigint  comment '消息类型',
                                         recvtype                       string  comment '无人机数据类型,示例:2.4G',
                                         recvmac                        string  comment '无人机的mac地址',
                                         mac                            string  comment 'rid设备MAC地址',
                                         rssi                           bigint  comment '信号强度',
                                         longitude                      double  comment '融合-探测到的无人机经度',
                                         latitude                       double  comment '融合-探测到的无人机纬度',
                                         old_longitude                  double  comment '原始的经度',
                                         old_latitude                   double  comment '原始的纬度',
                                         location_alit                  double  comment '气压高度',
                                         ew                             double  comment '航迹角',
                                         speed_h                        double  comment '无人机地速-水平速度',
                                         speed_v                        double  comment '垂直速度',
                                         height                         double  comment '无人机距地高度',
                                         height_type                    double  comment '高度类型',
                                         control_station_longitude      double  comment '控制无人机人员经度',
                                         control_station_latitude       double  comment '控制无人机人员纬度',
                                         control_station_height 	     double  comment '控制站高度',
                                         target_name             	     string  comment 'aoa-目标名称',
                                         altitude                	     double  comment 'aoa-无人机所在海拔高度',
                                         distance_from_station   	     double  comment 'aoa-无人机距离监测站的距离',
                                         speed_ms                       double  comment 'aoa-无人机飞行速度 (m/s)',
                                         target_frequency_khz    	     double  comment 'aoa-目标使用频率 (k_hz)',
                                         target_bandwidth_khz		     double  comment 'aoa-目标带宽 (k_hz)',
                                         target_signal_strength_db	     double  comment 'aoa-目标信号强度 (d_b)',
                                         `target_list_status` int NULL COMMENT '名单状态1是白名单 2是黑名单 4是民众注册无人机',
                                         `target_list_type` int NULL COMMENT '名单类型3是政务无人机 2是低空经济无人机 5是重点关注',
                                         `user_company_name` varchar(50) NULL COMMENT '持有单位',
                                         `user_full_name` varchar(50) NULL COMMENT '持有者姓名',
                                         `target_direction` double NULL COMMENT '无人机方位（对于发现设备）',
                                         `target_area_code` varchar(20) NULL COMMENT '无人机飞行地区行政编码',
                                         `target_registered` int NULL COMMENT '无人机是否报备1是已报备',
                                         `target_fly_report_status` int NULL COMMENT '无人机报备状态1是未报备 2 是已报备 3 超出报备区域范围 4 是不符合报备飞行时间5 是超出报备区域范围 6是 飞行海拔高度超过120米',
                                         `home_longitude` double NULL COMMENT '无人机返航点经度',
                                         `home_latitude` double NULL COMMENT '无人机返航点纬度',
                                         `no_fly_zone_id` int NULL COMMENT '无人机是否飞入禁飞区0是未飞入',
                                         `user_phone` varchar(20) NULL COMMENT '持有者手机号',
                                         filter_col                     string  comment '动态筛选字段拼接',
                                         update_time                    string  comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      'table.identifier' = 'sa.dwd_bhv_merge_target_rt',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );





create table dwd_bhv_uav_illegal_list_rt (
                                             `sn` string NULL COMMENT '无人机id',
                                             `type` int NULL COMMENT '违法类型 1：超高 144 米',
                                             `acquire_time` string NULL COMMENT '采集时间',
                                             `longitude` double NULL COMMENT '经度',
                                             `latitude` double NULL COMMENT '纬度',
                                             `altitude` double NULL COMMENT '高度',
                                             `speed_ms` double NULL COMMENT '速度',
                                             `update_time` string NULL COMMENT '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      'table.identifier' = 'sa.dwd_bhv_uav_illegal_list_rt',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- -- 融合状态表
-- create table dws_bhv_merge_target_last_location_rt (
--   uav_id                         string  comment '无人机的id-sn号',
--   control_station_id             string  comment '控制站的id',
--   device_id                      string  comment 'RID、AOA设备id',
--   acquire_time                   string  comment '采集时间',
--   src_code                       string  comment '自己本身数据类型 RID、AOA RADAR',
--   src_pk                	     string  comment '数据自身的id',
--   merge_type                     string  comment '融合的设备类型,逗号分隔,示例：RID,AOA,RADAR',
--   merge_cnt                      bigint  comment '融合的设备类型数量,示例：1',
--   merge_target_cnt               bigint  comment '融合的目标id数量,示例，100个RID目标融合为100',
--   uav_device_id                  string  comment '牍术无人机id',
--   device_name                    string  comment '设备名称',
--   rid_devid                      string  comment 'rid设备的id-飞机上报的',
--   msgtype                        bigint  comment '消息类型',
--   recvtype                       string  comment '无人机数据类型,示例:2.4G',
--   recvmac                        string  comment '无人机的mac地址',
--   mac                            string  comment 'rid设备MAC地址',
--   rssi                           bigint  comment '信号强度',
--   longitude                      double  comment '融合-探测到的无人机经度',
--   latitude                       double  comment '融合-探测到的无人机纬度',
--   old_longitude                  double  comment '原始的经度',
--   old_latitude                   double  comment '原始的纬度',
--   location_alit                  double  comment '气压高度',
--   ew                             double  comment '航迹角',
--   speed_h                        double  comment '无人机地速-水平速度',
--   speed_v                        double  comment '垂直速度',
--   height                         double  comment '无人机距地高度',
--   height_type                    double  comment '高度类型',
--   control_station_longitude      double  comment '控制无人机人员经度',
--   control_station_latitude       double  comment '控制无人机人员纬度',
--   control_station_height 	     double  comment '控制站高度',
--   target_name             	     string  comment 'aoa-目标名称',
--   altitude                	     double  comment 'aoa-无人机所在海拔高度',
--   distance_from_station   	     double  comment 'aoa-无人机距离监测站的距离',
--   speed_ms                       double  comment 'aoa-无人机飞行速度 (m/s)',
--   target_frequency_khz    	     double  comment 'aoa-目标使用频率 (k_hz)',
--   target_bandwidth_khz		     double  comment 'aoa-目标带宽 (k_hz)',
--   target_signal_strength_db	     double  comment 'aoa-目标信号强度 (d_b)',
--   filter_col                     string  comment '动态筛选字段拼接',
--   update_time                    string  comment '更新时间'
--   ) with (
--       'connector' = 'doris',
--       'fenodes' = '135.100.11.132:30030',
--       -- 'fenodes' = '172.21.30.245:8030',
--       'table.identifier' = 'sa.dws_bhv_merge_target_last_location_rt',
--       'username' = 'root',
--       'password' = 'Jingansi@110',
--       'doris.request.tablet.size'='5',
--       'doris.request.read.timeout.ms'='30000',
--       'sink.batch.size'='3000',
--       'sink.batch.interval'='2s',
--       'sink.properties.escape_delimiters' = 'true',
--       'sink.properties.column_separator' = '\x01',	 -- 列分隔符
--       'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
--   );



-- 控制站实体表
create table `dws_et_control_station_info` (
                                               id              string  comment '控制站id',
                                               acquire_time    string  comment '采集时间',
                                               name            string  comment '控制站名称',
                                               register_uav    string  comment '登记无人机-序列号（产品型号）',
                                               source          string  comment '数据来源',
                                               search_content  string  comment '倒排索引数据',
                                               update_time     string  comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      -- 'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_et_control_station_info',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 无人机实体表
create table `dws_et_uav_info` (
                                   id                      string  comment '无人机id-sn号',
                                   sn                      string  comment '序列号',
                                   name                    string  comment '无人机名称',
                                   device_id               string  comment '无人机的设备id-牍术介入的',
                                   recvmac                 string  comment 'MAC地址',
                                   manufacturer            string  comment '厂商',
                                   model                   string  comment '型号',
                                   owner                   string  comment '所有者',
                                   type                    string  comment '类型',
                                   source                  string  comment '数据来源',
                                   search_content          string  comment '倒排索引数据',
                                   update_time             string  comment '数据入库时间',
                                   category                string  COMMENT '类别',
                                   card_code               string  COMMENT '所有人身份证号/统一信用代码',
                                   card_type_name          string  COMMENT '证件类型名称',
                                   phone                   string  COMMENT '电话',
                                   email                   string  COMMENT '邮箱',
                                   company_name            string  COMMENT '持有单位',
                                   empty_weight            string  COMMENT '空机重量',
                                   maximum_takeoff_weight  string  COMMENT '最大起飞重量',
                                   identity_type           int     COMMENT '无人机身份类型（0 未知，1 无人机，2 低慢小）',
                                   identity_type_name      string  COMMENT '无人机身份类型名称',
                                   engine_type             string  COMMENT '动力类型',
                                   area_code               string  COMMENT '所属地区',
                                   address                 string  COMMENT '详细地址',
                                   username                string  COMMENT '持有者姓名',
                                   user_type_name          string  COMMENT '用户类型名称',
                                   list_status             int     COMMENT '名单状态（0 正常，1 白名单，2 灰名单）',
                                   list_type               int     COMMENT '名单类型(1 警用白名单，2 低空经济白名单，3 政务白名单， 4 多次黑飞黑名单， 5 重点人员黑名单)',
                                   gmt_expire              string  COMMENT '过期时间 ',
                                   real_name               boolean COMMENT '是否实名（0 未实名，1已实名） ',
                                   gmt_register            string  COMMENT '实名注册时间',
                                   residence               string  COMMENT '居住地',
                                   purpose                 string  COMMENT '用途'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      -- 'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_et_uav_info',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- rid、控制站、无人机关系表
create table `dws_rl_rid_uav_rt` (
                                     uav_id               string  comment '无人机的id-sn号',
                                     control_station_id   string  comment '控制站id',
                                     device_id   	       string  comment 'RID的设备id-牍术接入的',
                                     rid_devid       	   string  comment 'rid设备的id-飞机上报的',
                                     acquire_time         string  comment '采集时间',
                                     update_time          string  comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      -- 'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_rl_rid_uav_rt',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- -- 无人机实体信息表
-- create table `dwd_et_uav_jh_info` (
--   id  bigint	comment '无人机id',
--   type  int	comment '无人机类型（0 未知，1 无人机，2 低慢小）',
--   type_name  varchar(20)	comment '无人机类型名称',
--   full_name  varchar(20)	comment '无人机持有者姓名',
--   sn  varchar(20)	comment '无人机序列号',
--   uav_model_name  varchar(20)	comment '无人机型号名称',
--   uav_company_name  varchar(20)	comment '无人机厂商名称',
--   gmt_register  string	comment '注册时间',
--   area_code  varchar(20)	comment '所属地区',
--   address  varchar(200)	comment '详细地址',
--   username  varchar(20)	comment '持有者姓名',
--   card_code  varchar(20)	comment '持有者身份证号',
--   phone  varchar(20)	comment '持有者手机号',
--   deleted  int	comment '是否删除（0 正常，1 已删除）',
--   gmt_create  string	comment '创建时间',
--   update_time  string	comment '更新时间'
-- ) with (
--       'connector' = 'doris',
--       'fenodes' = '135.100.11.132:30030',
--       -- 'fenodes' = '172.21.30.245:8030',
--       'table.identifier' = 'sa.dwd_et_uav_jh_info',
--       'username' = 'root',
--       'password' = 'Jingansi@110',
--       'doris.request.tablet.size'='5',
--       'doris.request.read.timeout.ms'='30000',
--       'sink.batch.size'='3000',
--       'sink.batch.interval'='2s',
--       'sink.properties.escape_delimiters' = 'true',
--       'sink.properties.column_separator' = '\x01',	 -- 列分隔符
--       'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
-- );


-- -- 无人机白名单表
-- create table `dwd_et_uav_jh_white_info` (
--   id  bigint  comment '无人机id',
--   sn  varchar(20)  comment '无人机序列号',
--   uav_model_name  varchar(20)  comment '无人机型号名称',
--   max_height  int  comment '最大高度（单位米）',
--   engine_type  varchar(20)  comment '动力类型',
--   company_name  varchar(50)  comment '持有单位',
--   list_status  int  comment '名单状态（0 正常，1 白名单，2 灰名单）',
--   list_type  int  comment '名单类型(1 警用白名单，2 低空经济白名单，3 政务白名单， 4 多次黑飞黑名单， 5 重点人员黑名单)',
--   gmt_expire  string  comment '过期时间',
--   deleted  int  comment '是否删除（0 正常，1 已删除）',
--   gmt_create  string  comment '创建时间',
--   update_time  string  comment '更新时间'
-- ) with (
--       'connector' = 'doris',
--       'fenodes' = '135.100.11.132:30030',
--       -- 'fenodes' = '172.21.30.245:8030',
--       'table.identifier' = 'sa.dwd_et_uav_jh_white_info',
--       'username' = 'root',
--       'password' = 'Jingansi@110',
--       'doris.request.tablet.size'='5',
--       'doris.request.read.timeout.ms'='30000',
--       'sink.batch.size'='3000',
--       'sink.batch.interval'='2s',
--       'sink.properties.escape_delimiters' = 'true',
--       'sink.properties.column_separator' = '\x01',	 -- 列分隔符
--       'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
-- );



-- -- 无人机用户数据表
-- create table `dwd_et_uav_jh_user_info` (
-- id  bigint comment  'id',
-- username  varchar(20) comment  '持有者姓名',
-- user_type_name  varchar(20) comment  '用户类型名称',
-- card_type_name  varchar(20) comment  '证件类型名称',
-- phone  varchar(20) comment  '持有者手机号',
-- card_code  varchar(20) comment  '证件号码',
-- area_code  varchar(20) comment  '所属地区',
-- real_name  Boolean comment  '是否实名（0 未实名，1已实名）',
-- gmt_register  string comment  '实名注册时间',
-- full_name  varchar(20) comment  '姓名',
-- residence  varchar(20) comment  '居住地',
-- list_status  int comment  '名单状态（0 正常，1 白名单，2 灰名单，3 黑名单）',
-- deleted  Boolean comment  '是否删除（0 正常，1 已删除）',
-- gmt_create  string comment  '创建时间',
-- update_time  string comment  '更新时间'
-- ) with (
--       'connector' = 'doris',
--       'fenodes' = '135.100.11.132:30030',
--       -- 'fenodes' = '172.21.30.245:8030',
--       'table.identifier' = 'sa.dwd_et_uav_jh_user_info',
--       'username' = 'root',
--       'password' = 'Jingansi@110',
--       'doris.request.tablet.size'='5',
--       'doris.request.read.timeout.ms'='30000',
--       'sink.batch.size'='3000',
--       'sink.batch.interval'='2s',
--       'sink.properties.escape_delimiters' = 'true',
--       'sink.properties.column_separator' = '\x01',	 -- 列分隔符
--       'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
-- );


create table `dws_et_uav_pilot_info` (
                                         id              varchar(100) NULL COMMENT '飞手id',
                                         a_method        varchar(100) NULL COMMENT '认证方式 个人认证；企业认证',
                                         name            varchar(200) NULL COMMENT '个人认证（名称）；企业认证（企业名称）',
                                         card_code       varchar(300) NULL COMMENT '个人认证（身份证号）；企业认证（统一社会信用代码）',
                                         phone           varchar(200) NULL COMMENT '联系方式"',
                                         email           varchar(300) NULL COMMENT '邮箱',
                                         address         varchar(300) NULL COMMENT '地址',
                                         register_uav    varchar(200) NULL COMMENT '登记无人机-序列号（产品型号）',
                                         username        varchar(20) NULL COMMENT '持有者姓名',
                                         user_type_name  varchar(20) NULL COMMENT '用户类型名称',
                                         card_type_name  varchar(20) NULL COMMENT '证件类型名称',
                                         area_code       varchar(20) NULL COMMENT '所属地区',
                                         real_name       boolean NULL COMMENT '是否实名（0 未实名，1已实名）',
                                         gmt_register    string NULL COMMENT '实名注册时间',
                                         list_status     int NULL COMMENT '名单状态（0 正常，1 白名单，2 灰名单，3 黑名单）',
                                         search_content  varchar(500) NULL COMMENT '搜索字段 将所有搜索值放在该字段，建立倒排索引',
                                         update_time     string NULL COMMENT '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      -- 'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_et_uav_pilot_info',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- -- 飞手和无人机关系
-- create table `dws_rl_uav_pilot` (
--   `uav_id` varchar(200) NULL COMMENT '无人机的id-sn号',
--   `pilot_id` varchar(200) NULL COMMENT '飞机id',
--   update_time     string NULL COMMENT '数据入库时间'
-- ) with (
--       'connector' = 'doris',
--       'fenodes' = '135.100.11.132:30030',
--       -- 'fenodes' = '172.21.30.245:8030',
--       'table.identifier' = 'sa.dws_rl_uav_pilot',
--       'username' = 'root',
--       'password' = 'Jingansi@110',
--       'doris.request.tablet.size'='5',
--       'doris.request.read.timeout.ms'='30000',
--       'sink.batch.size'='3000',
--       'sink.batch.interval'='2s',
--       'sink.properties.escape_delimiters' = 'true',
--       'sink.properties.column_separator' = '\x01',	 -- 列分隔符
--       'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
-- );


-- ********************************** doris数据表读取 ***********************************

-- 无人机实体表来源
create table `dws_et_uav_info_source` (
                                          id                      string  comment '无人机id-sn号',
                                          sn                      string  comment '序列号',
                                          device_id               string  comment '设备编号',
                                          name                    string  comment '无人机名称',
                                          recvmac                 string  comment 'MAC地址',
                                          manufacturer            string  comment '厂商',
                                          model                   string  comment '型号',
                                          owner                   string  comment '所有者',
                                          type                    string  comment '类型',
                                          source                  string  comment '数据来源',
                                          category                string  comment '类别',
                                          phone                   string  comment '电话',
                                          empty_weight            string  comment '空机重量',
                                          maximum_takeoff_weight  string  comment '最大起飞重量',
                                          purpose                 string  comment '用途',
                                          PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://135.100.11.132:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      -- 'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_uav_info',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '600s',
      'lookup.max-retries' = '10'
      );



-- 无人机设备表（Sink：mysql）
create table device (
                        id                  string,
                        device_id	          string, -- 设备id
                        name	              string, -- 设备名称
                        manufacturer	      string, -- 设备厂商
                        model	              string, -- 设备型号
                        type	              string, -- 设备类型
                        owner	              string, -- 所属人
                        sn	              string, -- 唯一序列号
                        status              string, -- 在线状态
                        longitude           decimal(12,8), -- 经度
                        latitude            decimal(12,8), -- 纬度
                        PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://135.100.11.110:31306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'device',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '600s',
      'lookup.max-retries' = '10'
      );


-- ****************************规则引擎写入数据******************************** --
create table uav_source(
                           id                    string, -- id
                           name                  string, -- 名称
                           type                  string, -- 类型
                           manufacturer          string, -- 厂商
                           model                 string, -- 型号
                           ew                    double, -- 航迹角
                           height                double, -- 无人机距地高度
                           locationAlit          double, -- 气压高度
                           speedV                double, -- 垂直速度
                           speedH                double, -- 无人机地速,水平速度
                           recvtype              string, -- 无人机数据类型
                           rssi                  double, -- 信号强度
                           lng                   double, -- 融合的经度 - 给大武规则
                           lat                   double, -- 维度的纬度 - 给大武规则
                           longitude             double, -- 原始经度 - 给抓捕
                           latitude              double, -- 原始纬度 - 给抓捕
                           acquireTime           string, -- 采集时间
                           targetType            string, -- 实体类型 固定值 UAV
                           updateTime            string -- flink处理时间
) with (
      'connector' = 'kafka',
      'topic' = 'uav_source',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'uav_source1',
      'key.format' = 'json',
      'key.fields' = 'id',
      'format' = 'json'
      );


-----------------------

-- 数据处理

-----------------------

-- 数据字段处理
create view temp01 as
select
    REGEXP_REPLACE(id,'\u0002','') as id                          , -- id
    device_id                     , -- 数据来源的设备id
    acquire_time          		, -- 采集时间
    src_code              		, -- 自己本身数据类型 RID、AOA、RADAR
    REGEXP_REPLACE(src_pk,'\u0002','') as    src_pk                    , -- 自己网站的目标id
    device_name                   , -- 设备名称
    rid_devid             		, -- rid设备的id-飞机上报的
    msgtype               		, -- 消息类型
    recvtype              		, -- 无人机数据类型,示例:2.4G
    recvmac                       , -- 无人机的mac地址
    mac                   		, -- rid设备MAC地址
    rssi                  		, -- 信号强度
    longitude   as old_longitude  , -- 探测到的无人机经度
    latitude    as old_latitude   , -- 探测到的无人机纬度',
    location_alit         		, -- 气压高度,
    ew                    		, -- rid航迹角,aoa监测站识别的目标方向角
    speed_h               		, -- 水平速度
    speed_v               		, -- 垂直速度
    height                		, -- 距地高度
    height_type           		, -- 高度类型
    control_station_longitude  	, -- 控制无人机人员经度
    control_station_latitude   	, -- 控制无人机人员纬度
    control_station_height 	   	, -- 控制站高度
    target_name             		, -- aoa-目标名称
    altitude                		, -- aoa-无人机所在海拔高度
    distance_from_station   		, -- aoa-无人机距离监测站的距离
    speed_ms                  	, -- aoa-无人机飞行速度 (m/s)
    target_frequency_khz    		, -- aoa-目标使用频率 (k_hz)
    target_bandwidth_khz		    , -- aoa-目标带宽 (k_hz)
    target_signal_strength_db		, -- aoa-目标信号强度 (d_b)
    split_index(REGEXP_REPLACE(id,'\u0002',''),';',0)                                      as uav_id,
    coalesce(split_index(REGEXP_REPLACE(id,'\u0002',''),';',1),'RADAR')                    as merge_type,
    coalesce(split_index(REGEXP_REPLACE(id,'\u0002',''),';',2),'1')                        as merge_cnt,
    coalesce(split_index(REGEXP_REPLACE(id,'\u0002',''),';',3),'1')                        as merge_target_cnt,
    coalesce(cast(split_index(REGEXP_REPLACE(id,'\u0002',''),';',4) as double),longitude)  as longitude,
    coalesce(cast(split_index(REGEXP_REPLACE(id,'\u0002',''),';',5) as double),latitude)   as latitude,
    target_type,
    target_signal_strength_db,
    target_list_status,
    target_list_type,
    user_company_name,
    user_full_name,
    target_direction,
    target_area_code,
    target_registered,
    target_fly_report_status,
    home_longitude,
    home_latitude,
    no_fly_zone_id,
    user_phone,
    PROCTIME() as proctime
from uav_merge_target_kafka
where -- src_code in ('RID','AOA','阵地','1')
    src_code <> 'RADAR'
   or (src_code='RADAR' and (id is not null or target_type=9));
-- 将雷达所有类型入融合，融合上返回id，融合不上返回null
-- 这里id不为空就是融合的，类型为9的是无人机
-- 因为雷达识别目标类型错误，可能将无人机识别成未知，所有不能在merge程序中直接筛选类型为9的，全部入融合，只有融合上和无人机入库


-- 数据union 整合字段，入库融合表
create view temp02 as
select
    coalesce(t1.uav_id,src_pk)                                   as uav_id,
    if(t1.uav_id is not null,concat('cs',t1.uav_id),t1.uav_id)   as control_station_id,
    t1.device_id,
    acquire_time,
    src_code,
    src_pk,
    merge_type,
    cast(merge_cnt as bigint)          as merge_cnt,
    cast(merge_target_cnt as bigint)   as merge_target_cnt,
    device_name,
    rid_devid,
    msgtype,
    recvtype,
    t1.recvmac,
    mac,
    rssi,
    t1.longitude,
    t1.latitude,
    t1.old_longitude,
    t1.old_latitude,
    location_alit,
    ew,
    speed_h,
    speed_v,
    height,
    height_type,
    control_station_longitude,
    control_station_latitude,
    control_station_height,
    target_name,
    altitude,
    distance_from_station,
    speed_ms,
    target_frequency_khz,
    target_bandwidth_khz,
    target_signal_strength_db,

    target_list_status,
    target_list_type,
    user_company_name,
    user_full_name,
    target_direction,
    target_area_code,
    target_registered,
    target_fly_report_status,
    home_longitude,
    home_latitude,
    no_fly_zone_id,
    user_phone,

    coalesce(t2.device_id,t4.device_id)         as join_dushu_uav_device_id,
    coalesce(t2.name,t4.name)                   as join_dushu_uav_name,
    coalesce(t2.manufacturer,t4.manufacturer)   as join_dushu_uav_manufacturer,
    coalesce(t2.model,t4.model)                 as join_dushu_uav_model,
    coalesce(t2.owner,t4.owner)                 as join_dushu_uav_owner,
    coalesce(t2.type,t4.type)                   as join_dushu_uav_type,
    coalesce(t2.status,t4.status)               as join_dushu_uav_status,

    t3.id                                       as doris_uav_join_id,
    t3.name                                     as doris_uav_join_name,
    t3.recvmac                                  as doris_uav_join_recvmac,
    t3.manufacturer                             as doris_uav_join_manufacturer,
    t3.model                                    as doris_uav_join_model,
    t3.owner                                    as doris_uav_join_owner,
    t3.type                                     as doris_uav_join_type,
    t3.category                                 as doris_uav_join_category,
    t3.phone                                    as doris_uav_join_phone,
    t3.empty_weight                             as doris_uav_join_empty_weight,
    t3.maximum_takeoff_weight                   as doris_uav_join_maximum_takeoff_weightn,
    t3.purpose                                  as doris_uav_join_purpose,
    proctime,

    concat(
            ifnull(cast(t1.longitude as varchar),''),'¥',
            ifnull(cast(t1.latitude as varchar),''),'¥',
            ifnull(cast(location_alit as varchar),''),'¥',
            ifnull(cast(ew as varchar),''),'¥',
            ifnull(cast(height as varchar),''),'¥',
            ifnull(cast(speed_v as varchar),''),'¥',
            ifnull(cast(speed_h as varchar),''),'¥',
            ifnull(recvtype,''),'¥',
            ifnull(cast(rssi as varchar),''),'¥',
            ifnull(t2.`status`,''),'¥',
            ifnull(cast(control_station_longitude as varchar),''),'¥',
            ifnull(cast(control_station_latitude as varchar),''),'¥',
            ifnull(rid_devid,''),'¥',
            ifnull(src_code,''),'¥',
            ifnull(merge_type,''),'¥',
            ifnull(merge_cnt,''),'¥',
            ifnull(merge_target_cnt,''),'¥',
            ifnull(target_name,''),'¥',
            ifnull(cast(altitude as varchar),''),'¥',
            ifnull(cast(distance_from_station as varchar),''),'¥',
            ifnull(cast(speed_ms as varchar),''),'¥',
            ifnull(cast(target_frequency_khz as varchar),''),'¥',
            ifnull(cast(target_bandwidth_khz as varchar),''),'¥',
            ifnull(cast(target_signal_strength_db as varchar),''),'¥',
            ifnull(cast(target_list_status as varchar),''),'¥',
            ifnull(cast(target_registered as varchar),''),'¥',
            ifnull(cast(target_fly_report_status as varchar),''),'¥',
            ifnull(cast(home_longitude as varchar),''),'¥',
            ifnull(cast(home_latitude as varchar),''),'¥',
            ifnull(cast(no_fly_zone_id as varchar),'')
    ) as filter_col

from temp01 as t1
         left join device FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 设备表 关联无人机
                   on t1.uav_id = t2.sn  and 'UAV' = t2.type

         left join device FOR SYSTEM_TIME AS OF t1.proctime as t4   -- 设备表 关联无人机
                   on t1.src_pk = t4.sn and 'UAV' = t4.type

         left join dws_et_uav_info_source FOR SYSTEM_TIME AS OF t1.proctime as t3   -- 设备表 关联无人机
                   on t1.uav_id = t3.id;






-----------------------

-- 数据写入

-----------------------


begin statement set;


-- rid、控制站、无人机关系表
insert into dws_rl_rid_uav_rt
select
    uav_id,
    control_station_id,
    device_id,
    rid_devid,
    acquire_time,
    update_time
from (
         select
             uav_id,
             control_station_id,
             device_id,
             rid_devid,
             acquire_time,
             from_unixtime(unix_timestamp()) as update_time,
             count(1) over (partition by uav_id,control_station_id order by proctime) as cnt
         from temp02
     ) a
where cnt=1;



-- 控制站实体表
insert into dws_et_control_station_info
select
    control_station_id                                          as id,
    acquire_time,
    coalesce(join_dushu_uav_name,doris_uav_join_name,uav_id)    as name, -- 无人机id-sn号
    uav_id                            as register_uav,
    src_code                          as source,
    uav_id                            as search_content,
    from_unixtime(unix_timestamp())   as update_time
from temp02
where doris_uav_join_id is null;


-- 无人机实体表
insert into dws_et_uav_info
select
    uav_id                       as id,
    if(src_code <> 'RADAR',uav_id,cast(null as varchar))            as sn,
    coalesce(join_dushu_uav_name,doris_uav_join_name,uav_id,target_name)  as name,
    join_dushu_uav_device_id                                              as device_id,
    coalesce(doris_uav_join_recvmac,recvmac) as recvmac,
    coalesce(join_dushu_uav_manufacturer,doris_uav_join_manufacturer)     as manufacturer,
    coalesce(join_dushu_uav_model,doris_uav_join_model,target_name)       as model,
    coalesce(join_dushu_uav_owner,doris_uav_join_owner)                   as owner,
    coalesce(join_dushu_uav_type,doris_uav_join_type)                     as type,
    src_code as source,
    concat(
            ifnull(coalesce(join_dushu_uav_name,doris_uav_join_name,uav_id),''),' ',
            ifnull(uav_id,'')
    )         as search_content,
    from_unixtime(unix_timestamp()) as update_time,
    cast(null as string  ) as category                 , -- 类别
    cast(null as string  ) as card_code                , -- 所有人身份证号/统一信用代码
    cast(null as string  ) as card_type_name           , -- 证件类型名称
    cast(null as string  ) as phone                    , -- 电话
    cast(null as string  ) as email                    , -- 邮箱
    cast(null as string  ) as company_name             , -- 持有单位
    cast(null as string  ) as empty_weight             , -- 空机重量
    cast(null as string  ) as maximum_takeoff_weight   , -- 最大起飞重量
    cast(null as int     ) as identity_type            , -- 无人机身份类型（0 未知，1 无人机，2 低慢小）
    cast(null as string  ) as identity_type_name       , -- 无人机身份类型名称
    cast(null as string  ) as engine_type              , -- 动力类型
    cast(null as string  ) as area_code                , -- 所属地区
    cast(null as string  ) as address                  , -- 详细地址
    cast(null as string  ) as username                 , -- 持有者姓名
    cast(null as string  ) as user_type_name           , -- 用户类型名称
    cast(null as int     ) as list_status              , -- 名单状态（0 正常，1 白名单，2 灰名单）
    cast(null as int     ) as list_type                , -- 名单类型(1 警用白名单，2 低空经济白名单，3 政务白名单， 4 多次黑飞黑名单， 5 重点人员黑名单)
    cast(null as string  ) as gmt_expire               , -- 过期时间
    cast(null as boolean ) as real_name                , -- 是否实名（0 未实名，1已实名）
    cast(null as string  ) as gmt_register             , -- 实名注册时间
    cast(null as string  ) as residence                , -- 居住地
    cast(null as string  ) as purpose                   -- 用途
from temp02
where doris_uav_join_id is null;



-- 融合数据入库
insert into dwd_bhv_merge_target_rt
select
    uav_id                         ,
    control_station_id             ,
    device_id,
    acquire_time                   ,
    src_code                       ,
    src_pk                	     ,
    merge_type,
    merge_cnt,
    merge_target_cnt,
    join_dushu_uav_device_id as uav_device_id,
    device_name,
    rid_devid                      ,
    msgtype                        ,
    recvtype                       ,
    recvmac                        ,
    mac                            ,
    rssi                           ,
    longitude                      ,
    latitude                       ,
    old_longitude,
    old_latitude,
    location_alit                  ,
    ew                             ,
    speed_h                        ,
    speed_v                        ,
    height                         ,
    height_type                    ,
    control_station_longitude      ,
    control_station_latitude       ,
    control_station_height 	     ,
    target_name             	     ,
    altitude                	     ,
    distance_from_station   	     ,
    speed_ms                       ,
    target_frequency_khz    	     ,
    target_bandwidth_khz		     ,
    target_signal_strength_db	     ,
    target_list_status,
    target_list_type,
    user_company_name,
    user_full_name,
    target_direction,
    target_area_code,
    target_registered,
    target_fly_report_status,
    home_longitude,
    home_latitude,
    no_fly_zone_id,
    user_phone,
    filter_col                     ,
    from_unixtime(unix_timestamp()) as update_time
from temp02;


insert into dwd_bhv_uav_illegal_list_rt
select
    uav_id as sn , -- 无人机id
    1 as type, -- 违法类型 1：超高 144 米
    acquire_time as acquire_time, -- 采集时间
    longitude as longitude , -- 经度
    latitude as latitude , -- 纬度
    altitude as altitude , -- 高度
    speed_ms as speed_ms, -- 速度
    from_unixtime(unix_timestamp()) as update_time  -- 更新时间
from temp02
where altitude>144
  and (join_dushu_uav_status is null or join_dushu_uav_status = '' or join_dushu_uav_status = 'OFFLINE');



-- -- 融合状态数据入库
-- insert into dws_bhv_merge_target_last_location_rt
-- select
--   uav_id                         ,
--   control_station_id             ,
--   device_id,
--   acquire_time                   ,
--   src_code                       ,
--   src_pk                	     ,
--   merge_type,
--   merge_cnt,
--   merge_target_cnt,
--   join_dushu_uav_device_id as uav_device_id,
--   device_name,
--   rid_devid                      ,
--   msgtype                        ,
--   recvtype                       ,
--   recvmac                        ,
--   mac                            ,
--   rssi                           ,
--   longitude                      ,
--   latitude                       ,
--   old_longitude,
--   old_latitude,
--   location_alit                  ,
--   ew                             ,
--   speed_h                        ,
--   speed_v                        ,
--   height                         ,
--   height_type                    ,
--   control_station_longitude      ,
--   control_station_latitude       ,
--   control_station_height 	     ,
--   target_name             	     ,
--   altitude                	     ,
--   distance_from_station   	     ,
--   speed_ms                       ,
--   target_frequency_khz    	     ,
--   target_bandwidth_khz		     ,
--   target_signal_strength_db	     ,
--   filter_col                     ,
--   from_unixtime(unix_timestamp()) as update_time
-- from temp02;



-- *********** 规则引擎数据 ***********
insert into uav_source
select
    uav_id                      as  id,
    join_dushu_uav_name         as name,
    join_dushu_uav_type         as type,
    join_dushu_uav_model        as model,
    join_dushu_uav_manufacturer as manufacturer,
    ew,
    height,
    location_alit   as locationAlit,
    speed_v         as speedV,
    speed_h         as speedH,
    recvtype,
    rssi,
    longitude       as lng,
    latitude        as lat,
    old_longitude   as longitude,
    old_latitude    as latitude,
    acquire_time    as acquireTime,
    'UAV'           as targetType,
    from_unixtime(unix_timestamp())  as updateTime
from temp02;


-- -- 写入无人机数据表
-- insert into dwd_et_uav_jh_info
-- select
--   uavInfo.id as id,
--   uavInfo.type as type,
--   uavInfo.typeName as type_name,
--   uavInfo.fullName as full_name,
--   uavInfo.sn as sn,
--   uavInfo.uavModelName as uav_model_name,
--   uavInfo.uavCompanyName as uav_company_name,
--   uavInfo.gmtRegister as gmt_register,
--   uavInfo.areaCode as area_code,
--   uavInfo.address as address,
--   uavInfo.username as username,
--   uavInfo.cardCode as card_code,
--   uavInfo.phone as phone,
--   uavInfo.`delete` as deleted,
--   uavInfo.gmtCreate as gmt_create,
--   from_unixtime(unix_timestamp())  as update_time
-- from jh_uav_info
-- where uavInfo.id is not null;

-- -- 写入白名单数据表
-- insert into dwd_et_uav_jh_white_info
-- select
--   whiteInfo.id as id,
--   whiteInfo.sn as sn,
--   whiteInfo.uavModelName as uav_model_name,
--   whiteInfo.maxHeight as max_height,
--   whiteInfo.engineType as engine_type,
--   whiteInfo.companyName as company_name,
--   whiteInfo.listStatus as list_status,
--   whiteInfo.listType as list_type,
--   whiteInfo.gmtExpire as gmt_expire,
--   whiteInfo.`delete` as deleted,
--   whiteInfo.gmtCreate as gmt_create,
--   from_unixtime(unix_timestamp())  as update_time
-- from jh_uav_info
-- where whiteInfo.id is not null;

-- -- 写入用户数据表
-- insert into dwd_et_uav_jh_user_info
-- select
--   userInfo.id as id,
--   userInfo.username as username,
--   userInfo.userTypeName as user_type_name,
--   userInfo.cardTypeName as card_type_name,
--   userInfo.phone as phone,
--   userInfo.cardCode as card_code,
--   userInfo.areaCode as area_code,
--   userInfo.realName as real_name,
--   userInfo.gmtRegister as gmt_register,
--   userInfo.fullName as full_name,
--   userInfo.residence as residence,
--   userInfo.listStatus as list_status,
--   userInfo.`delete` as deleted,
--   userInfo.gmtCreate as gmt_create,
--   from_unixtime(unix_timestamp())  as update_time
-- from jh_uav_info
-- where userInfo.id is not null;




-- -- 警航数据写入实体表
-- insert into dws_et_uav_info
-- select
--   coalesce(uavInfo.sn,whiteInfo.sn)                       as id,
--   coalesce(uavInfo.sn,whiteInfo.sn)            as sn,
--   coalesce(whiteInfo.uavModelName,uavInfo.uavModelName,uavInfo.sn,b.name)  as name,
--   b.device_id                                              as device_id,
--   b.recvmac as recvmac,
--   coalesce(uavInfo.uavCompanyName,b.manufacturer)     as manufacturer,
--   coalesce(whiteInfo.uavModelName,uavInfo.uavModelName,b.model)       as model,
--   coalesce(whiteInfo.fullName,userInfo.fullName,uavInfo.fullName,b.owner)          as owner,
--   b.type                     as type,
--   'ZHENDI' as source,
--   concat(
--     ifnull(coalesce(whiteInfo.uavModelName,uavInfo.cardCode,uavInfo.sn),''),' ',
--     ifnull(coalesce(uavInfo.sn,whiteInfo.sn),'')
--    )         as search_content,
--   from_unixtime(unix_timestamp()) as update_time,
--   b.category as category                 , -- 类别
--   coalesce(uavInfo.cardCode,userInfo.cardCode) as card_code                , -- 所有人身份证号/统一信用代码
--   userInfo.cardTypeName as card_type_name           , -- 证件类型名称
--   coalesce(whiteInfo.phone,uavInfo.phone,userInfo.phone) as phone                    , -- 电话
--   cast(null as string  ) as email                    , -- 邮箱
--   whiteInfo.companyName as company_name             , -- 持有单位
--   b.empty_weight as empty_weight             , -- 空机重量
--   b.maximum_takeoff_weight as maximum_takeoff_weight   , -- 最大起飞重量
--   uavInfo.type as identity_type            , -- 无人机身份类型（0 未知，1 无人机，2 低慢小）
--   uavInfo.typeName as identity_type_name       , -- 无人机身份类型名称
--   whiteInfo.engineType as engine_type              , -- 动力类型
--   coalesce(whiteInfo.areaCode,uavInfo.areaCode,userInfo.areaCode) as area_code                , -- 所属地区
--   uavInfo.address as address                  , -- 详细地址
--   coalesce(uavInfo.username,userInfo.username) as username                 , -- 持有者姓名
--   userInfo.userTypeName as user_type_name           , -- 用户类型名称
--   coalesce(whiteInfo.listStatus,userInfo.listStatus) as list_status              , -- 名单状态（0 正常，1 白名单，2 灰名单）
--   whiteInfo.listType as list_type                , -- 名单类型(1 警用白名单，2 低空经济白名单，3 政务白名单， 4 多次黑飞黑名单， 5 重点人员黑名单)
--   replace(replace(whiteInfo.gmtExpire,'T',' '),'.000+00:00','') as gmt_expire               , -- 过期时间
--   userInfo.realName as real_name                , -- 是否实名（0 未实名，1已实名）
--   replace(replace(coalesce(uavInfo.gmtRegister,userInfo.gmtRegister),'T',' '),'.000+00:00','') as gmt_register             , -- 实名注册时间
--   userInfo.residence as residence                , -- 居住地
--   b.purpose as purpose                   -- 用途
-- from jh_uav_info a
--   left join dws_et_uav_info_source FOR SYSTEM_TIME AS OF a.proctime as b   -- 设备表 关联无人机
--   on a.uavInfo.sn = b.id;


-- insert into dws_et_uav_pilot_info
-- select
--   cast(userInfo.id as string) as id,
--   userInfo.userTypeName as a_method,
--   coalesce(userInfo.fullName,uavInfo.fullName,whiteInfo.fullName,uavInfo.username,userInfo.username) as name,
--   coalesce(uavInfo.cardCode,userInfo.cardCode) as card_code,
--   coalesce(userInfo.phone,uavInfo.phone,whiteInfo.phone) as phone,
--   cast(null as string  ) as email,
--   uavInfo.address as address,
--   coalesce(uavInfo.sn,whiteInfo.sn)  as register_uav,
--   coalesce(userInfo.username,uavInfo.username) as username,
--   userInfo.userTypeName as user_type_name,
--   userInfo.cardTypeName as card_type_name,
--   coalesce(whiteInfo.areaCode,uavInfo.areaCode,userInfo.areaCode)  as area_code,
--   userInfo.realName as real_name,
--   replace(replace(coalesce(uavInfo.gmtRegister,userInfo.gmtRegister),'T',' '),'.000+00:00','') as gmt_register,
--   coalesce(userInfo.listStatus,whiteInfo.listStatus) as list_status,
--   concat(
--     nullif(coalesce(userInfo.fullName,uavInfo.fullName,whiteInfo.fullName,uavInfo.username,userInfo.username),''),' ',
--     nullif(coalesce(uavInfo.cardCode,userInfo.cardCode),'')
--   ) as search_content,
--   from_unixtime(unix_timestamp()) as update_time
-- from jh_uav_info
-- where userInfo.id is not null;



-- insert into dws_rl_uav_pilot
-- select
--    uavInfo.sn as uav_id , -- 无人机的id-sn号
--    cast(userInfo.id as string) as pilot_id , -- 飞手
--    from_unixtime(unix_timestamp())  as update_time  -- 更新时间
-- from jh_uav_info
-- where userInfo.id is not null;



end;

