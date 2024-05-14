--********************************************************************--
-- author:      yibo
-- create time: 2023/10/21 22:44:32
-- description: adsbexchange数据消费入库
--********************************************************************--
set 'pipeline.name' = 'ja-adsbexchange-aircraft-rt';


set 'parallelism.default' = '4';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '600000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-adsbexchange-aircraft-rt';


 -----------------------

 -- 数据结构

 -----------------------

-- 创建kafka全量数据表（Source：kafka）
drop table if exists adsb_exchange_aircraft_list_kafka;
create table adsb_exchange_aircraft_list_kafka(
                                                  hex                  string,     -- 飞机的 24 位 ICAO 标识符，为 6 个十六进制数字
                                                  seen_pos             double,     -- 上次更新位置的时间（在“now”之前的秒数）
                                                  flight               string,     -- 呼号、航班名称或飞机注册，8 个字符
                                                  alt_geom             double,     -- 高度 参考 WGS84 椭球体的几何 (GNSS / INS) 高度（以英尺为单位）
                                                  nic                  bigint,     -- 导航完整性类别
                                                  emergency            double,     -- ADS-B紧急/优先状态 无、一般、救生员、minfuel、nordo、非法、击落、保留）
                                                  lon                  double,     -- 经度
                                                  lat                  double,     -- 纬度
                                                  nogps                bigint,     -- 0 - 表示飞机是否具有GPS（0表示有GPS）
                                                  type                 string,     -- 消息的类型/该位置/飞机的当前数据的最佳来源
                                                  seen                 double,     -- 多久以前（以“现在”之前的几秒为单位）最后一次从这架飞机收到消息
                                                  nav_qnh              double,     -- 高度计设置的高度（QFE 或 QNH/QNE）、hPa
                                                  adsb_version         bigint,     -- ADS-B协议版本
                                                  squawk               string,     -- A模式应答机代码，编码为4个八进制数字
                                                  gva                  double,     -- 几何垂直精度
                                                  nic_a                bigint,     -- 导航完整性类别
                                                  sil                  bigint,     -- 源完整性级别
                                                  sil_type             bigint,     -- SIL 的解释：未知、每小时、每个样本
                                                  receiverCount        bigint,     -- 当前接收来自该飞机的数据的接收器数量
                                                  nic_baro             bigint,     -- 气压高度的导航完整性类别
                                                  track                double,     -- 地面上的方向角度轨迹，以度为单位（0-359）
                                                  airground            bigint,     -- 表示飞机当前在空中还是地面上
                                                  tisb_version         bigint,     -- tis-b 版本
                                                  dbFlags              bigint,     -- 某些数据库标志的位字段
                                                  alt_baro             double,     -- 飞机气压高度（以英尺为单位）作为数字或“地面”作为字符串
                                                  geom_rate            double,     -- 几何（GNSS / INS）高度的变化率，英尺/分钟
                                                  rssi                 double,     -- 最近平均RSSI（信号功率），以dbFS为单位；这将永远是负面的
                                                  nav_altitude_mcp     double,     -- 从模式控制面板/飞行控制单元（MCP/FCU）或同等设备选择的高度
                                                  gs                   double,     -- 地面速度（节）
                                                  spi                  int,        -- 飞行状态特殊位置标识位
                                                  version              bigint,     -- 版本：ADS-B 版本号 0、1、2（3-7 保留）
                                                  rc                   double,     -- 限定半径，米；源自 NIC 和补充位的位置完整性度量
                                                  sda                  bigint,     -- 系统设计保证
                                                  r                    string,     -- 这册号
                                                  alert1               bigint,     -- 表示是否存在警报情况（0表示无警报）
                                                  t                    string,     -- 从数据库中提取的飞机类型
                                                  extraFlags           bigint,     -- 附加标志
                                                  messages             bigint,     -- 处理的 S 模式消息总数（任意）
                                                  nowtime              double,     -- 文件生成时间
                                                  adsr_version         bigint,     -- ADS-R协议版本
                                                  nac_p                double,     -- 位置导航精度
                                                  category             string,     -- 发射器类别，用于识别特定飞机或车辆类别
                                                  nac_v                bigint     -- 速度导航精度
) with (
      'connector' = 'kafka',
      'topic' = 'adsb-exchange-aircraft-list',
      -- 'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
      'properties.group.id' = 'adbs-exchange-aircraft-list-group',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 创建写入doris全量数据表（Sink：doris）
drop table if exists dwd_adsbexchange_aircraft_list_rt;
create table dwd_adsbexchange_aircraft_list_rt (
                                                   hex                  string,     -- 飞机的 24 位 ICAO 标识符，为 6 个十六进制数字
                                                   acquire_timestamp_format string ,-- 时间
                                                   seen_pos             double,     -- 上次更新位置的时间（在“now”之前的秒数）
                                                   flight               string,     -- 呼号、航班名称或飞机注册，8 个字符
                                                   alt_geom             double,     -- 高度 参考 WGS84 椭球体的几何 (GNSS / INS) 高度（以英尺为单位）
                                                   nic                  bigint,     -- 导航完整性类别
                                                   emergency            double,     -- ADS-B紧急/优先状态 无、一般、救生员、minfuel、nordo、非法、击落、保留）
                                                   lon                  double,     -- 经度
                                                   lat                  double,     -- 纬度
                                                   nogps                bigint,     -- 0 - 表示飞机是否具有GPS（0表示有GPS）
                                                   type                 string,     -- 消息的类型/该位置/飞机的当前数据的最佳来源
                                                   seen                 double,     -- 多久以前（以“现在”之前的几秒为单位）最后一次从这架飞机收到消息
                                                   nav_qnh              double,     -- 高度计设置的高度（QFE 或 QNH/QNE）、hPa
                                                   adsb_version         bigint,     -- ADS-B协议版本
                                                   squawk               string,     -- A模式应答机代码，编码为4个八进制数字
                                                   gva                  double,     -- 几何垂直精度
                                                   nic_a                bigint,     -- 导航完整性类别
                                                   sil                  bigint,     -- 源完整性级别
                                                   sil_type             bigint,     -- SIL 的解释：未知、每小时、每个样本
                                                   receiver_count       bigint,     -- 当前接收来自该飞机的数据的接收器数量
                                                   nic_baro             bigint,     -- 气压高度的导航完整性类别
                                                   track                double,     -- 地面上的方向角度轨迹，以度为单位（0-359）
                                                   airground            bigint,     -- 表示飞机当前在空中还是地面上
                                                   tisb_version         bigint,     -- tis-b 版本
                                                   db_flags             bigint,     -- 某些数据库标志的位字段
                                                   alt_baro             double,     -- 飞机气压高度（以英尺为单位）作为数字或“地面”作为字符串
                                                   geom_rate            double,     -- 几何（GNSS / INS）高度的变化率，英尺/分钟
                                                   rssi                 double,     -- 最近平均RSSI（信号功率），以dbFS为单位；这将永远是负面的
                                                   nav_altitude_mcp     double,     -- 从模式控制面板/飞行控制单元（MCP/FCU）或同等设备选择的高度
                                                   gs                   double,     -- 地面速度（节）
                                                   spi                  int,        -- 飞行状态特殊位置标识位
                                                   version              bigint,     -- 版本：ADS-B 版本号 0、1、2（3-7 保留）
                                                   rc                   double,     -- 限定半径，米；源自 NIC 和补充位的位置完整性度量
                                                   sda                  bigint,     -- 系统设计保证
                                                   r                    string,     -- 这册号
                                                   alert1               bigint,     -- 表示是否存在警报情况（0表示无警报）
                                                   t                    string,     -- 从数据库中提取的飞机类型
                                                   extra_flags          bigint,     -- 附加标志
                                                   messages             bigint,     -- 处理的 S 模式消息总数（任意）
                                                   nowtime              double,     -- 文件生成时间
                                                   adsr_version         bigint,     -- ADS-R协议版本
                                                   nac_p                double,     -- 位置导航精度
                                                   category             string,     -- 发射器类别，用于识别特定飞机或车辆类别
                                                   nac_v                bigint,     -- 速度导航精度
                                                   h3_code              string,     -- h3-code
                                                   update_time          string  	-- 数据入库时间

) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'table.identifier' = 'sa.dwd_adsbexchange_aircraft_list_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



drop table if exists dws_aircraft_combine_list_rt;
create table dws_aircraft_combine_list_rt (
                                              flight_id											varchar(20) 	comment '飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码',
                                              acquire_time										string 		comment '采集时间',
                                              src_code											int 			comment '来源网站标识 1. radarbox 2. adsbexchange',
                                              icao_code											varchar(10) 	comment '24位 icao编码',
                                              registration										varchar(20) 	comment '注册号',
                                              flight_no											varchar(20) 	comment '航班号',
                                              callsign											varchar(20) 	comment '呼号',
                                              flight_type										varchar(20) 	comment '飞机型号',
                                              is_military										int 			comment '是否军用飞机 0 非军用 1 军用',
                                              pk_type											varchar(10) 	comment 'flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex',
                                              src_pk											varchar(20) 	comment '源网站主键',
                                              flight_category									varchar(10) 	comment '飞机类型',
                                              flight_category_name						        varchar(50) 	comment '飞机类型名称',
                                              lng												double 			comment '经度',
                                              lat												double 			comment '纬度',
                                              speed												double 			comment '飞行当时的速度（单位：节）',
                                              speed_km											double 			comment '速度单位 km/h',
                                              altitude_baro										double 			comment '气压高度 海拔 航班当前高度，单位为（ft）',
                                              altitude_baro_m									double 			comment '气压高度 海拔 单位米',
                                              altitude_geom										double 			comment '海拔高度 海拔 航班当前高度，单位为（ft）',
                                              altitude_geom_m									double 			comment '海拔高度 海拔 单位米',
                                              heading											double 			comment '方向  正北为0 ',
                                              squawk_code										varchar(10) 	comment '当前应答机代码',
                                              flight_status										varchar(20) 	comment '飞机状态： 已启程',
                                              special											int 			comment '是否有特殊情况',
                                              origin_airport3_code						        varchar(10) 	comment '起飞机场的iata代码',
                                              origin_airport_e_name						        varchar(50) 	comment '来源机场英文',
                                              origin_airport_c_name						        varchar(100) 	comment '来源机场中文',
                                              origin_lng										double 			comment '来源机场经度',
                                              origin_lat										double 			comment '来源机场纬度',
                                              dest_airport3_code							    varchar(10) 	comment '目标机场的 iata 代码',
                                              dest_airport_e_name							    varchar(50) 	comment '目的机场英文',
                                              dest_airport_c_name							    varchar(100) 	comment '目的机场中文',
                                              dest_lng											double 			comment '目的地坐标经度',
                                              dest_lat											double 			comment '目的地坐标纬度',
                                              flight_photo										varchar(200) 	comment '飞机的图片',
                                              flight_departure_time						        string 		comment '航班起飞时间',
                                              expected_landing_time						        string 		comment '预计降落时间',
                                              to_destination_distance					        double 			comment '目的地距离',
                                              estimated_landing_duration			            double 			comment '预计还要多久着陆',
                                              airlines_icao										varchar(10) 	comment '航空公司的icao代码',
                                              airlines_e_name									varchar(50) 	comment '航空公司英文',
                                              airlines_c_name									varchar(100) 	comment '航空公司中文',
                                              country_code										varchar(10) 	comment '飞机所属国家代码',
                                              country_name										varchar(50) 	comment '国家中文',
                                              data_source										varchar(20) 	comment '数据来源的系统',
                                              source											varchar(20) 	comment '来源',
                                              position_country_code2					        varchar(2) 		comment '位置所在国家简称',
                                              position_country_name						        varchar(50) 	comment '位置所在国家名称',
                                              friend_foe										varchar(20) 	comment '敌我',
                                              sea_id											varchar(3) 		comment '海域id',
                                              sea_name											varchar(100) 	comment '海域名字',
                                              h3_code											varchar(20) 	comment '位置h3编码',
                                              extend_info										varchar(65533)  comment '扩展信息 json 串',
                                              update_time										string 		comment '更新时间'

) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'table.identifier' = 'sa.dws_aircraft_combine_list_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );

-- 飞机各个网站数据融合状态表
drop table if exists dws_aircraft_combine_status_rt;
create table dws_aircraft_combine_status_rt (
                                                flight_id											varchar(20) 	comment '飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码',
                                                src_code											int 			comment '来源网站标识 1. radarbox 2. adsbexchange',
                                                acquire_time										string 		    comment '采集时间',
                                                icao_code											varchar(10) 	comment '24位 icao编码',
                                                registration										varchar(20) 	comment '注册号',
                                                flight_no											varchar(20) 	comment '航班号',
                                                callsign											varchar(20) 	comment '呼号',
                                                flight_type										string 	        comment '飞机型号',
                                                is_military										int 			comment '是否军用飞机 0 非军用 1 军用',
                                                pk_type											varchar(10) 	comment 'flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex',
                                                src_pk											varchar(20) 	comment '源网站主键',
                                                flight_category									varchar(10) 	comment '飞机类型',
                                                flight_category_name						        varchar(50) 	comment '飞机类型名称',
                                                lng												double 			comment '经度',
                                                lat												double 			comment '纬度',
                                                speed												double 			comment '飞行当时的速度（单位：节）',
                                                speed_km											double 			comment '速度单位 km/h',
                                                altitude_baro										double 			comment '气压高度 海拔 航班当前高度，单位为（ft）',
                                                altitude_baro_m									double 			comment '气压高度 海拔 单位米',
                                                altitude_geom										double 			comment '海拔高度 海拔 航班当前高度，单位为（ft）',
                                                altitude_geom_m									double 			comment '海拔高度 海拔 单位米',
                                                heading											double 			comment '方向  正北为0 ',
                                                squawk_code										varchar(10) 	comment '当前应答机代码',
                                                flight_status										varchar(20) 	comment '飞机状态： 已启程',
                                                special											int 			comment '是否有特殊情况',
                                                origin_airport3_code						        varchar(10) 	comment '起飞机场的iata代码',
                                                origin_airport_e_name						        string 	        comment '来源机场英文',
                                                origin_airport_c_name						        string 	        comment '来源机场中文',
                                                origin_lng										double 			comment '来源机场经度',
                                                origin_lat										double 			comment '来源机场纬度',
                                                dest_airport3_code							    varchar(10) 	comment '目标机场的 iata 代码',
                                                dest_airport_e_name							    string 	        comment '目的机场英文',
                                                dest_airport_c_name							    string 	        comment '目的机场中文',
                                                dest_lng											double 			comment '目的地坐标经度',
                                                dest_lat											double 			comment '目的地坐标纬度',
                                                flight_photo										varchar(200) 	comment '飞机的图片',
                                                flight_departure_time						        string 		    comment '航班起飞时间',
                                                expected_landing_time						        string 		    comment '预计降落时间',
                                                to_destination_distance					        double 			comment '目的地距离',
                                                estimated_landing_duration			            double 			comment '预计还要多久着陆',
                                                airlines_icao										varchar(10) 	comment '航空公司的icao代码',
                                                airlines_e_name									string 	        comment '航空公司英文',
                                                airlines_c_name									varchar(100) 	comment '航空公司中文',
                                                country_code										varchar(10) 	comment '飞机所属国家代码',
                                                country_name										varchar(50) 	comment '国家中文',
                                                data_source										varchar(20) 	comment '数据来源的系统',
                                                source											varchar(20) 	comment '来源',
                                                position_country_code2					        varchar(2) 		comment '位置所在国家简称',
                                                position_country_name						        varchar(50) 	comment '位置所在国家名称',
                                                friend_foe										varchar(20) 	comment '敌我',
                                                sea_id											varchar(3) 		comment '海域id',
                                                sea_name											varchar(100) 	comment '海域名字',
                                                h3_code											varchar(20) 	comment '位置h3编码',
                                                extend_info									    string          comment '扩展信息 json 串',
                                                update_time										string 		    comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'table.identifier' = 'sa.dws_aircraft_combine_status_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );

drop table if exists dim_aircraft_type_category;
create table dim_aircraft_type_category (
                                            id 			string COMMENT '飞机机型',
                                            category_code 			string COMMENT '飞机类型代码',
                                            category_c_name 		string COMMENT '飞机类型中文名称',
                                            primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_aircraft_type_category',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );


-- 飞机实体表（Source：doris）
drop table if exists dws_aircraft_info;
create table dws_aircraft_info (
                                   icao_code           string        comment '飞机的 24 位 ICAO 标识符，为 6 个十六进制数字 大写',
                                   registration        string        comment '地区国家三位编码',
                                   icao_type           string        comment '飞机的机型型码，用于标识不同类型的飞机',
                                   is_mil              int           comment '是否军用飞机',
                                   operator		    string	comment '飞机的运营者，即运营和管理飞机的实体或公司。',
                                   operator_c_name		string	      comment '运营者的中文名',
                                   operator_icao		string		  comment '运营者的 ICAO 代码，用于标识飞机的运营者。',
                                   primary key (icao_code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_aircraft_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );

-- 国家数据匹配库（Source：doris）
drop table if exists dim_country_code_name_info;
create table dim_country_code_name_info (
                                            id                        string        comment '国家英文-id',
                                            source                    string        comment '来源',
                                            e_name                    string        comment '国家的英文',
                                            c_name                    string        comment '国家的中文',
                                            country_code2             string        comment '国家的编码2',
                                            primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_code_name_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );

-- 位置所在的国家代码转换（Source：doris）
drop table if exists dim_country_info;
create table dim_country_info (
                                  code2              string        comment '地区国家两位编码',
                                  code3              string        comment '地区国家三位编码',
                                  primary key (code3) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      -- 'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );

-- 航空器国籍登记代码表
drop table if exists dim_aircraft_country_prefix_code;
create table dim_aircraft_country_prefix_code (
                                                  prefix_code 	string  COMMENT '代码前缀',
                                                  country_code 	string  COMMENT '国家代码',
                                                  primary key (prefix_code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_aircraft_country_prefix_code',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );

-- 海域表
drop table if exists dim_sea_area;
create table dim_sea_area (
                              id 			varchar(5) COMMENT '海域编号',
                              name 		varchar(60) COMMENT '名称',
                              c_name 		varchar(60) COMMENT '中文名称',
                              primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_sea_area',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );



create function getCountry as 'com.jingan.udf.sea.GetCountryFromLngLat';
create function getSeaArea as 'com.jingan.udf.sea.GetSeaArea';
create function getH3CodeUber as 'com.jingan.udf.h3.H3CodeUber';
create function passThrough as 'com.jingan.udtf.PassThroughUdtf';







-----------------------

-- 数据处理

-----------------------


-- 计算h3、时间
drop view if exists tmp_table01;
create view tmp_table01 as
select
    hex                  ,
    from_unixtime(cast((nowtime - coalesce(seen_pos,seen,0)) as bigint),'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    -- timestampadd(second,seen_pos,cast(nowtime as TIMESTAMP)) as acquire_timestamp_format,
    -- cast(cast(nowtime as TIMESTAMP) + INTERVAL seen_pos SECOND as string) as acquire_timestamp_format,
    seen_pos             ,
    flight               ,
    alt_geom             ,
    nic                  ,
    emergency            ,
    lon                  ,
    lat                  ,
    nogps                ,
    type                 ,
    seen                 ,
    nav_qnh              ,
    adsb_version         ,
    squawk               ,
    gva                  ,
    nic_a                ,
    sil                  ,
    sil_type             ,
    receiverCount as receiver_count,
    nic_baro             ,
    track                ,
    airground            ,
    tisb_version         ,
    dbFlags  as db_flags ,
    alt_baro             ,
    geom_rate            ,
    rssi                 ,
    nav_altitude_mcp     ,
    gs                   ,
    spi                  ,
    version              ,
    rc                   ,
    sda                  ,
    r                    ,
    alert1               ,
    t                    ,
    extraFlags as extra_flags,
    messages             ,
    nowtime              ,
    adsr_version         ,
    nac_p                ,
    category             ,
    nac_v                ,
    getH3CodeUber(lon,lat) as h3_code,
    if(lon is null or lat is null,cast(null as string),getCountry(lon,lat)) as country_code3,
    if(lon is null or lat is null,cast(null as string),getSeaArea(lon,lat)) as sea_id,
    PROCTIME()  as proctime
from adsb_exchange_aircraft_list_kafka
-- ,lateral table(passThrough(if(lon is null or lat is null,cast(null as string),getCountry(lon,lat)))) as t1(country_code3)
-- ,lateral table(passThrough(if(lon is null or lat is null,cast(null as string),getSeaArea(lon,lat)))) as t2(sea_id)
-- ,lateral table(passThrough(getH3CodeUber(lon,lat))) as t3(h3_code)
where nowtime is not null
  -- and nowtime <> ''
  and hex is not null
  and hex <> '';


drop view if exists tmp_dws_aircraft_combine_list_rt_02;
create view tmp_dws_aircraft_combine_list_rt_02 as
select
    upper(hex) as flight_id											, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
    acquire_timestamp_format as acquire_time										, -- 采集时间
    2 as src_code											, -- 来源网站标识 1. radarbox 2. adsbexchange
    if(left(hex,1)='~',cast(null as string),upper(hex)) as icao_code											, -- 24位 icao编码
    coalesce(if(r='',cast(null as string),r),c.registration) as registration										, -- 注册号
    flight as flight_no											, -- 航班号
    cast(null as string) callsign											, -- 呼号
    coalesce(if(t='',cast(null as string),t),c.icao_type) as flight_type										, -- 飞机型号
    db_flags % 2 as is_military										, -- 是否军用飞机 0 非军用 1 军用
  if(left(hex,1)='~','non_icao','hex') as pk_type											, -- flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex
  hex as src_pk											, -- 源网站主键
  b.category_code as flight_category									, -- 飞机类型
  b.category_c_name as flight_category_name						, -- 飞机类型名称
  lon as lng												, -- 经度
  lat as lat												, -- 纬度
  gs as speed												, -- 飞行当时的速度（单位：节）
  cast(gs as double)*1.852 as speed_km											, -- 速度单位 km/h
  alt_baro as altitude_baro										, -- 气压高度 海拔 航班当前高度，单位为（ft）
  cast(alt_baro as double)*0.3048 as altitude_baro_m									, -- 气压高度 海拔 单位米
  alt_geom as altitude_geom										, -- 海拔高度 海拔 航班当前高度，单位为（ft）
  cast(alt_geom as double)*0.3048 as altitude_geom_m									, -- 海拔高度 海拔 单位米
  track as heading											, -- 方向  正北为0
  squawk as squawk_code										, -- 当前应答机代码
  cast(null as string) as flight_status										, -- 飞机状态： 已启程
  spi as special											, -- 是否有特殊情况
  cast(null as string) as origin_airport3_code						        , -- 起飞机场的iata代码
  cast(null as string) as origin_airport_e_name						        , -- 来源机场英文
  cast(null as string) as origin_airport_c_name						        , -- 来源机场中文
  cast(null as double) as origin_lng										, -- 来源机场经度
  cast(null as double) as origin_lat										, -- 来源机场纬度
  cast(null as string) as dest_airport3_code							    , -- 目标机场的 iata 代码
  cast(null as string) as dest_airport_e_name							    , -- 目的机场英文
  cast(null as string) as dest_airport_c_name							    , -- 目的机场中文
  cast(null as double) as dest_lng											, -- 目的地坐标经度
  cast(null as double) as dest_lat											, -- 目的地坐标纬度
  cast(null as string) as flight_photo										, -- 飞机的图片
  cast(null as string) as flight_departure_time						        , -- 航班起飞时间
  cast(null as string) as expected_landing_time						        , -- 预计降落时间
  cast(null as double) as to_destination_distance					        , -- 目的地距离
  cast(null as int) as estimated_landing_duration			            , -- 预计还要多久着陆
  c.operator_icao as airlines_icao										, -- 航空公司的icao代码
  c.operator as airlines_e_name									, -- 航空公司英文
  c.operator_c_name as airlines_c_name									, -- 航空公司中文
  -- country_code										, -- 飞机所属国家代码
  -- country_name										, -- 国家中文
  type as data_source										, -- 数据来源的系统
  cast(null as string) as source											, -- 来源
  country_code3, -- 经纬度位置转换国家 position_country_code2					        , -- 位置所在国家简称
  -- position_country_name						        , -- 位置所在国家名称
  -- friend_foe										, -- 敌我
  sea_id											, -- 海域id
  -- sea_name											, -- 海域名字
  h3_code as h3_code											, -- 位置h3编码
  concat('{',
  if(seen_pos is null,'',concat('"seen_pos":',cast(seen_pos as string),',')),
  if(seen is null,'',concat('"seen":',cast(seen as string),',')),
  if(nav_qnh is null,'',concat('"nav_qnh":',cast(nav_qnh as string),',')),
  if(nav_altitude_mcp is null,'',concat('"nav_altitude_mcp":',cast(nav_altitude_mcp as string),',')),
  if(geom_rate is null,'',concat('"geom_rate":',cast(geom_rate as string),',')),
  if(gva is null,'',concat('"gva":',cast(gva as string),',')),
  if(nic is null,'',concat('"nic":',cast(nic as string),',')),
  if(nic_a is null,'',concat('"nic_a":',cast(nic_a as string),',')),
  if(nic_baro is null,'',concat('"nic_baro":',cast(nic_baro as string),',')),
  if(airground is null,'',concat('"airground":',cast(airground as string),',')),
  if(rc is null,'',concat('"rc":',cast(rc as string),',')),
  if(nac_p is null,'',concat('"nac_p":',cast(nac_p as string),',')),
  if(nac_v is null,'',concat('"nac_v":',cast(nac_v as string),',')),
  if(nogps is null,'',concat('"nogps":',cast(nogps as string),',')),
  if(sil is null,'',concat('"sil":',cast(sil as string),',')),
  if(sil_type is null,'',concat('"sil_type":',cast(sil_type as string),',')),
  if(emergency is null,'',concat('"emergency":',cast(emergency as string),',')),
  if(rssi is null,'',concat('"rssi":',cast(rssi as string),',')),
  if(receiver_count is null,'',concat('"receiver_count":',cast(receiver_count as string),',')),
  if(messages is null,'',concat('"messages":',cast(messages as string),',')),
  if(alert1 is null,'',concat('"alert1":',cast(alert1 as string),',')),
  if(category is null or category='','',concat('"category":"',cast(category as string),'",')),
  if(db_flags is null,'',concat('"db_flags":',cast(db_flags as string),',')),
  if(extra_flags is null,'',concat('"extraFlags":',cast(extra_flags as string),',')),
  if(adsb_version is null,'',concat('"adsb_version":',cast(adsb_version as string),',')),
  if(adsr_version is null,'',concat('"adsr_version":',cast(adsr_version as string),',')),
  if(tisb_version is null,'',concat('"tisb_version":',cast(tisb_version as string),',')),
  if(version is null,'',concat('"version":',cast(version as string),',')),
  if(sda is null,'',concat('"sda":',cast(sda as string),',')),
  if(nowtime is null,'',concat('"nowtime":',cast(nowtime as string))),
  '}') as extend_info,										 -- 扩展信息 json 串
  a.proctime as proctime
  -- update_time										  -- 更新时间'
from tmp_table01 a
left join dim_aircraft_type_category
FOR SYSTEM_TIME AS OF a.proctime as b
on a.t=b.id
left join dws_aircraft_info
FOR SYSTEM_TIME AS OF a.proctime as c
on upper(a.hex)=c.icao_code;





-- 1.截取注册号 2.将国家代码转化为两位字母简称 3.海域名称
drop view if exists tmp_dws_aircraft_combine_list_rt_03;
create view tmp_dws_aircraft_combine_list_rt_03 as
select
    a.*,
    b.code2 as position_country_2code,  -- 当前所处的区域
    c.c_name as sea_name											, -- 海域名字
    if(instr(registration,'-')>0,substring(registration,1,2),concat(substring(registration,1,1),'-')) as prefix_code2,
    if(instr(registration,'-')>0,substring(registration,1,3),concat(substring(registration,1,2),'-')) as prefix_code3,
    if(instr(registration,'-')>0,substring(registration,1,4),concat(substring(registration,1,3),'-')) as prefix_code4,
    if(instr(registration,'-')>0,substring(registration,1,5),concat(substring(registration,1,4),'-')) as prefix_code5
from tmp_dws_aircraft_combine_list_rt_02 a
         left join dim_country_info
    FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.country_code3=b.code3
         left join dim_sea_area
    FOR SYSTEM_TIME AS OF a.proctime as c
                   on a.sea_id = c.id;

-- 1.注册号 前缀 转换国家 2.南海 东海 区域 划规中国
drop view if exists tmp_dws_aircraft_combine_list_rt_04;
create view tmp_dws_aircraft_combine_list_rt_04 as
select
    a.*,
    if(position_country_2code is null
           and ((lng between 107.491636 and 124.806089 and lat between 20.522241 and 40.799277)
            or
                (lng between 107.491636 and 121.433286 and lat between 3.011639 and 20.522241)
           )
        ,'CN', position_country_2code) as position_country_code2,
    coalesce(t7.country_code,t6.country_code,t5.country_code,t4.country_code) as country_code
from tmp_dws_aircraft_combine_list_rt_03 a
         left join dim_aircraft_country_prefix_code
    FOR SYSTEM_TIME AS OF a.proctime as t4
                   on a.prefix_code2=t4.prefix_code
         left join dim_aircraft_country_prefix_code
    FOR SYSTEM_TIME AS OF a.proctime as t5
                   on a.prefix_code3=t5.prefix_code
         left join dim_aircraft_country_prefix_code
    FOR SYSTEM_TIME AS OF a.proctime as t6
                   on a.prefix_code4=t6.prefix_code
         left join dim_aircraft_country_prefix_code
    FOR SYSTEM_TIME AS OF a.proctime as t7
                   on a.prefix_code5=t7.prefix_code;


-- 1. 关联位置所在的国家中文名 2.关联国家中文名 3. 敌我转换
drop view if exists tmp_dws_aircraft_combine_list_rt_05;
create view tmp_dws_aircraft_combine_list_rt_05 as
select
    a.*,
    b.c_name as position_country_name						        , -- 位置所在国家名称
    c.c_name as country_name						       , -- 飞机所属的国家
    case
        when country_code in('IN','US','JP','AU') and is_military=1 then 'ENEMY' -- 敌
        when country_code='CN' and is_military=1  then 'OUR_SIDE' -- 我
        when country_code='CN' and is_military=0  then 'FRIENDLY_SIDE' -- 友
        else 'NEUTRALITY' end friend_foe -- 敌我类型
from tmp_dws_aircraft_combine_list_rt_04 a
         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.position_country_code2 = b.country_code2 and 'COMMON' = b.source
         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF a.proctime as c
                   on a.country_code = c.country_code2 and 'COMMON' = c.source;




-----------------------

-- 数据插入

-----------------------

begin statement set;

insert into dwd_adsbexchange_aircraft_list_rt
select
    hex                  ,
    acquire_timestamp_format,
    seen_pos             ,
    flight               ,
    alt_geom             ,
    nic                  ,
    emergency            ,
    lon                  ,
    lat                  ,
    nogps                ,
    type                 ,
    seen                 ,
    nav_qnh              ,
    adsb_version         ,
    squawk               ,
    gva                  ,
    nic_a                ,
    sil                  ,
    sil_type             ,
    receiver_count       ,
    nic_baro             ,
    track                ,
    airground            ,
    tisb_version         ,
    db_flags             ,
    alt_baro             ,
    geom_rate            ,
    rssi                 ,
    nav_altitude_mcp     ,
    gs                   ,
    spi                  ,
    version              ,
    rc                   ,
    sda                  ,
    r                    ,
    alert1               ,
    t                    ,
    extra_flags          ,
    messages             ,
    nowtime              ,
    adsr_version         ,
    nac_p                ,
    category             ,
    nac_v                ,
    h3_code as h3_code ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_table01
where acquire_timestamp_format is not null
  and acquire_timestamp_format <> '';

insert into dws_aircraft_combine_list_rt
select
    flight_id											, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
    acquire_time										, -- 采集时间
    src_code											, -- 来源网站标识 1. radarbox 2. adsbexchange
    icao_code											, -- 24位 icao编码
    registration										, -- 注册号
    flight_no											, -- 航班号
    callsign											, -- 呼号
    flight_type										, -- 飞机型号
    is_military										, -- 是否军用飞机 0 非军用 1 军用
    pk_type											, -- flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex
    src_pk											, -- 源网站主键
    flight_category									, -- 飞机类型
    flight_category_name						        , -- 飞机类型名称
    lng												, -- 经度
    lat												, -- 纬度
    speed												, -- 飞行当时的速度（单位：节）
    speed_km											, -- 速度单位 km/h
    altitude_baro										, -- 气压高度 海拔 航班当前高度，单位为（ft）
    altitude_baro_m									, -- 气压高度 海拔 单位米
    altitude_geom										, -- 海拔高度 海拔 航班当前高度，单位为（ft）
    altitude_geom_m									, -- 海拔高度 海拔 单位米
    heading											, -- 方向  正北为0
    squawk_code										, -- 当前应答机代码
    flight_status										, -- 飞机状态： 已启程
    special											, -- 是否有特殊情况
    origin_airport3_code						        , -- 起飞机场的iata代码
    origin_airport_e_name						        , -- 来源机场英文
    origin_airport_c_name						        , -- 来源机场中文
    origin_lng										, -- 来源机场经度
    origin_lat										, -- 来源机场纬度
    dest_airport3_code							    , -- 目标机场的 iata 代码
    dest_airport_e_name							    , -- 目的机场英文
    dest_airport_c_name							    , -- 目的机场中文
    dest_lng											, -- 目的地坐标经度
    dest_lat											, -- 目的地坐标纬度
    flight_photo										, -- 飞机的图片
    flight_departure_time						        , -- 航班起飞时间
    expected_landing_time						        , -- 预计降落时间
    to_destination_distance					        , -- 目的地距离
    estimated_landing_duration			            , -- 预计还要多久着陆
    airlines_icao										, -- 航空公司的icao代码
    airlines_e_name									, -- 航空公司英文
    airlines_c_name									, -- 航空公司中文
    country_code										, -- 飞机所属国家代码
    country_name										, -- 国家中文
    data_source										, -- 数据来源的系统
    source											, -- 来源
    position_country_code2					        , -- 位置所在国家简称
    position_country_name						        , -- 位置所在国家名称
    friend_foe										, -- 敌我
    sea_id											, -- 海域id
    sea_name											, -- 海域名字
    h3_code											, -- 位置h3编码
    extend_info										, -- 扩展信息 json 串
    from_unixtime(unix_timestamp()) as update_time	-- 更新时间
from tmp_dws_aircraft_combine_list_rt_05;


insert into dws_aircraft_combine_status_rt
select
    flight_id											, -- 飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码
    src_code											, -- 来源网站标识 1. radarbox 2. adsbexchange
    acquire_time										, -- 采集时间
    icao_code											, -- 24位 icao编码
    registration										, -- 注册号
    flight_no											, -- 航班号
    callsign											, -- 呼号
    flight_type										, -- 飞机型号
    is_military										, -- 是否军用飞机 0 非军用 1 军用
    pk_type											, -- flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex
    src_pk											, -- 源网站主键
    flight_category									, -- 飞机类型
    flight_category_name						        , -- 飞机类型名称
    lng												, -- 经度
    lat												, -- 纬度
    speed												, -- 飞行当时的速度（单位：节）
    speed_km											, -- 速度单位 km/h
    altitude_baro										, -- 气压高度 海拔 航班当前高度，单位为（ft）
    altitude_baro_m									, -- 气压高度 海拔 单位米
    altitude_geom										, -- 海拔高度 海拔 航班当前高度，单位为（ft）
    altitude_geom_m									, -- 海拔高度 海拔 单位米
    heading											, -- 方向  正北为0
    squawk_code										, -- 当前应答机代码
    flight_status										, -- 飞机状态： 已启程
    special											, -- 是否有特殊情况
    origin_airport3_code						        , -- 起飞机场的iata代码
    origin_airport_e_name						        , -- 来源机场英文
    origin_airport_c_name						        , -- 来源机场中文
    origin_lng										, -- 来源机场经度
    origin_lat										, -- 来源机场纬度
    dest_airport3_code							    , -- 目标机场的 iata 代码
    dest_airport_e_name							    , -- 目的机场英文
    dest_airport_c_name							    , -- 目的机场中文
    dest_lng											, -- 目的地坐标经度
    dest_lat											, -- 目的地坐标纬度
    flight_photo										, -- 飞机的图片
    flight_departure_time						        , -- 航班起飞时间
    expected_landing_time						        , -- 预计降落时间
    to_destination_distance					        , -- 目的地距离
    estimated_landing_duration			            , -- 预计还要多久着陆
    airlines_icao										, -- 航空公司的icao代码
    airlines_e_name									, -- 航空公司英文
    airlines_c_name									, -- 航空公司中文
    country_code										, -- 飞机所属国家代码
    country_name										, -- 国家中文
    data_source										, -- 数据来源的系统
    source											, -- 来源
    position_country_code2					        , -- 位置所在国家简称
    position_country_name						        , -- 位置所在国家名称
    friend_foe										, -- 敌我
    sea_id											, -- 海域id
    sea_name											, -- 海域名字
    h3_code											, -- 位置h3编码
    extend_info										, -- 扩展信息 json 串
    from_unixtime(unix_timestamp()) as update_time	-- 更新时间
from tmp_dws_aircraft_combine_list_rt_05;

end;



