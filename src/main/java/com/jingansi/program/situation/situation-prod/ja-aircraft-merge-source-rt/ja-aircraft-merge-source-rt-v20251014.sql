--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/07/30 16:48:50
-- description: 新增航班号对应表
-- version: ja-aircraft-merge-source-rt-v20251014
--********************************************************************--

set 'pipeline.name' = 'ja-aircraft-merge-source-rt';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '300000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'parallelism.default' = '10';
SET 'execution.checkpointing.interval' = '300000';
SET 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-aircraft-merge-source-rt';

set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';

 -----------------------

 -- 数据结构

 -----------------------

-- 来源kafka的源数据
create table aircraft_source(
                                id                         string, -- id
                                srcCode                    bigint, --网站标识
                                acquireTime                string, -- 采集事件年月日时分秒
                                icaoCode                   string, -- icao
                                registration               string, -- 注册号
                                flightNo                   string, -- 航班号
                                callsign                   string, -- 呼号
                                flightType                 string, -- 飞机型号
                                isMilitary                 bigint, -- 是否军用
                                pkType                     string, -- flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex
                                srcPk                      string, -- 源网站主键
                                flightCategory             string, -- 飞机类别
                                flightCategoryName         string, -- 飞机类别名称
                                lng                        double, -- 经度
                                lat                        double, -- 纬度
                                speed                      double, -- 速度节
                                speedKm                    double, -- 速度 km
                                altitudeBaro               double, -- 气压高度 海拔 航班当前高度，单位为（ft）
                                altitudeBaroM              double, -- 气压高度 海拔 单位米
                                altitudeGeom               double, -- 海拔高度 海拔 航班当前高度，单位为（ft）
                                altitudeGeomM              double, -- 海拔高度 海拔 单位米
                                heading                    double, -- 方向
                                squawkCode                 string, -- 应答器代码
                                flightStatus               bigint, -- 飞机状态
                                special                    int,    -- 是否有特殊情况
                                originAirport3Code         string, -- 起飞机场的iata代码
                                originAirportEName         string, -- 来源机场英文
                                originAirportCName         string, -- 来源机场中文
                                originLng                  double, -- 来源机场经度
                                originLat                  double, -- 来源机场纬度
                                destAirport3Code           string, -- 目的机场3字代码
                                destAirportEName           string, -- 目的机场英文
                                destAirportCName           string, -- 目的机场中文
                                destLng                    double, -- 目的地坐标经度
                                destLat                    double, -- 目的地坐标纬度
                                flightPhoto                string, -- 飞机的图片
                                flightDepartureTime        string, -- 飞机起飞时间
                                expectedLandingTime        string, -- 预计降落时间
                                toDestinationDistance      double, -- 距离目的地距离
                                estimatedLandingDuration   string, -- 预计还要多久着陆
                                airlinesIcao               string, -- 航空公司icao
                                airlinesEName              string, -- 航空公司英文
                                airlinesCName              string, -- 航空公司中文
                                countryCode                string, -- 国家代码
                                countryName                string, -- 国家名称
                                dataSource                 string, -- 数据来源的系统
                                source                     string, -- 来源
                                positionCountryCode2       string, -- 所处国家
                                positionCountryName        string, -- 所处国家名称
                                friendFoe                  string, -- 敌我代码
                                seaId                      string, -- 海域id
                                seaName                    string, -- 海域名称
                                h3Code                     string, -- 位置h3编码
                                extendInfo                 string, -- 扩展信息 json 串
                                targetType                 string, -- 实体类型 固定值 AIRCRAFT
                                updateTime                 string  -- flink处理时间
) with (
      'connector' = 'kafka',
      'topic' = 'aircraft_source',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'aircraft_source_idc1-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1737986719000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 飞机各个网站数据融合表
create table dwd_bhv_aircraft_combine_rt (
                                             flight_id						  string 	comment '飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码',
                                             acquire_time					  string 	comment '采集时间',
                                             src_code						  bigint 	comment '来源网站标识 1. radarbox 2. adsbexchange',
                                             icao_code						  string 	comment '24位 icao编码',
                                             registration					  string 	comment '注册号',
                                             flight_no						  string 	comment '航班号',
                                             callsign						  string 	comment '呼号',
                                             flight_type					  string 	comment '飞机型号',
                                             is_military					  bigint 	comment '是否军用飞机 0 非军用 1 军用',
                                             pk_type						  string 	comment 'flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex',
                                             src_pk						  string 	comment '源网站主键',
                                             flight_category				  string 	comment '飞机类型',
                                             flight_category_name			  string 	comment '飞机类型名称',
                                             longitude						  double 	comment '经度',
                                             latitude						  double 	comment '纬度',
                                             speed							  double 	comment '飞行当时的速度（单位：节）',
                                             speed_km						  double 	comment '速度单位 km/h',
                                             altitude_baro					  double 	comment '气压高度 海拔 航班当前高度，单位为（ft）',
                                             altitude_baro_m				  double 	comment '气压高度 海拔 单位米',
                                             altitude_geom					  double 	comment '海拔高度 海拔 航班当前高度，单位为（ft）',
                                             altitude_geom_m				  double 	comment '海拔高度 海拔 单位米',
                                             heading						  double 	comment '方向  正北为0 ',
                                             squawk_code					  string 	comment '当前应答机代码',
                                             flight_status					  bigint 	comment '飞机状态： 已启程',
                                             special						  int 	    comment '是否有特殊情况',
                                             origin_airport3_code			  string 	comment '起飞机场的iata代码',
                                             origin_airport_e_name			  string 	comment '来源机场英文',
                                             origin_airport_c_name			  string 	comment '来源机场中文',
                                             origin_lng					  double 	comment '来源机场经度',
                                             origin_lat					  double 	comment '来源机场纬度',
                                             dest_airport3_code			  string 	comment '目标机场的 iata 代码',
                                             dest_airport_e_name			  string 	comment '目的机场英文',
                                             dest_airport_c_name			  string 	comment '目的机场中文',
                                             dest_lng						  double 	comment '目的地坐标经度',
                                             dest_lat						  double 	comment '目的地坐标纬度',
                                             flight_photo					  string 	comment '飞机的图片',
                                             flight_departure_time			  string    comment '航班起飞时间',
                                             expected_landing_time			  string    comment '预计降落时间',
                                             to_destination_distance		  double    comment '目的地距离',
                                             estimated_landing_duration	  string    comment '预计还要多久着陆',
                                             airlines_icao					  string 	comment '航空公司的icao代码',
                                             airlines_e_name				  string 	comment '航空公司英文',
                                             airlines_c_name				  string 	comment '航空公司中文',
                                             country_code					  string 	comment '飞机所属国家代码',
                                             country_name					  string 	comment '国家中文',
                                             data_source					  string 	comment '数据来源的系统',
                                             source						  string 	comment '来源',
                                             position_country_code2		  string 	comment '位置所在国家简称',
                                             position_country_name			  string 	comment '位置所在国家名称',
                                             friend_foe					  string 	comment '敌我',
                                             filter_col                      string    comment '动态筛选字段拼接',
                                             sea_id						  string 	comment '海域id',
                                             sea_name						  string 	comment '海域名字',
                                             h3_code						  string 	comment '位置h3编码',
                                             extend_info					  string    comment '扩展信息 json 串',
                                             update_time					  string    comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dwd_bhv_aircraft_combine_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='1s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- 飞机各个网站数据融合状态表
create table dws_bhv_aircraft_last_location_rt (
                                                   flight_id						  string 	comment '飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码',
                                                   acquire_time					  string 	comment '采集时间',
                                                   src_code						  bigint 	comment '来源网站标识 1. radarbox 2. adsbexchange',
                                                   icao_code						  string 	comment '24位 icao编码',
                                                   registration					  string 	comment '注册号',
                                                   flight_no						  string 	comment '航班号',
                                                   callsign						  string 	comment '呼号',
                                                   flight_type					  string 	comment '飞机型号',
                                                   is_military					  bigint 	comment '是否军用飞机 0 非军用 1 军用',
                                                   pk_type						  string 	comment 'flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex',
                                                   src_pk						  string 	comment '源网站主键',
                                                   flight_category				  string 	comment '飞机类型',
                                                   flight_category_name			  string 	comment '飞机类型名称',
                                                   longitude						  double 	comment '经度',
                                                   latitude						  double 	comment '纬度',
                                                   speed							  double 	comment '飞行当时的速度（单位：节）',
                                                   speed_km						  double 	comment '速度单位 km/h',
                                                   altitude_baro					  double 	comment '气压高度 海拔 航班当前高度，单位为（ft）',
                                                   altitude_baro_m				  double 	comment '气压高度 海拔 单位米',
                                                   altitude_geom					  double 	comment '海拔高度 海拔 航班当前高度，单位为（ft）',
                                                   altitude_geom_m				  double 	comment '海拔高度 海拔 单位米',
                                                   heading						  double 	comment '方向  正北为0 ',
                                                   squawk_code					  string 	comment '当前应答机代码',
                                                   flight_status					  bigint 	comment '飞机状态： 已启程',
                                                   special						  int 	    comment '是否有特殊情况',
                                                   origin_airport3_code			  string 	comment '起飞机场的iata代码',
                                                   origin_airport_e_name			  string 	comment '来源机场英文',
                                                   origin_airport_c_name			  string 	comment '来源机场中文',
                                                   origin_lng					  double 	comment '来源机场经度',
                                                   origin_lat					  double 	comment '来源机场纬度',
                                                   dest_airport3_code			  string 	comment '目标机场的 iata 代码',
                                                   dest_airport_e_name			  string 	comment '目的机场英文',
                                                   dest_airport_c_name			  string 	comment '目的机场中文',
                                                   dest_lng						  double 	comment '目的地坐标经度',
                                                   dest_lat						  double 	comment '目的地坐标纬度',
                                                   flight_photo					  string 	comment '飞机的图片',
                                                   flight_departure_time			  string    comment '航班起飞时间',
                                                   expected_landing_time			  string    comment '预计降落时间',
                                                   to_destination_distance		  double    comment '目的地距离',
                                                   estimated_landing_duration	  string    comment '预计还要多久着陆',
                                                   airlines_icao					  string 	comment '航空公司的icao代码',
                                                   airlines_e_name				  string 	comment '航空公司英文',
                                                   airlines_c_name				  string 	comment '航空公司中文',
                                                   country_code					  string 	comment '飞机所属国家代码',
                                                   country_name					  string 	comment '国家中文',
                                                   data_source					  string 	comment '数据来源的系统',
                                                   source						  string 	comment '来源',
                                                   position_country_code2		  string 	comment '位置所在国家简称',
                                                   position_country_name			  string 	comment '位置所在国家名称',
                                                   friend_foe					  string 	comment '敌我',
                                                   filter_col                      string    comment '动态筛选字段拼接',
                                                   sea_id						  string 	comment '海域id',
                                                   sea_name						  string 	comment '海域名字',
                                                   h3_code						  string 	comment '位置h3编码',
                                                   extend_info					  string    comment '扩展信息 json 串',
                                                   update_time					  string    comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_bhv_aircraft_last_location_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='1s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 飞机实体表（Source：doris）
create table dws_et_aircraft_info_source (
                                             flight_id           string        comment '飞机id',
                                             primary key (flight_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_aircraft_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );



-- 飞机实体表（Source：doris）
create table dws_et_aircraft_info (
                                      flight_id           string,
                                      acquire_time        string,
                                      src_code            bigint,
                                      icao_code           string,
                                      pk_type             string,
                                      src_pk              string,
                                      registration        string,
                                      flight_type         string,
                                      category_code       string,
                                      category_name       string,
                                      country_code        string,
                                      country_name        string,
                                      airlines_icao       string,
                                      airlines_e_name     string,
                                      airlines_c_name     string,
                                      is_military         int,
                                      friend_foe          string,
                                      search_content      string,
                                      update_time         string
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_et_aircraft_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='30000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 飞机各个网站数据融合表 - 写给XT的老系统
create table dws_aircraft_combine_list_rt (
                                              flight_id						  string 	comment '飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码',
                                              acquire_time					  string 	comment '采集时间',
                                              src_code						  bigint 	comment '来源网站标识 1. radarbox 2. adsbexchange',
                                              icao_code						  string 	comment '24位 icao编码',
                                              registration					  string 	comment '注册号',
                                              flight_no						  string 	comment '航班号',
                                              callsign						  string 	comment '呼号',
                                              flight_type					  string 	comment '飞机型号',
                                              is_military					  bigint 	comment '是否军用飞机 0 非军用 1 军用',
                                              pk_type						  string 	comment 'flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex',
                                              src_pk						  string 	comment '源网站主键',
                                              flight_category				  string 	comment '飞机类型',
                                              flight_category_name			  string 	comment '飞机类型名称',
                                              lng							  double 	comment '经度',
                                              lat							  double 	comment '纬度',
                                              speed							  double 	comment '飞行当时的速度（单位：节）',
                                              speed_km						  double 	comment '速度单位 km/h',
                                              altitude_baro					  double 	comment '气压高度 海拔 航班当前高度，单位为（ft）',
                                              altitude_baro_m				  double 	comment '气压高度 海拔 单位米',
                                              altitude_geom					  double 	comment '海拔高度 海拔 航班当前高度，单位为（ft）',
                                              altitude_geom_m				  double 	comment '海拔高度 海拔 单位米',
                                              heading						  double 	comment '方向  正北为0 ',
                                              squawk_code					  string 	comment '当前应答机代码',
                                              flight_status					  string 	comment '飞机状态： 已启程',
                                              special						  int 	    comment '是否有特殊情况',
                                              origin_airport3_code			  string 	comment '起飞机场的iata代码',
                                              origin_airport_e_name			  string 	comment '来源机场英文',
                                              origin_airport_c_name			  string 	comment '来源机场中文',
                                              origin_lng					  double 	comment '来源机场经度',
                                              origin_lat					  double 	comment '来源机场纬度',
                                              dest_airport3_code			  string 	comment '目标机场的 iata 代码',
                                              dest_airport_e_name			  string 	comment '目的机场英文',
                                              dest_airport_c_name			  string 	comment '目的机场中文',
                                              dest_lng						  double 	comment '目的地坐标经度',
                                              dest_lat						  double 	comment '目的地坐标纬度',
                                              flight_photo					  string 	comment '飞机的图片',
                                              flight_departure_time			  string    comment '航班起飞时间',
                                              expected_landing_time			  string    comment '预计降落时间',
                                              to_destination_distance		  double    comment '目的地距离',
                                              estimated_landing_duration	  string    comment '预计还要多久着陆',
                                              airlines_icao					  string 	comment '航空公司的icao代码',
                                              airlines_e_name				  string 	comment '航空公司英文',
                                              airlines_c_name				  string 	comment '航空公司中文',
                                              country_code					  string 	comment '飞机所属国家代码',
                                              country_name					  string 	comment '国家中文',
                                              data_source					  string 	comment '数据来源的系统',
                                              source						  string 	comment '来源',
                                              position_country_code2		  string 	comment '位置所在国家简称',
                                              position_country_name			  string 	comment '位置所在国家名称',
                                              friend_foe					  string 	comment '敌我',
                                              sea_id						  string 	comment '海域id',
                                              sea_name						  string 	comment '海域名字',
                                              h3_code						  string 	comment '位置h3编码',
                                              extend_info					  string    comment '扩展信息 json 串',
                                              update_time					  string    comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_aircraft_combine_list_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='5s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- 飞机各个网站数据融合状态表 - 写给XT的老系统
create table dws_aircraft_combine_status_rt (
                                                flight_id						  string 	comment '飞机标识字段  1. 24位 icao编码 2. 来源站的标识如 a. radarbox flight_trace_id  b. adsbexchange ～开头的编码',
                                                acquire_time					  string 	comment '采集时间',
                                                src_code						  bigint 	comment '来源网站标识 1. radarbox 2. adsbexchange',
                                                icao_code						  string 	comment '24位 icao编码',
                                                registration					  string 	comment '注册号',
                                                flight_no						  string 	comment '航班号',
                                                callsign						  string 	comment '呼号',
                                                flight_type					  string 	comment '飞机型号',
                                                is_military					  bigint 	comment '是否军用飞机 0 非军用 1 军用',
                                                pk_type						  string 	comment 'flight_id 主键的类型 hex： icao hex 24位编码 trace_id：radarbox 的追踪id non_icao: adsbexchange 不是真正的 hex',
                                                src_pk						  string 	comment '源网站主键',
                                                flight_category				  string 	comment '飞机类型',
                                                flight_category_name			  string 	comment '飞机类型名称',
                                                lng							  double 	comment '经度',
                                                lat							  double 	comment '纬度',
                                                speed							  double 	comment '飞行当时的速度（单位：节）',
                                                speed_km						  double 	comment '速度单位 km/h',
                                                altitude_baro					  double 	comment '气压高度 海拔 航班当前高度，单位为（ft）',
                                                altitude_baro_m				  double 	comment '气压高度 海拔 单位米',
                                                altitude_geom					  double 	comment '海拔高度 海拔 航班当前高度，单位为（ft）',
                                                altitude_geom_m				  double 	comment '海拔高度 海拔 单位米',
                                                heading						  double 	comment '方向  正北为0 ',
                                                squawk_code					  string 	comment '当前应答机代码',
                                                flight_status					  string 	comment '飞机状态： 已启程',
                                                special						  int 	    comment '是否有特殊情况',
                                                origin_airport3_code			  string 	comment '起飞机场的iata代码',
                                                origin_airport_e_name			  string 	comment '来源机场英文',
                                                origin_airport_c_name			  string 	comment '来源机场中文',
                                                origin_lng					  double 	comment '来源机场经度',
                                                origin_lat					  double 	comment '来源机场纬度',
                                                dest_airport3_code			  string 	comment '目标机场的 iata 代码',
                                                dest_airport_e_name			  string 	comment '目的机场英文',
                                                dest_airport_c_name			  string 	comment '目的机场中文',
                                                dest_lng						  double 	comment '目的地坐标经度',
                                                dest_lat						  double 	comment '目的地坐标纬度',
                                                flight_photo					  string 	comment '飞机的图片',
                                                flight_departure_time			  string    comment '航班起飞时间',
                                                expected_landing_time			  string    comment '预计降落时间',
                                                to_destination_distance		  double    comment '目的地距离',
                                                estimated_landing_duration	  string    comment '预计还要多久着陆',
                                                airlines_icao					  string 	comment '航空公司的icao代码',
                                                airlines_e_name				  string 	comment '航空公司英文',
                                                airlines_c_name				  string 	comment '航空公司中文',
                                                country_code					  string 	comment '飞机所属国家代码',
                                                country_name					  string 	comment '国家中文',
                                                data_source					  string 	comment '数据来源的系统',
                                                source						  string 	comment '来源',
                                                position_country_code2		  string 	comment '位置所在国家简称',
                                                position_country_name			  string 	comment '位置所在国家名称',
                                                friend_foe					  string 	comment '敌我',
                                                sea_id						  string 	comment '海域id',
                                                sea_name						  string 	comment '海域名字',
                                                h3_code						  string 	comment '位置h3编码',
                                                extend_info					  string    comment '扩展信息 json 串',
                                                update_time					  string    comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_aircraft_combine_status_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='5s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );

-- 航班号对应关系表
create table dim_rel_flightaware_no (
                                        flight_no             string  comment '飞机航班号',
                                        flight_id             string  comment '飞机id-icaoCode',
                                        acquire_time          string  comment '数据时间',
                                        update_time           string  comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dim_rel_flightaware_no',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='5s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-----------------------

-- 数据处理

-----------------------

create view aircraft_merge_temp01 as
select
    if(id='ABC123' and lng between 91 and 124 and lat between 15 and 46,'ABC123-JH',id)  as flight_id, -- 上海警航的飞机单独处理
    acquireTime                as acquire_time,
    srcCode                    as src_code,
    icaoCode                   as icao_code,
    registration,
    flightNo                   as flight_no,
    callsign,
    flightType                 as flight_type,
    isMilitary                 as is_military,
    pkType                     as pk_type,
    srcPk                      as src_pk,
    flightCategory             as flight_category,
    flightCategoryName         as flight_category_name,
    lng                        as longitude,
    lat                        as latitude,
    speed,
    speedKm                    as speed_km,
    altitudeBaro               as altitude_baro,
    altitudeBaroM              as altitude_baro_m,
    altitudeGeom               as altitude_geom,
    altitudeGeomM              as altitude_geom_m,
    heading,
    squawkCode                 as squawk_code,
    flightStatus               as flight_status,
    special,
    originAirport3Code         as origin_airport3_code,
    originAirportEName         as origin_airport_e_name,
    originAirportCName         as origin_airport_c_name,
    originLng                  as origin_lng,
    originLat                  as origin_lat,
    destAirport3Code           as dest_airport3_code,
    destAirportEName           as dest_airport_e_name,
    destAirportCName           as dest_airport_c_name,
    destLng                    as dest_lng,
    destLat                    as dest_lat,
    flightPhoto                as flight_photo,
    flightDepartureTime        as flight_departure_time,
    expectedLandingTime        as expected_landing_time,
    toDestinationDistance      as to_destination_distance,
    estimatedLandingDuration   as estimated_landing_duration,
    airlinesIcao               as airlines_icao,
    airlinesEName              as airlines_e_name,
    airlinesCName              as airlines_c_name,
    countryCode                as country_code,
    countryName                as country_name,
    dataSource                 as data_source,
    source,
    positionCountryCode2       as position_country_code2,
    positionCountryName        as position_country_name,
    friendFoe                  as friend_foe,
    concat(
            ifnull(flightNo,''),'¥',
            ifnull(cast(srcCode as varchar),''),'¥',
            ifnull(cast(lng as varchar),''),'¥',
            ifnull(cast(lat as varchar),''),'¥',
            ifnull(cast(speedKm as varchar),''),'¥',
            ifnull(cast(altitudeBaroM as varchar),''),'¥',
            ifnull(cast(heading as varchar),''),'¥',
            ifnull(squawkCode,''),'¥',
            ifnull(cast(flightStatus as varchar),''),'¥',
            ifnull(originAirport3Code,''),'¥',
            ifnull(originAirportEName,''),'¥',
            ifnull(originAirportCName,''),'¥',
            ifnull(destAirport3Code,''),'¥',
            ifnull(destAirportEName,''),'¥',
            ifnull(destAirportCName,''),'¥',
            ifnull(flightDepartureTime,''),'¥',
            ifnull(expectedLandingTime,''),'¥',
            ifnull(positionCountryCode2,''),'¥',
            ifnull(seaId,''),'¥',
            ifnull(dataSource,''),'¥',
            ifnull(source,'')
        ) as filter_col,
    seaId                      as sea_id,
    seaName                    as sea_name,
    h3Code                     as h3_code,
    extendInfo                 as extend_info,
    PROCTIME()                 as proctime
from aircraft_source;


create view aircraft_merge_temp02 as
select
    *
from (
         select
             t1.flight_id,
             acquire_time,
             src_code,
             icao_code,
             pk_type,
             src_pk,
             registration,
             flight_type,
             flight_category as category_code,
             flight_category_name as category_name,
             country_code,
             country_name,
             airlines_icao,
             airlines_e_name,
             airlines_c_name,
             cast(is_military as int) as is_military,
             friend_foe,
             -- concat(
             --   ifnull(icao_code,''),' ',
             --   ifnull(registration,''),' ',
             --   ifnull(flight_type,''),' '
             -- ) as search_content,
             count(*) over (partition by t1.flight_id order by proctime) as cnt
      -- from_unixtime(unix_timestamp()) as update_time
         from aircraft_merge_temp01 as t1
                  left join dws_et_aircraft_info_source FOR SYSTEM_TIME AS OF t1.proctime as t2
                            on t1.flight_id = t2.flight_id
         where t2.flight_id is null
     ) a
where cnt <= 10;


-----------------------

-- 数据写入

-----------------------


begin statement set;

-- 航班对应关系表入库
insert into dim_rel_flightaware_no
select
    flight_no,
    flight_id,
    acquire_time,
    from_unixtime(unix_timestamp()) as update_time
from aircraft_merge_temp01
where src_code <=3
  and flight_no is not null
  and flight_no <> ''
  and flight_id is not null;



-- 飞机轨迹表
insert into dwd_bhv_aircraft_combine_rt
select
    flight_id,
    acquire_time,
    src_code,
    icao_code,
    registration,
    flight_no,
    callsign,
    flight_type,
    is_military,
    pk_type,
    src_pk,
    flight_category,
    flight_category_name,
    longitude,
    latitude,
    speed,
    speed_km,
    altitude_baro,
    altitude_baro_m,
    altitude_geom,
    altitude_geom_m,
    heading,
    squawk_code,
    flight_status,
    special,
    origin_airport3_code,
    origin_airport_e_name,
    origin_airport_c_name,
    origin_lng,
    origin_lat,
    dest_airport3_code,
    dest_airport_e_name,
    dest_airport_c_name,
    dest_lng,
    dest_lat,
    flight_photo,
    flight_departure_time,
    expected_landing_time,
    to_destination_distance,
    estimated_landing_duration,
    airlines_icao,
    airlines_e_name,
    airlines_c_name,
    country_code,
    country_name,
    data_source,
    source,
    position_country_code2,
    position_country_name,
    friend_foe,
    filter_col,
    sea_id,
    sea_name,
    h3_code,
    extend_info,
    from_unixtime(unix_timestamp()) as update_time
from aircraft_merge_temp01;


-- 飞机最后位置表
insert into dws_bhv_aircraft_last_location_rt
select
    flight_id,
    acquire_time,
    src_code,
    icao_code,
    registration,
    flight_no,
    callsign,
    flight_type,
    is_military,
    pk_type,
    src_pk,
    flight_category,
    flight_category_name,
    longitude,
    latitude,
    speed,
    speed_km,
    altitude_baro,
    altitude_baro_m,
    altitude_geom,
    altitude_geom_m,
    heading,
    squawk_code,
    flight_status,
    special,
    origin_airport3_code,
    origin_airport_e_name,
    origin_airport_c_name,
    origin_lng,
    origin_lat,
    dest_airport3_code,
    dest_airport_e_name,
    dest_airport_c_name,
    dest_lng,
    dest_lat,
    flight_photo,
    flight_departure_time,
    expected_landing_time,
    to_destination_distance,
    estimated_landing_duration,
    airlines_icao,
    airlines_e_name,
    airlines_c_name,
    country_code,
    country_name,
    data_source,
    source,
    position_country_code2,
    position_country_name,
    friend_foe,
    filter_col,
    sea_id,
    sea_name,
    h3_code,
    extend_info,
    from_unixtime(unix_timestamp()) as update_time
from aircraft_merge_temp01;


-- 飞机实体表
insert into dws_et_aircraft_info
select
    flight_id,
    acquire_time,
    src_code,
    icao_code,
    pk_type,
    src_pk,
    registration,
    flight_type,
    category_code,
    category_name,
    country_code,
    country_name,
    airlines_icao,
    airlines_e_name,
    airlines_c_name,
    is_military,
    friend_foe,
    concat(
            ifnull(icao_code,''),' ',
            ifnull(registration,''),' ',
            ifnull(flight_type,''),' '
        ) as search_content,
    from_unixtime(unix_timestamp()) as update_time
from aircraft_merge_temp02;


-- 原本表结构，同步数据给信通
insert into dws_aircraft_combine_list_rt
select
    flight_id,
    acquire_time,
    src_code,
    icao_code,
    registration,
    flight_no,
    callsign,
    flight_type,
    is_military,
    pk_type,
    src_pk,
    flight_category,
    flight_category_name,
    longitude as lng,
    latitude as lat,
    speed,
    speed_km,
    altitude_baro,
    altitude_baro_m,
    altitude_geom,
    altitude_geom_m,
    heading,
    squawk_code,
    cast(null as varchar) as flight_status,
    special,
    origin_airport3_code,
    origin_airport_e_name,
    origin_airport_c_name,
    origin_lng,
    origin_lat,
    dest_airport3_code,
    dest_airport_e_name,
    dest_airport_c_name,
    dest_lng,
    dest_lat,
    flight_photo,
    flight_departure_time,
    expected_landing_time,
    to_destination_distance,
    estimated_landing_duration,
    airlines_icao,
    airlines_e_name,
    airlines_c_name,
    country_code,
    country_name,
    data_source,
    source,
    position_country_code2,
    position_country_name,
    friend_foe,
    sea_id,
    sea_name,
    h3_code,
    extend_info,
    from_unixtime(unix_timestamp()) as update_time
from aircraft_merge_temp01;


-- 原本表结构，同步数据给信通
insert into dws_aircraft_combine_status_rt
select
    flight_id,
    acquire_time,
    src_code,
    icao_code,
    registration,
    flight_no,
    callsign,
    flight_type,
    is_military,
    pk_type,
    src_pk,
    flight_category,
    flight_category_name,
    longitude as lng,
    latitude as lat,
    speed,
    speed_km,
    altitude_baro,
    altitude_baro_m,
    altitude_geom,
    altitude_geom_m,
    heading,
    squawk_code,
    cast(null as varchar) as flight_status,
    special,
    origin_airport3_code,
    origin_airport_e_name,
    origin_airport_c_name,
    origin_lng,
    origin_lat,
    dest_airport3_code,
    dest_airport_e_name,
    dest_airport_c_name,
    dest_lng,
    dest_lat,
    flight_photo,
    flight_departure_time,
    expected_landing_time,
    to_destination_distance,
    estimated_landing_duration,
    airlines_icao,
    airlines_e_name,
    airlines_c_name,
    country_code,
    country_name,
    data_source,
    source,
    position_country_code2,
    position_country_name,
    friend_foe,
    sea_id,
    sea_name,
    h3_code,
    extend_info,
    from_unixtime(unix_timestamp()) as update_time
from aircraft_merge_temp01;



end;



