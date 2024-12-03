--********************************************************************--
-- author:      yibi@jingan-inc.com
-- create time: 2023/5/25 15:18:32
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ja-radarbox-airport-rt';

set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';


-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '100000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-radarbox-airport-rt';



 -----------------------

 -- 数据结构

 -----------------------

-- radarbox上的机场数据列表（Source：Kafka）
drop table if exists radarbox_airport_list_kafka;
create table radarbox_airport_list_kafka(
                                            airportFlag         string			comment '机场名称英文全称-带有代号的',
                                            airport			    string			comment '机场名称英文全称-去除代号',
                                            icao			    string			comment '机场代号-从URL中截取',
                                            country			    string			comment '机场所属国家名称英文',
                                            cnt					string			comment '机场所属国家总共机场个数',
                                            detailUrl 			string			comment '机场详情请求的地址',
                                            updateTime			string			comment '数据入库时间'
) with (
      'connector' = 'kafka',
      'topic' = 'radarbox_airport_list',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'radarbox-airport-list-rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1685000409000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 机场数据详情（Source：Kafka）
drop table if exists radarbox_airport_detail_info_kafka;
create table radarbox_airport_detail_info_kafka(
                                                   name      				    string			comment '机场名',
                                                   iata						string			comment '机场的三字代码',
                                                   icao						string		    comment '机场的ICAO',
                                                   apttype						string			comment '机场类型',
                                                   tzns						string			comment '时区缩写',
                                                   tznl						string			comment '时区名称',
                                                   country						string			comment '国家',
                                                   country_code				string			comment '国家代码',
                                                   `state`						string			comment '州/省份',
                                                   city						string			comment '城市',
                                                   fullName					string			comment '机场所在地的全名',
                                                   iconweather					string			comment '天气状况图标',
                                                   cbase						string			comment '云底高度',
                                                   fltcat						string			comment '所允许的航班类别',
                                                   wts							string			comment '上次更新',
                                                   wt							bigint 			comment '天气时间戳',
                                                   elevation					double 			comment '机场高度',
                                                   tz							double 			comment '时区',
                                                   longitude					double 			comment '机场所在经度',
                                                   latitude					double 			comment '机场所在纬度',
                                                   dewp						double 			comment '露点',
                                                   qnh							double 			comment 'ALTIMETER SETTING',
                                                   wspd						double 			comment '风速',
                                                   wdir						double 			comment '风向',
                                                   temp						double 			comment '温度',
                                                   visib						double 			comment '可见度',
                                                   taf row(
                                                       raw_text				string          , --  TAF的原始文本信息
                                                       station_id              string          , --  机场的ICAO代码
                                                       issue_time              bigint          , --  TAF发布时间，时间戳
                                                       bulletin_time           bigint          , --  TAF公告时间，时间戳
                                                       valid_time_from         bigint          , --  TAF预报的有效起始时间，时间戳
                                                       valid_time_to           bigint          , --  TAF预报的有效截止时间，时间戳
                                                       latitude                double          , --  机场的纬度
                                                       longitude               double          , --  机场的经度
                                                       elevation_m             double          , --  机场的海拔高度，单位为米
                                                       forecast array <
                                                       row(
                                                       fcst_time_from			bigint			, -- 预报起始时间，时间戳
                                                       fcst_time_to			bigint			, -- 预报截止时间，时间戳
                                                       wind_dir_degrees		double			, -- 风向，单位为度
                                                       wind_speed_kt			double			, -- 风速，单位为节
                                                       visibility_statute_mi	double			, -- 可见度
                                                       wx_string				string			, -- 天气现象的描述
                                                       sky_condition array <
                                                       row(
                                                       sky_cover				string			, -- 云层状况
                                                       cloud_base_ft_agl		double			 -- 云层底部高度，单位为英尺
                                                       )
                                                       >
                                                       )
                                                       >
                                                       ),
                                                   nearest string ,
                                                   runways row (
                                                       landing  string , -- 着陆
                                                       takeoff  string
                                                       ),
                                                   proctime          as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'radarbox_airport_detail_info',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'radarbox-airport-detail-info-rt1',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );





-- 机场列表入库doris（Sink：Doris）
drop table if exists dwd_airport_list_info;
create table dwd_airport_list_info (
                                       icao			    string			comment '机场代号',
                                       airport_flag        string			comment '机场名称英文全称-带有代号的',
                                       airport			    string			comment '机场名称英文全称-去除代号',
                                       airport_name	    string			comment '机场名称中文',
                                       country			    string			comment '机场所属国家名称英文',
                                       cnt					string			comment '机场所属国家总共机场个数',
                                       detail_url 			string			comment '机场详情请求的地址',
                                       update_time			string			comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.27.95.211:30030',
      'table.identifier' = 'sa.dwd_airport_list_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='5000',
      'sink.batch.interval'='10s'
      );



--机场详情数据（Sink：Doris）
drop table if exists dws_airport_detail_info;
create table dws_airport_detail_info (
                                         icao						string		comment '机场的ICAO',
                                         `name`      					string		comment '机场名',
                                         iata						string		comment '机场的三字代码',
                                         apttype						string		comment '机场类型',
                                         apttype_name 				string 		comment '机场类型中文',
                                         tzns						string		comment '时区缩写',
                                         tznl						string		comment '时区名称',
                                         country						string		comment '国家',
                                         country_code				string		comment '国家代码',
                                         country_name                string      comment '国家中文',
                                         `state`						string		comment '州/省份',
                                         city						string		comment '城市',
                                         full_name					string		comment '所在地的全名',
                                         iconweather					string		comment '天气状况图标',
                                         iconweather_name			string		comment '天气状况中文',
                                         cbase						string		comment '云底高度',
                                         fltcat						string		comment '所允许的航班类别',
                                         fltcat_name 				string 		comment '所允许的航班类别中文',
                                         wts							string		comment '上次更新',
                                         wt							bigint 		comment '天气时间戳',
                                         elevation					double 		comment '机场高度',
                                         tz							double 		comment '时区',
                                         longitude					double 		comment '机场所在经度',
                                         latitude					double 		comment '机场所在纬度',
                                         dewp						double 		comment '露点',
                                         qnh							double 		comment 'ALTIMETER SETTING',
                                         wspd						double 		comment '风速',
                                         wdir						double 		comment '风向',
                                         temp						double 		comment '温度',
                                         visib						double 		comment '可见度',
                                         taf_raw_text				string      comment 'TAF的原始文本信息',
                                         taf_station_id              string      comment '机场的ICAO代码',
                                         taf_issue_time              bigint      comment 'TAF发布时间，时间戳',
                                         taf_bulletin_time           bigint      comment 'TAF公告时间，时间戳',
                                         taf_valid_time_from         bigint      comment 'TAF预报的有效起始时间，时间戳',
                                         taf_valid_time_to           bigint      comment 'TAF预报的有效截止时间，时间戳',
                                         taf_latitude                double      comment '机场的纬度',
                                         taf_longitude               double      comment '机场的经度',
                                         taf_elevation_m             double      comment '机场的海拔高度，单位为米',
                                         taf_fcst_time_from			bigint		comment '预报起始时间，时间戳',
                                         taf_fcst_time_to			bigint		comment '预报截止时间，时间戳',
                                         taf_wind_dir_degrees		double		comment '风向，单位为度',
                                         taf_wind_speed_kt			double		comment '风速，单位为节',
                                         taf_visibility_statute_mi	double		comment '可见度',
                                         taf_wx_string				string		comment '天气现象的描述',
                                         raf_sky_cover				string		comment '云层状况',
                                         raf_cloud_base_ft_agl		double		comment '云层底部高度，单位为英尺',
                                         nearest						string		comment '最近的机场信息',
                                         runways_landing				string		comment '机场跑道信息-降落',
                                         runways_takeoff				string		comment '机场跑道信息-起飞',
                                         update_time					string		comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.27.95.211:30030',
      'table.identifier' = 'sa.dws_airport_detail_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='5000',
      'sink.batch.interval'='10s'
      --  	'sink.properties.escape_delimiters' = 'false',
      -- 'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      -- 'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      -- 'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 国家数据匹配库（Source：doris）
drop table if exists dim_vessel_country_code_list;
create table dim_vessel_country_code_list (
                                              country      			string			comment '国家英文',
                                              flag_country_code	    string			comment '国家的编码',
                                              country_name			string			comment '国家的中文',
                                              belong_type             string          comment '所属类型',
                                              primary key (country) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_vessel_country_code_list',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '60s',
      'lookup.max-retries' = '1'
      );



-- 机场类型匹配库（Source：doris）
drop table if exists dim_airport_apttype_info;
create table dim_airport_apttype_info (
                                          apttype      			string			comment 'apttype字段',
                                          name 					string			comment 'apttype字段中文名称',
                                          primary key (apttype) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_airport_apttype_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '60s',
      'lookup.max-retries' = '1'
      );


-- 机场天气匹配库（Source：doris）
drop table if exists dim_airport_iconweather_info;
create table dim_airport_iconweather_info (
                                              iconweather      		string			comment 'iconweather字段',
                                              name 					string			comment 'iconweather字段中文名称',
                                              primary key (iconweather) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_airport_iconweather_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '60s',
      'lookup.max-retries' = '1'
      );



-- 机场所允许的航班类别匹配库（Source：doris）
drop table if exists dim_airport_fltcat_info;
create table dim_airport_fltcat_info (
                                         fltcat      				string			comment 'fltcat字段',
                                         name 						string			comment 'fltcat字段中文名称',
                                         primary key (fltcat) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_airport_fltcat_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '60s',
      'lookup.max-retries' = '1'
      );



-----------------------

-- 数据处理

-----------------------

--机场详情数据解析
drop table if exists tmp_airport_01;
create view tmp_airport_01 as
select
    t1.iata			                   , -- 机场的三字代码
    t1.name      		               , -- 机场名
    t1.icao			                   , -- 机场的ICAO
    t1.apttype			               , -- 机场类型
    t5.name as apttype_name            , -- 机场类型中文
    t1.tzns			                   , -- 时区缩写
    t1.tznl			                   , -- 时区名称
    t1.country			               , -- 国家
    t1.country_code	                   , -- 国家代码
    t2.country_name as country_name    , -- 国家名称
    t1.`state`			               , -- 州/省份
    t1.city			                   , -- 城市
    t1.fullName as full_name		   , -- 机场所在地的全名
    t1.iconweather		               , -- 天气状况图标
    t3.name as iconweather_name        , -- 天气状况中文
    t1.cbase			               , -- 云底高度
    t1.fltcat			               , -- 所允许的航班类别
    t4.name as fltcat_name             , -- 所允许的航班类别中文
    t1.wts				               , -- 上次更新
    t1.wt				               , -- 天气时间戳
    t1.elevation		               , -- 机场高度
    t1.tz				               , -- 时区
    t1.longitude		               , -- 机场所在经度
    t1.latitude		                   , -- 机场所在纬度
    t1.dewp			                   , -- 露点
    t1.qnh				               , -- ALTIMETER SETTING
    t1.wspd			                   , -- 风速
    t1.wdir			                   , -- 风向
    t1.temp			                   , -- 温度
    t1.visib			                   , -- 可见度
    t1.taf.raw_text		as taf_raw_text	   , --  TAF的原始文本信息
    t1.taf.station_id      as taf_station_id            , --  机场的ICAO代码
    t1.taf.issue_time      as taf_issue_time            , --  TAF发布时间，时间戳
    t1.taf.bulletin_time   as taf_bulletin_time         , --  TAF公告时间，时间戳
    t1.taf.valid_time_from as taf_valid_time_from       , --  TAF预报的有效起始时间，时间戳
    t1.taf.valid_time_to   as taf_valid_time_to         , --  TAF预报的有效截止时间，时间戳
    t1.taf.latitude        as taf_latitude              , --  机场的纬度
    t1.taf.longitude       as taf_longitude             , --  机场的经度
    t1.taf.elevation_m     as taf_elevation_m           , --  机场的海拔高度，单位为米
    t1.taf.forecast[1].fcst_time_from			   as taf_fcst_time_from             , -- 预报起始时间，时间戳
    t1.taf.forecast[1].fcst_time_to			       as taf_fcst_time_to               , -- 预报截止时间，时间戳
    t1.taf.forecast[1].wind_dir_degrees		       as taf_wind_dir_degrees           , -- 风向，单位为度
    t1.taf.forecast[1].wind_speed_kt			   as taf_wind_speed_kt              , -- 风速，单位为节
    t1.taf.forecast[1].visibility_statute_mi	   as taf_visibility_statute_mi      , -- 可见度
    t1.taf.forecast[1].wx_string				   as taf_wx_string                  , -- 天气现象的描述
    t1.taf.forecast[1].sky_condition[1].sky_cover  as taf_sky_cover                  , -- 云层状况
    t1.taf.forecast[1].sky_condition[1].cloud_base_ft_agl  as taf_cloud_base_ft_agl  , -- 云层底部高度，单位为英尺
    t1.nearest                                  , -- 附近的机场
    t1.runways.landing as runways_landing       , -- 着陆
    t1.runways.takeoff as runways_takeoff        -- 起飞
from radarbox_airport_detail_info_kafka as t1
         left join dim_vessel_country_code_list               -- 关联国家
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on UPPER(t1.country_code) = t2.flag_country_code
                       and ('FLEETMON' = t2.belong_type or 'RADARBOX' = t2.belong_type)

         left join dim_airport_iconweather_info               -- 关联天气
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.iconweather = t3.iconweather

         left join dim_airport_fltcat_info               -- 关联所允许的航班类别
    FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.fltcat = t4.fltcat

         left join dim_airport_apttype_info               -- 关联类型
    FOR SYSTEM_TIME AS OF t1.proctime as t5
                   on t1.apttype = t5.apttype;

-----------------------

-- 数据插入

-----------------------

begin statement set;

-- 机场列表数据入库
insert into dwd_airport_list_info
select
    icao			    ,
    airportFlag as airport_flag,
    airport,
    cast(null as varchar) as airport_name,
    country,
    cnt,
    detailUrl as detail_url,
    updateTime as update_time
from radarbox_airport_list_kafka
where updateTime is not null
  and icao is not null;


-- 机场详情数据入库
insert into dws_airport_detail_info
select
    icao			                , -- 机场的ICAO
    name      		                , -- 机场名
    iata			                , -- 机场的三字代码
    apttype			                , -- 机场类型
    apttype_name                    , -- 机场类型中文
    tzns			                , -- 时区缩写
    tznl			                , -- 时区名称
    country			                , -- 国家
    country_code	                , -- 国家代码
    country_name                    , --'国家中文'
    `state`			                , -- 州/省份
    city			                , -- 城市
    full_name		                , -- 机场所在地的全名
    iconweather		                , -- 天气状况图标
    iconweather_name				, -- '天气状况中文'
    cbase			                , -- 云底高度
    fltcat			                , -- 所允许的航班类别
    fltcat_name 					, --'所允许的航班类别中文'
    wts				                , -- 上次更新
    wt				                , -- 天气时间戳
    elevation		                , -- 机场高度
    tz				                , -- 时区
    longitude		                , -- 机场所在经度
    latitude		                , -- 机场所在纬度
    dewp			                , -- 露点
    qnh				                , -- ALTIMETER SETTING
    wspd			                , -- 风速
    wdir			                , -- 风向
    temp			                , -- 温度
    visib			                , -- 可见度
    taf_raw_text			        , --  TAF的原始文本信息
    taf_station_id                  , --  机场的ICAO代码
    taf_issue_time                  , --  TAF发布时间，时间戳
    taf_bulletin_time               , --  TAF公告时间，时间戳
    taf_valid_time_from             , --  TAF预报的有效起始时间，时间戳
    taf_valid_time_to               , --  TAF预报的有效截止时间，时间戳
    taf_latitude                    , --  机场的纬度
    taf_longitude                   , --  机场的经度
    taf_elevation_m                 , --  机场的海拔高度，单位为米
    taf_fcst_time_from			    , -- 预报起始时间，时间戳
    taf_fcst_time_to			    , -- 预报截止时间，时间戳
    taf_wind_dir_degrees		    , -- 风向，单位为度
    taf_wind_speed_kt			    , -- 风速，单位为节
    taf_visibility_statute_mi	    , -- 可见度
    taf_wx_string				    , -- 天气现象的描述
    taf_sky_cover                   , -- 云层状况
    taf_cloud_base_ft_agl           , -- 云层底部高度，单位为英尺
    nearest                         , -- 附近的机场
    runways_landing                 , -- 着陆
    runways_takeoff                 , -- 起飞
    from_unixtime(unix_timestamp()) as update_time
from tmp_airport_01
where icao is not null;

end;
