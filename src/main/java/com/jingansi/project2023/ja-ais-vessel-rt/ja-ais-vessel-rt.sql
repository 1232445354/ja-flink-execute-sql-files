--********************************************************************--
-- author:      write your name here
-- create time: 2023/5/14 14:27:58
-- description: ais的数据入库
--********************************************************************--
set 'pipeline.name' = 'ja-ais-vessel-list-rt';

set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '4';

set 'execution.checkpointing.interval' = '300000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-ais-vessel-list-rt-checkpoint';
-- 空闲分区不用等待
-- set 'table.exec.source.idle-timeout' = '3s';


-----------------------

 -- 数据结构

 -----------------------


-- 创建kafka全量AIS数据来源的表（Source：kafka）
drop table  if exists ais_fleetmon_collect_item_kafka;
create table ais_fleetmon_collect_item_kafka(
                                                vesselId             bigint       comment '船ID',
                                                `timestamp`           bigint       comment '时间戳',
                                                name                 string       comment '船名称',
                                                rateOfTurn           double       comment '转向率',
                                                orientation          double       comment '方向',
                                                masterImageId        bigint       comment '主图像ID',
                                                coordinates array <               -- 船只坐标
                                                    double
                                                    >,
                                                source               string       comment '数据来源简称',
                                                speed                double       comment '速度',
                                                vesselClass          string       comment '船类别',
                                                draught              double       comment '吃水深度',
                                                cnIso2               string       comment '',
                                                navStatus            bigint       comment '导航状态',
                                                dimensions array<
                                                    double
                                                    >,
                                                block_map_index      bigint      comment '图层层级',
                                                block_range_x        double      comment '区域块x',
                                                block_range_y        double      comment '区域块y',
                                                proctime          as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'ais_fleetmon_collect_item',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ais-fleetmon-collect-item-rt',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1685968831000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 创建映射doris的全量数据表(Sink:doris)
drop table  if exists ais_all_info_doris;
create table ais_all_info_doris(
                                   vessel_id      	 				    bigint		comment '船ID',
                                   acquire_timestamp_format			string      comment '时间戳格式化',
                                   acquire_timestamp					bigint		comment '时间戳',
                                   vessel_name							string      comment '船名称',
                                   rate_of_turn						double  	comment '转向率',
                                   orientation							double 		comment '方向',
                                   master_image_id						bigint		comment '主图像ID',
                                   lng 					            double 		comment '经度',
                                   lat 					            double 		comment '纬度',
                                   source								string 		comment	'来源类型',
                                   speed								double 		comment '速度',
                                   speed_km		                    double      comment '速度 单位 km/h ',
                                   vessel_class						string		comment '船类型',
                                   vessel_class_name                   string      comment '船类型中文',
                                   draught								double 		comment '吃水深度',
                                   cn_iso2								string		comment '国家code',
                                   country_name                        string      comment '国家中文',
    -- nation_flag_minio_url_jpg           string      comment '国旗',
                                   nav_status 							double 		comment '航行状态',
                                   nav_status_name                     string      comment '航行状态中文',
                                   dimensions_01						double 		comment '',
                                   dimensions_02						double 		comment '',
                                   dimensions_03						double 		comment '',
                                   dimensions_04						double 		comment '',
                                   block_map_index             		bigint      comment '图层层级',
                                   block_range_x             			double      comment '块x',
                                   block_range_y              			double      comment '块y',
                                   position_country_code2              string      comment '位置所在的国家',
                                   friend_foe 					        string      comment '敌我类型',
                                   update_time             			string      comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.27.95.211:30030',
     'table.identifier' = 'sa.dwd_ais_vessel_all_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='10000',
     'sink.batch.interval'='10s',
     'sink.properties.escape_delimiters' = 'flase',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 创建映射doris的状态数据表(Sink:doris)
drop table  if exists ais_status_info_doris;
create table ais_status_info_doris(
                                      vessel_id      	 				    bigint		comment '船ID',
                                      acquire_timestamp_format			string      comment '时间戳格式化',
                                      acquire_timestamp					bigint		comment '时间戳',
                                      vessel_name							string      comment '船名称',
                                      rate_of_turn						double  	comment '转向率',
                                      orientation							double 		comment '方向',
                                      master_image_id						bigint		comment '主图像ID',
                                      lng 					            double 		comment '经度',
                                      lat 					            double 		comment '纬度',
                                      source								string 		comment	'来源类型',
                                      speed								double 		comment '速度',
                                      speed_km		                    double      comment '速度 单位 km/h ',
                                      vessel_class						string		comment '船类别',
                                      vessel_class_name                   string      comment '船类型中文',
                                      draught								double 		comment '吃水深度',
                                      cn_iso2								string		comment '国家code',
                                      country_name                        string      comment '国家中文',
                                      nation_flag_minio_url_jpg           string      comment '国旗',
                                      nav_status 							double 		comment '航行状态',
                                      nav_status_name                     string      comment '航行状态中文',
                                      dimensions_01						double 		comment '',
                                      dimensions_02						double 		comment '',
                                      dimensions_03						double 		comment '',
                                      dimensions_04						double 		comment '',
                                      block_map_index             		bigint      comment '图层层级',
                                      block_range_x             			double      comment '块x',
                                      block_range_y              			double      comment '块y',
                                      position_country_code2              string      comment '位置所在的国家',
                                      friend_foe 					        string      comment '敌我类型',
                                      update_time             			string      comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.27.95.211:30030',
     'table.identifier' = 'sa.dws_ais_vessel_status_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='10000',
     'sink.batch.interval'='5s',
     'sink.properties.escape_delimiters' = 'flase',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-- 航行状态数据匹配库（Source：doris）
drop table if exists dim_vessel_nav_status_list;
create table dim_vessel_nav_status_list (
                                            nav_status_num_code         string        comment '航向状态代码code',
                                            nav_status_name             string        comment '航行状态名称',
                                            primary key (nav_status_num_code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_vessel_nav_status_list',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );



-- 船国家数据匹配库（Source：doris）
drop table if exists dim_vessel_country_code_list;
create table dim_vessel_country_code_list (
                                              country                   string        comment '国家英文',
                                              flag_country_code         string        comment '国家的编码',
                                              country_name              string        comment '国家的中文',
                                              minio_url_jpg             string        comment '国家的国旗',
                                              belong_type               string        comment '数据所属实体',
                                              primary key (country) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_vessel_country_code_list',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );



-- 船类型数据匹配库（Source：doris）
drop table if exists dim_vessel_class_list;
create table dim_vessel_class_list (
                                       vessel_class              string        comment '船类型',
                                       vessel_class_num_code     string        comment '列表接口返回的数字',
                                       vessel_class_name         string        comment '注释名称',
                                       primary key (vessel_class) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_vessel_class_list',
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
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );


-- 船静态属性信息
drop table if exists dws_ais_vessel_detail_static_attribute;
create table dws_ais_vessel_detail_static_attribute (
                                                        vessel_id          bigint        comment '船舶ID',
                                                        vessel_type        string        comment '船小类别',
                                                        primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_ais_vessel_detail_static_attribute',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '20000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '1'
      );

-----------------------

-- 数据处理

-----------------------

-- create function getCountry as 'GetCountryFromLngLat.getCountryFromLngLat' language python ;
create function getCountry as 'com.jingan.udf.GetCountryFromLngLat';


-- 解析数据第1步
drop table if exists tem_ais_kafka_01_pre_00;
create view tem_ais_kafka_01_pre_00 as
select
    t1.vesselId        as vessel_id,
    from_unixtime(t1.`timestamp`,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    t1.`timestamp`	 as acquire_timestamp,
    t1.name            as vessel_name,
    t1.rateOfTurn      as rate_of_turn,
    t1.orientation     ,
    t1.masterImageId   as master_image_id,
    t1.coordinates[1]  as lng,
    t1.coordinates[2]  as lat,
    t1.source          ,
    t1.speed           ,
    t1.vesselClass as vessel_class,
    t4.vessel_class_name        as vessel_class_name,
    t1.draught         ,
    if(t1.cnIso2 is not null and t1.cnIso2 <> '',t1.cnIso2,'NONE') as cn_iso2         ,
    t3.country_name as country_name,
    t3.minio_url_jpg as nation_flag_minio_url_jpg,
    t1.navStatus as nav_status      ,
    t2.nav_status_name as nav_status_name,
    t1.dimensions[1] as dimensions_01,
    t1.dimensions[2] as dimensions_02,
    t1.dimensions[3] as dimensions_03,
    t1.dimensions[4] as dimensions_04,
    t1.block_map_index               ,
    t1.block_range_x                 ,
    t1.block_range_y                 ,
    proctime                         ,
    getCountry(t1.coordinates[1],t1.coordinates[2]) as country_code3, -- 经纬度位置转换国家
    t5.vessel_type as vessel_type,
    case
        when cnIso2 in('IN','US','JP','AU') and t5.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR') then 'ENEMY' -- 敌
        when cnIso2='CN' and t5.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then 'OUR_SIDE' -- 我
        when cnIso2='CN' and t5.vessel_type not in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then 'FRIENDLY_SIDE' -- 友
        else 'NEUTRALITY' end friend_foe -- 敌我类型
    -- vessel_detail.data.vessels[1] as tmp_object_data
from ais_fleetmon_collect_item_kafka as t1

         left join dim_vessel_nav_status_list
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on cast(t1.navStatus as string) = t2.nav_status_num_code

         left join dim_vessel_country_code_list
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on if(t1.cnIso2 is not null and t1.cnIso2 <> '',t1.cnIso2,'NONE')  = t3.flag_country_code
                       and 'FLEETMON' = t3.belong_type

         left join dim_vessel_class_list
    FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.vesselClass = t4.vessel_class_num_code

         left join dws_ais_vessel_detail_static_attribute
    FOR SYSTEM_TIME AS OF t1.proctime as t5
                   on t1.vesselId = t5.vessel_id
where t1.vesselId is not null;

-- 转换国家编码
drop table if exists tem_ais_kafka_01_pre;
create view tem_ais_kafka_01_pre as
select
    a.*,
    b.code2 as position_country_2code
from tem_ais_kafka_01_pre_00 a
         left join dim_country_info
    FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.country_code3=b.code3;


drop table if exists tem_ais_kafka_01;
create view tem_ais_kafka_01 as
select
    *,
    if(position_country_2code is null
           and ((lng between 107.491636 and 124.806089 and lat between 20.522241 and 40.799277)
            or
                (lng between 107.491636 and 121.433286 and lat between 3.011639 and 20.522241)
           )
        ,'CN', position_country_2code) as position_country_code2
from tem_ais_kafka_01_pre;



-- select vessel_id,cn_iso2,vessel_type,friend_foe from tem_ais_kafka_01;

-----------------------

-- 数据插入

-----------------------

begin statement set;

-- ais的全量数据入库
insert into ais_all_info_doris
select
    vessel_id      	 					    ,
    acquire_timestamp_format				,
    acquire_timestamp						,
    vessel_name								,
    rate_of_turn							,
    orientation								,
    master_image_id							,
    lng 						            ,
    lat 						            ,
    source									,
    speed									,
    speed * 1.852 as speed_km               ,
    vessel_class							,
    vessel_class_name                       ,
    draught									,
    cn_iso2									,
    country_name                            ,
    -- nation_flag_minio_url_jpg               ,
    nav_status 								,
    nav_status_name                         ,
    dimensions_01							,
    dimensions_02							,
    dimensions_03							,
    dimensions_04							,
    block_map_index             			,
    block_range_x             				,
    block_range_y              				,
    position_country_code2                  ,
    friend_foe                              ,
    from_unixtime(unix_timestamp()) as update_time
from tem_ais_kafka_01;


-- ais的全量状态数据入库
insert into ais_status_info_doris
select
    vessel_id      	 					    ,
    acquire_timestamp_format				,
    acquire_timestamp						,
    vessel_name								,
    rate_of_turn							,
    orientation								,
    master_image_id							,
    lng 						            ,
    lat 						            ,
    source									,
    speed									,
    speed * 1.852 as speed_km               ,
    vessel_class							,
    vessel_class_name                       ,
    draught									,
    cn_iso2									,
    country_name                            ,
    nation_flag_minio_url_jpg               ,
    nav_status 								,
    nav_status_name                         ,
    dimensions_01							,
    dimensions_02							,
    dimensions_03							,
    dimensions_04							,
    block_map_index             			,
    block_range_x             				,
    block_range_y              				,
    position_country_code2                  ,
    friend_foe                              ,
    from_unixtime(unix_timestamp()) as update_time
from tem_ais_kafka_01;

end;



-- -- ais全量数据的详细信息入库
-- insert into ais_all_detail_info_doris
-- select
--     VESSEL_ID                   as vessel_id      	 	,
-- 	cast(null as varchar)       as fleet_typename 		,
-- 	cast(null as boolean)       as is_on_my_fleet		,
-- 	cast(null as boolean)       as is_on_shared_fleet	,
-- 	cast(null as boolean)       as is_on_own_fleet		,
-- 	cast(null as bigint)        as eta                  ,
-- 	DRAFT                       as draught				,
-- 	cast(null as varchar)       as static_typename		,
-- 	cast(null as varchar)       as destination 			,
-- 	cast(null as bigint)        as static_timestamp		,
-- 	cast(null as bigint)        as vessel_id2			,
-- 	MMSI                        as mmsi					,
-- 	cast(null as varchar)       as identity_typename	,
-- 	cast(null as varchar)       as vessel_name 			,
-- 	cast(null as varchar)       as callsign 			,
-- 	IMO                         as imo      			,
-- 	cast(null as varchar)       as vessels_typename					,
-- 	cast(null as double)        as rate_of_turn 					,
-- 	cast(null as double)        as heading							,
-- 	cast(null as varchar)       as navigational_typename			,
-- 	cast(null as double)        as course_over_ground				,
-- 	cast(null as double)        as longitude				        ,
-- 	cast(null as double)        as latitude				            ,
-- 	cast(null as varchar)       as location_typename				,
-- 	cast(null as varchar)       as location_description_typename	,
-- 	cast(null as varchar)       as location_description_short_name	,
-- 	cast(null as varchar)       as navigational_status				,
-- 	cast(null as varchar)       as navigational_source         		,
--     cast(null as double)        as navigational_speed          		,
--     cast(null as bigint)        as navigational_timestamp      		,
--     cast(null as varchar)       as datasheet_owner             		,
--     cast(null as double)        as draught_average           		,
-- 	cast(null as double)        as gross_tonnage             		,
--     cast(null as double)        as speed_max                 		,
--     cast(null as varchar)       as datasheet_typename          		,
--     LENGTH                      as length                   		,
--     cast(null as double)        as speed_average             		,
--     cast(null as varchar)       as flag_country_code          		,
--     BUILD_TIME_YEAR             as year_built                		,
--     cast(null as varchar)       as vessel_class              		,
--     cast(null as varchar)       as risk_rating               		,
--     cast(null as varchar)       as vessel_type               		,
--     cast(null as varchar)       as service_status            		,
--     WIDTH                       as width                    		,
--     cast(null as double)        as deadweight               		,
--     cast(null as varchar)       as height                   		,
--     AIS_NAME                    as ais_name                         ,
--     SPEED                       as speed                            ,
--     WAIL                        as wail                             ,
--     COUNTORY                    as country                          ,
-- 	from_unixtime(unix_timestamp()) as update_time
-- from ais_fleetmon_detail_collect_kafka;

-- -- 创建映射doris的船只的详细信息数据表(Sink:doris)
-- drop table  if exists ais_all_detail_info_doris;
-- create table ais_all_detail_info_doris(
--   	vessel_id      	 				    bigint		comment '船ID',
-- 	fleet_typename 						string		comment '船队类型名称flag',
-- 	is_on_my_fleet						boolean		comment '是我的船队',
-- 	is_on_shared_fleet					boolean		comment '是否共享船队',
-- 	is_on_own_fleet						boolean 	comment '是否拥有船队',
-- 	eta                  				bigint   	comment '预计到达时间，在军用词汇中经常用到',
-- 	draught							    string   	comment '吃水',
-- 	static_typename						string		comment '静态类型名称flag',
-- 	destination 						string		comment '目的地',
-- 	static_timestamp					bigint 		comment '船只时间',
-- 	vessel_id2							bigint 		comment '船ID',
-- 	mmsi								string      comment 'mmsi',
-- 	identity_typename					string		comment '身份类型名称flag',
-- 	vessel_name 						string      comment '船名称2',
-- 	callsign 							string      comment '呼号',
-- 	imo      							string      comment 'IMO',
-- 	vessels_typename					string		comment '船类型名称flag',
-- 	rate_of_turn 						double		comment '转向率2',
-- 	heading								double 		comment '船首向',
-- 	navigational_typename				string		comment '导航类型名称flag',
-- 	course_over_ground					double      comment '对地航向',
-- 	longitude				            double 		comment '导航经度',
-- 	latitude				            double 		comment '导航纬度',
-- 	location_typename					string		comment '位置类型名称flag',
-- 	location_description_typename		string		comment '位置描述类型名称flag',
-- 	location_description_short_name		string		comment '位置短名称',
-- 	navigational_status					string		comment '导航状态',
-- 	navigational_source         		string      comment '导航数据来源',
--     navigational_speed          		double      comment '导航速度',
--     navigational_timestamp      		bigint      comment '导航时间戳',
--     datasheet_owner             		string      comment '所有者',
--     draught_average           			double      comment '吃水平均值',
-- 	gross_tonnage             			double      comment '总吨数',
--     speed_max                 			double      comment '速度最大值',
--     datasheet_typename          		string      comment '类型名称flag',
--     length                   			string		comment '长度',
--     speed_average             			double      comment '速度平均值',
--     flag_country_code          			string      comment '标志国家代码',
--     year_built                			string      comment '建成年份',
--     vessel_class              			string	 	comment '船类别',
--     risk_rating               			string	 	comment '风险评级',
--     vessel_type               			string	 	comment '船类型',
--     service_status            			string	 	comment '服务状态',
--     width                    			string      comment '宽度',
--     deadweight               			double      comment '重物，载重吨位',
--     height                   			string      comment '高度',
--     ais_name                            string      comment 'ais名称',
--     speed                               string      comment '速度',
--     wail                                string      comment '',
--     country                             string      comment '国家',
-- 	update_time             			string      comment '数据入库时间'
-- )WITH (
-- 'connector' = 'doris',
-- 'fenodes' = '172.21.30.202:30030',
-- 'table.identifier' = 'kesadaran_situasional.ais_all_detail_info',
-- 'username' = 'admin',
-- 'password' = 'Jingansi@110',
-- 'doris.request.tablet.size'='1',
-- 'doris.request.read.timeout.ms'='30000',
-- 'sink.batch.size'='100000',
-- 'sink.batch.interval'='5s',
-- 'sink.properties.escape_delimiters' = 'flase',
-- 'sink.properties.column_separator' = '\x01',	 -- 列分隔符
-- 'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
-- 'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
-- );



-- -- 数据质量需要的数据表
-- -- ais全量数据表
-- drop table  if exists ais_all_info_doris;
-- create table ais_all_info_doris (
--     vessel_id                      bigint        comment '船ID',
--     acquire_timestamp_format       timestamp     comment '时间戳格式化',
--     acquire_timestamp              bigint        comment '时间戳',
--     update_time                    timestamp     comment '数据入库时间',
--    primary key (vessel_id,acquire_timestamp_format) NOT ENFORCED
-- ) with (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://172.21.30.202:31030/kesadaran_situasional?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
--     'username' = 'root',
--     'password' = 'root',
--     'table-name' = 'ais_all_info',
--     'driver' = 'com.mysql.cj.jdbc.Driver',
--     'lookup.cache.max-rows' = '3000',
--     'lookup.cache.ttl' = '10s',
--     'lookup.max-retries' = '1'
-- );


-- -- 飞机每天全量数据表
-- drop table  if exists dwd_ads_b_flightaware_rt_doris;
-- create table dwd_ads_b_flightaware_rt_doris (
--     flight_id                      string        comment '飞行ID',
--     request_time                   timestamp     comment '时间',
--     request_timestamp              bigint        comment '时间戳',
--     update_time                    timestamp     comment '更新时间',
--    primary key (flight_id,request_time) NOT ENFORCED
-- ) with (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://172.21.30.202:31030/kesadaran_situasional?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
--     'username' = 'root',
--     'password' = 'root',
--     'table-name' = 'dwd_ads_b_flightaware_rt',
--     'driver' = 'com.mysql.cj.jdbc.Driver',
--     'lookup.cache.max-rows' = '3000',
--     'lookup.cache.ttl' = '10s',
--     'lookup.max-retries' = '1'
-- );


-- -- ais详情数据表
-- drop table  if exists ais_all_detail_info_doris;
-- create table ais_all_detail_info_doris (
--     vessel_id                      bigint        comment '船ID',
--     fleet_typename                 string        comment '船队类型名称flag',
--     update_time                    timestamp     comment '数据入库时间',
--    primary key (vessel_id) NOT ENFORCED
-- ) with (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://172.21.30.202:31030/kesadaran_situasional?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
--     'username' = 'root',
--     'password' = 'root',
--     'table-name' = 'ais_all_detail_info',
--     'driver' = 'com.mysql.cj.jdbc.Driver',
--     'lookup.cache.max-rows' = '3000',
--     'lookup.cache.ttl' = '10s',
--     'lookup.max-retries' = '1'
-- );




-- -- 创建kafka全量AIS数据来源的详情信息数据表（Source：kafka）
-- drop table  if exists ais_fleetmon_detail_collect_kafka;
-- create table ais_fleetmon_detail_collect_kafka(
-- 	VESSEL_ID             bigint       comment '船ID',
--     MMSI                  string       comment 'MMSI号',
--     IMO                   string       comment 'IMO号',
--     AIS_NAME              string       comment 'AIS名称',
--     SPEED                 string       comment '速度',
--     BUILD_TIME_YEAR       string       comment '建造年份',
--     WAIL                  string       comment '',
--     DRAFT                 string       comment '吃水',
--     WIDTH                 string       comment '宽度',
--     LENGTH                string       comment '长度',
--     COUNTORY              string       comment '国家'
-- ) with (
--     'connector' = 'kafka',
--     'topic' = 'ais_fleetmon_detail_collect',
--     'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
--     'properties.group.id' = 'ais-fleetmon-detail-collect-rt',
--     -- 'scan.startup.mode' = 'latest-offset',
--     'scan.startup.mode' = 'timestamp',
--     'scan.startup.timestamp-millis' = '1683610952000',
--     'format' = 'json',
--     'json.fail-on-missing-field' = 'false',
--     'json.ignore-parse-errors' = 'true'
-- );


-- -- 创建映射doris的船只的详细信息数据表(Sink:doris)
-- drop table  if exists ais_detail_info_doris;
-- create table ais_detail_info_doris(
--   	vessel_id             bigint        comment '船ID',
--     mmsi                  string        comment 'MMSI号',
--     imo                   string        comment 'IMO号',
--     ais_name              string        comment 'AIS名称',
--     speed                 string        comment '速度',
--     build_time_year       string        comment '建造年份',
--     wail                  string        comment '',
--     draft                 string        comment '吃水',
--     width                 string        comment '宽度',
--     height                string        comment '高度',
--     country               string        comment '国家',
-- 	update_time           string        comment '数据入库时间'
-- )WITH (
-- 'connector' = 'doris',
-- 'fenodes' = '172.21.30.202:30030',
-- 'table.identifier' = 'kesadaran_situasional.ais_detail_info',
-- 'username' = 'admin',
-- 'password' = 'admin',
-- 'doris.request.tablet.size'='1',
-- 'doris.request.read.timeout.ms'='30000',
-- 'sink.batch.size'='100000',
-- 'sink.batch.interval'='5s'
-- -- 'sink.properties.escape_delimiters' = 'true',
-- -- 'sink.properties.column_separator' = ','
-- );



-- -- 解析数据第2步
-- drop table if exists tem_ais_kafka_02;
-- create view tem_ais_kafka_02 as
-- select
--     vessel_id,
-- 	acquire_timestamp_format,
-- 	acquire_timestamp,
--     vessel_name,
--     rate_of_turn,
--     orientation     ,
--     master_image_id,
-- 	lng,
--   	lat,
--     source          ,
-- 	speed           ,
-- 	vessel_class    ,
--     draught         ,
--     cn_iso2         ,
--   	nav_status      ,
-- 	dimensions_01,
-- 	dimensions_02,
-- 	dimensions_03,
-- 	dimensions_04,
--     block_map_index,
--     block_range_x  ,
--     block_range_y  ,
--     tmp_object_data.myFleetInformation as my_fleet_information,
--     tmp_object_data.staticInformation as static_information,
--     tmp_object_data.`identity` as identity_information,
--     tmp_object_data.__typename as vessels_typename,
--     tmp_object_data.navigationalInformation as navigational_information,
--     tmp_object_data.datasheet as datasheet
-- from tem_ais_kafka_01
--   where tmp_object_data is not null;


-- -- 解析数据第3步
-- drop table if exists tem_ais_kafka_03;
-- create view tem_ais_kafka_03 as
-- select
--     vessel_id,
-- 	acquire_timestamp_format,
-- 	acquire_timestamp,
--     vessel_name,
--     rate_of_turn,
--     orientation     ,
--     master_image_id,
-- 	lng,
--   	lat,
--     source          ,
-- 	speed           ,
-- 	vessel_class     ,
--     draught         ,
--     cn_iso2         ,
--   	nav_status      ,
-- 	dimensions_01,
-- 	dimensions_02,
-- 	dimensions_03,
-- 	dimensions_04,
--     block_map_index,
--     block_range_x  ,
--     block_range_y  ,
--     my_fleet_information.__typename as fleet_typename,
--     my_fleet_information.isOnMyFleet as is_on_my_fleet,
--     my_fleet_information.isOnSharedFleet as is_on_shared_fleet,
--     my_fleet_information.isOnOwnFleet as is_on_own_fleet,
--     static_information.eta as eta,
--     static_information.draught as draught2,
--     static_information.__typename as static_typename,
--     static_information.destination as destination,
--     static_information.`timestamp` as static_timestamp,
--     identity_information.vesselId as vessel_id2,
--     identity_information.mmsi as mmsi,
--     identity_information.__typename as identity_typename,
--     identity_information.name as vessel_name2,
--     identity_information.callsign as callsign,
--     identity_information.imo as imo,
--     vessels_typename,
--     navigational_information.rateOfTurn as rate_of_turn2,
--     navigational_information.heading as heading,
--     navigational_information.__typename as navigational_typename,
--     navigational_information.courseOverGround as course_over_ground,
--     navigational_information.location.longitude as longitude,
--     navigational_information.location.latitude as latitude,
--     navigational_information.location.__typename as location_typename,
--     navigational_information.location.locationDescription[1].__typename as location_description_typename,
--     navigational_information.location.locationDescription[1].shortName as location_description_short_name,
--     navigational_information.navigationalStatus as navigational_status,
--     navigational_information.source as navigational_source,
--     navigational_information.speed as navigational_speed,
--     navigational_information.`timestamp` as navigational_timestamp,
--     datasheet.owner as datasheet_owner,
--     datasheet.draughtAverage as draught_average,
--     datasheet.grossTonnage as gross_tonnage,
--     datasheet.speedMax as speed_max,
--     datasheet.__typename as datasheet_typename,
--     datasheet.length as length,
--     datasheet.speedAverage as speed_average,
--     datasheet.flagCountryCode as flag_country_code,
--     datasheet.yearBuilt as year_built,
--     datasheet.vesselClass as vessel_class2,
--     datasheet.riskRating as risk_rating,
--     datasheet.vesselType as vessel_type,
--     datasheet.serviceStatus as service_status,
--     datasheet.width as width,
--     datasheet.deadweight as deadweight,
--     datasheet.height as height
-- from tem_ais_kafka_02;


-- -- 筛选出正常数据（不是详细信息的数据）
-- drop table if exists tem_ais_kafka_04;
-- create view tem_ais_kafka_04 as
-- select
--   *
-- from tem_ais_kafka_01
--   where tmp_object_data is null;


-- -- 创建映射doris的船只的详细信息数据表(Sink:doris)
-- drop table  if exists ais_all_detail_info_doris;
-- create table ais_all_detail_info_doris(
--   	vessel_id      	 				    bigint		comment '船ID',
-- 	fleet_typename 						string		comment '船队类型名称flag',
-- 	is_on_my_fleet						boolean		comment '是我的船队',
-- 	is_on_shared_fleet					boolean		comment '是否共享船队',
-- 	is_on_own_fleet						boolean 	comment '是否拥有船队',
-- 	eta                  				bigint   	comment '预计到达时间，在军用词汇中经常用到',
-- 	draught							    double   	comment '吃水深度',
-- 	static_typename						string		comment '静态类型名称flag',
-- 	destination 						string		comment '目的地',
-- 	static_timestamp					bigint 		comment '船只时间',
-- 	vessel_id2							bigint 		comment '船ID',
-- 	mmsi								bigint      comment 'mmsi',
-- 	identity_typename					string		comment '身份类型名称flag',
-- 	vessel_name 						string      comment '船名称2',
-- 	callsign 							string      comment '呼号',
-- 	imo      							bigint      comment 'IMO',
-- 	vessels_typename					string		comment '船类型名称flag',
-- 	rate_of_turn 						double		comment '转向率2',
-- 	heading								double 		comment '船首向',
-- 	navigational_typename				string		comment '导航类型名称flag',
-- 	course_over_ground					double      comment '对地航向',
-- 	longitude				            double 		comment '导航经度',
-- 	latitude				            double 		comment '导航纬度',
-- 	location_typename					string		comment '位置类型名称flag',
-- 	location_description_typename		string		comment '位置描述类型名称flag',
-- 	location_description_short_name		string		comment '位置短名称',
-- 	navigational_status					string		comment '导航状态',
-- 	navigational_source         		string      comment '导航数据来源',
--     navigational_speed          		double      comment '导航速度',
--     navigational_timestamp      		bigint      comment '导航时间戳',
--     datasheet_owner             		string      comment '所有者',
--     draught_average           			double      comment '吃水平均值',
-- 	gross_tonnage             			double      comment '总吨数',
--     speed_max                 			double      comment '速度最大值',
--     datasheet_typename          		string      comment '类型名称flag',
--     length                   			double		comment '长度',
--     speed_average             			double      comment '速度平均值',
--     flag_country_code          			string      comment '标志国家代码',
--     year_built                			double      comment '建成年份',
--     vessel_class              			string	 	comment '船类别',
--     risk_rating               			string	 	comment '风险评级',
--     vessel_type               			string	 	comment '船类型',
--     service_status            			string	 	comment '服务状态',
--     width                    			double      comment '宽度',
--     deadweight               			double      comment '重物，载重吨位',
--     height                   			double      comment '高度',
-- 	update_time             			string      comment '数据入库时间'
-- )WITH (
-- 'connector' = 'doris',
-- 'fenodes' = '172.21.30.202:30030',
-- 'table.identifier' = 'kesadaran_situasional.ais_all_detail_info',
-- 'username' = 'admin',
-- 'password' = 'admin',
-- 'doris.request.tablet.size'='1',
-- 'doris.request.read.timeout.ms'='30000',
-- 'sink.batch.size'='100000',
-- 'sink.batch.interval'='5s',
-- 'sink.properties.escape_delimiters' = 'true',
-- 'sink.properties.column_separator' = ','
-- );

-- -- ais全量数据的详细信息入库
-- insert into ais_all_detail_info_doris
-- select
--   	vessel_id      	 					    ,
--     fleet_typename 							,
--     is_on_my_fleet							,
--     is_on_shared_fleet						,
--     is_on_own_fleet							,
--     eta                  					,
--     draught 								,
--     static_typename							,
--     destination 							,
--     static_timestamp						,
--     vessel_id2					            ,
--     mmsi									,
--     identity_typename						,
--     vessel_name2 as vessel_name 			,
--     callsign 								,
--     imo      								,
--     vessels_typename						,
--     rate_of_turn2 as rate_of_turn 			,
--     heading									,
--     navigational_typename					,
--     course_over_ground						,
--     longitude					            ,
--     latitude					            ,
--     location_typename						,
--     location_description_typename			,
--     location_description_short_name			,
--     navigational_status						,
--     navigational_source         			,
--     navigational_speed          			,
--     navigational_timestamp      			,
--     datasheet_owner             			,
--     draught_average           				,
--     gross_tonnage             				,
--     speed_max                 				,
--     datasheet_typename          			,
--     length                   				,
--     speed_average             				,
--     flag_country_code          				,
--     year_built                				,
--     vessel_class2 as vessel_class           ,
--     risk_rating               				,
--     vessel_type               				,
--     service_status            				,
--     width                    				,
--     deadweight               				,
--     height                   				,
--     from_unixtime(unix_timestamp()) as update_time
-- from tem_ais_kafka_03;


-- -- 创建kafka数据来源的表（Source：kafka）
-- drop table  if exists ais_fleetmon_collect_item_kafka;
-- create table ais_fleetmon_collect_item_kafka(
-- 	vesselId             bigint       comment '船ID',
--    `timestamp`           bigint       comment '时间戳',
--     name                 string       comment '船名称',
--     rateOfTurn           double       comment '转向率',
--     orientation          double       comment '方向',
--     masterImageId        bigint       comment '主图像ID',
-- 	coordinates array <               -- 船只坐标
-- 		double
-- 	>,
--     source               string       comment '数据来源简称',
-- 	speed                double       comment '速度',
-- 	vesselClass          string       comment '船类别',
--     draught              double       comment '吃水深度',
--     cnIso2               string       comment '',
--   	navStatus            bigint       comment '导航状态',
-- 	dimensions array<
-- 		double
-- 	>,
--     block_map_index      bigint      comment '图层层级',
--     block_range_x        double      comment '区域块x',
--     block_range_y        double      comment '区域块y',
--     vessel_detail row(                                 -- 船只详细
--       `data` row(                                      -- 数据
--           vessels array<                               -- 船
--               row(
--                   myFleetInformation row(              -- 船队信息
--                       __typename          string,      -- 类型名称flag
--                       isOnMyFleet         boolean,     -- 是我的船队
--                       isOnSharedFleet     boolean,     -- 是否共享船队
--                       isOnOwnFleet        boolean      -- 是否拥有船队
--                     ),
--                   staticInformation row(               -- 静态信息
--                       eta                 bigint,      -- 预计到达时间，在军用词汇中经常用到
--                       draught             double,      -- 吃水深度
--                       __typename          string,      -- 类型名称flag
--                       destination         string,      -- 目的地
--                       `timestamp`         bigint       -- 船只时间
--                     ),
--                   `identity` row(                        -- 身份
--                       vesselId            bigint,      -- 船ID
--                       mmsi                bigint,      -- 船MMSI
--                       __typename          string,      -- 类型名称flag
--                       name                string,      -- 船名称
--                       callsign            string,      -- 呼号
--                       imo                 bigint       -- IMO
--                     ),
--                   __typename              string,      -- 类型名称flag
--                   navigationalInformation row(         -- 导航信息
--                       rateOfTurn          double,      -- 转向率
--                       heading             double,      -- 船首向
--                       __typename          string,      -- 类型名称
--                       courseOverGround    double,      -- 对地航向
--                       location row(
--                           longitude       double,      -- 导航经度
--                           latitude        double,      -- 导航纬度
--                           __typename      string,      -- 类型名称flag
--                           locationDescription array <  -- 位置描述
--                             row(
--                                 __typename string,     -- 类型名称flag
--                                 shortName  string      -- 短名称
--                               )
--                             >
--                         ),
--                       navigationalStatus       string,      -- 导航状态
--                       source                   string,      -- 导航数据来源
--                       speed                    double,      -- 导航速度
--                       `timestamp`              bigint       -- 导航时间戳
--                     ),
--                   datasheet row(                            -- 数据表
--                       owner                    string,      -- 所有者
--                       draughtAverage           double,      -- 吃水平均值
--                       grossTonnage             double,      -- 总吨数
--                       speedMax                 double,      -- 速度最大值
--                       __typename               string,      -- 类型名称flag
--                       length                   double,      -- 长度
--                       speedAverage             double,      -- 速度平均值
--                       flagCountryCode          string,      -- 标志国家代码
--                       yearBuilt                double,      -- 建成年份
--                       vesselClass              string,      -- 船类别
--                       riskRating               string,      -- 风险评级
--                       vesselType               string,      -- 船类型
--                       serviceStatus            string,      -- 服务状态
--                       width                    double,      -- 宽度
--                       deadweight               double,      -- 重物，载重吨位
--                       height                   double       -- 高度
--                     )
--               )
--           >
--       )
--     )
-- ) with (
--     'connector' = 'kafka',
--     'topic' = 'ais_fleetmon_collect_item',
--     'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
--     'properties.group.id' = 'ais-fleetmon-collect-item-rt',
--     -- 'scan.startup.mode' = 'latest-offset',
--     'scan.startup.mode' = 'timestamp',
--     'scan.startup.timestamp-millis' = '1677515502000',
--     'format' = 'json',
--     'json.fail-on-missing-field' = 'false',
--     'json.ignore-parse-errors' = 'true'
-- );


-- -- 创建映射doris的全量数据表(Sink:doris)
-- drop table if exists ais_fleetmon_collect_item_doris;
-- create table ais_fleetmon_collect_item_doris (
-- 	vessel_id      	 		        bigint			comment '船ID',
-- 	acquire_timestamp_format		string          comment '时间戳格式化',
-- 	acquire_timestamp				bigint			comment '时间戳',
-- 	vessel_name					    string          comment '船名称',
-- 	rate_of_turn			        double  		comment '转向率',
-- 	orientation				        double 			comment '方向',
-- 	master_image_id			        bigint			comment '主图像ID',
-- 	lng 					        double 			comment '经度',
-- 	lat 					        double 			comment '维度',
-- 	source					        string 		    comment	'',
-- 	speed					        double 			comment '速度',
-- 	vessel_class				    string		    comment '船类别',
-- 	draught					        double 			comment '吃水',
-- 	cn_iso2					        string		    comment '',
-- 	navStatus 				        double 			comment '导航状态',
-- 	dimensions_01			        double 			comment '',
-- 	dimensions_02			        double 			comment '',
-- 	dimensions_03			        double 			comment '',
-- 	dimensions_04			        double 			comment '',
--     block_map_index                 bigint          comment '图层层级',
--     block_range_x                   double          comment '区域块x',
--     block_range_y                   double          comment '区域块y',
--     update_time                     string          comment '数据入库时间',
--     primary key (vessel_id,acquire_timestamp_format) NOT ENFORCED
-- ) with (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/kesadaran_situasional?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
--     'username' = 'root',
--     'password' = 'jingansi110',
--     'table-name' = 'ais_all_info',
--     'driver' = 'com.mysql.cj.jdbc.Driver',
--     'lookup.cache.max-rows' = '5000',
--     'lookup.cache.ttl' = '3600s',
--     'lookup.max-retries' = '3'
-- );


-- -- 创建映射doris的状态信息表(Sink:doris)
-- drop table if exists ais_fleetmon_collect_item_status_doris;
-- create table ais_fleetmon_collect_item_status_doris (
-- 	vessel_id      	 		        bigint			comment '船ID',
-- 	acquire_timestamp_format		string          comment '时间戳格式化',
-- 	acquire_timestamp				bigint			comment '时间戳',
-- 	vessel_name					    string          comment '船名称',
-- 	rate_of_turn			        double  		comment '转向率',
-- 	orientation				        double 			comment '方向',
-- 	master_image_id			        bigint			comment '主图像ID',
-- 	lng 					        double 			comment '经度',
-- 	lat 					        double 			comment '维度',
-- 	source					        string 		    comment	'',
-- 	speed					        double 			comment '速度',
-- 	vessel_class				    string		    comment '船类别',
-- 	draught					        double 			comment '吃水',
-- 	cn_iso2					        string		    comment '',
-- 	navStatus 				        double 			comment '导航状态',
-- 	dimensions_01			        double 			comment '',
-- 	dimensions_02			        double 			comment '',
-- 	dimensions_03			        double 			comment '',
-- 	dimensions_04			        double 			comment '',
--     block_map_index                 bigint          comment '图层层级',
--     block_range_x                   double          comment '区域块x',
--     block_range_y                   double          comment '区域块y',
--     update_time                     string          comment '数据入库时间',
--     primary key (vessel_id) NOT ENFORCED
-- ) with (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/kesadaran_situasional?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
--     'username' = 'root',
--     'password' = 'jingansi110',
--     'table-name' = 'ais_status_info',
--     'driver' = 'com.mysql.cj.jdbc.Driver',
--     'lookup.cache.max-rows' = '5000',
--     'lookup.cache.ttl' = '3600s',
--     'lookup.max-retries' = '3'
-- );

