--********************************************************************--
-- author:      write your name here
-- create time: 2023/7/6 21:50:17
-- description: marinetraffic 网站船舶数据入库船舶融合表
--********************************************************************--
set 'pipeline.name' = 'ja-ais-marinetraffic-list-rt';


set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '6';

set 'execution.checkpointing.interval' = '300000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-ais-marineTraffic-list-rt-checkpoint-test';
-- 空闲分区不用等待
-- set 'table.exec.source.idle-timeout' = '3s';


-----------------------

 -- 数据结构

 -----------------------


-- 创建kafka全量marineTraffic数据来源的表（Source：kafka）
create table marinetraffic_ship_list(
                                        SHIP_ID              string, -- 船舶的唯一标识符
                                        SHIPNAME             string, -- 船舶的名称
                                        SPEED                string, -- 船舶的当前速度，以节（knots）为单位 10倍
                                        ROT                  string, -- 船舶的旋转率
                                        W_LEFT               string, -- 舶的左舷吃水线宽度
                                        L_FORE               string, -- 船舶的前吃水线长度
                                        `timeStamp`          bigint, -- 采集时间
                                        DESTINATION          string, -- 船舶的目的地
                                        LON                  string, -- 船舶当前位置的经度值
                                        ELAPSED              string, -- 自上次位置报告以来经过的时间，以分钟为单位。
                                        COURSE               string, -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方，
                                        GT_SHIPTYPE          string, -- 船舶的全球船舶类型码。
                                        FLAG                 string, -- 船舶的国家或地区旗帜标识。
                                        LAT                  string, -- 船舶当前位置的经度值。
                                        SHIPTYPE             string, -- 船舶的类型码，表示船舶所属的船舶类型。
                                        HEADING              string, -- 船舶的船首朝向
                                        LENGTH               string, -- 船舶的长度，以米为单位
                                        WIDTH                string, -- 船舶的宽度，以米为单位。
                                        DWT                  string, -- 船舶的载重吨位。
                                        proctime             as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'marinetraffic_ship_list',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-ais-marinetraffic-list-rt-test',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1701471840000',
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
                                   c_name                              string      comment '船中文名',
                                   imo                                 string      comment 'imo',
                                   mmsi                                string      comment 'mmsi',
                                   callsign                            string      comment '呼号',
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
                                   vessel_type                         string      comment '船小类别',
                                   vessel_type_name                    string      comment '船类型中文名-小类',
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
                                   sea_id 					            string      comment '海域编号',
                                   sea_name 					        string      comment '中文名称',
                                   update_time             			string      comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.27.95.211:30030',
     'table.identifier' = 'sa.dwd_ais_vessel_all_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='15s',
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
                                      c_name                              string      comment '船中文名',
                                      imo                                 string      comment 'imo',
                                      mmsi                                string      comment 'mmsi',
                                      callsign                            string      comment '呼号',
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
                                      vessel_type                         string      comment '船小类别',
                                      vessel_type_name                    string      comment '船类型中文名-小类',
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
                                      sea_id 					            string      comment '海域编号',
                                      sea_name 					        string      comment '中文名称',
                                      update_time             			string      comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.27.95.211:30030',
     'table.identifier' = 'sa.dws_ais_vessel_status_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='50000',
     'sink.batch.interval'='15s',
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


create table dim_mt_fm_id_relation (
                                       ship_id                        bigint        comment '船编号',
                                       vessel_id                      bigint        comment '编号',
                                       vessel_type                    string        comment 'FleetMon 船大类',
                                       vessel_type_name               string        comment 'FleetMon 船大类中文',
                                       vessel_class                   string        comment 'FleetMon 船小类',
                                       vessel_class_name              string        comment 'FleetMon 船小类中文',
                                       c_name                         string        comment '船中文名',
                                       imo                            string        comment 'imo',
                                       mmsi                           bigint        comment 'mmsi',
                                       callsign                       string        comment '呼号',
                                       primary key (ship_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_mt_fm_id_relation',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '400000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '3'
      );

create table dws_ais_vessel_status_info (
                                            vessel_id                      bigint      comment 'mmsi',
                                            acquire_timestamp_format	   TIMESTAMP   comment '时间戳格式化',
                                            source						   string 		comment	'来源类型',
                                            primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_ais_vessel_status_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '400000',
      'lookup.cache.ttl' = '600s',
      'lookup.max-retries' = '3'
      );


drop table if exists dim_sea_area;
create table dim_sea_area (
                              id 			varchar(5) COMMENT '海域编号',
                              name 		varchar(60) COMMENT '名称',
                              c_name 		varchar(60) COMMENT '中文名称',
                              primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.27.95.211:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_sea_area',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );

-----------------------

-- 数据处理

-----------------------

-- create function getCountry as 'GetCountryFromLngLat.getCountryFromLngLat' language python ;
create function getCountry as 'com.jingan.udf.sea.GetCountryFromLngLat';
create function getSeaArea as 'com.jingan.udf.sea.GetSeaArea';

-- 字段转换
drop table if exists tmp_marinetraffic_ship_list_01;
create view tmp_marinetraffic_ship_list_01 as
select
    t2.vessel_id as vesselId                          , -- 船ID
    from_unixtime(`timeStamp`-(cast(ELAPSED as int)*60),'yyyy-MM-dd HH:mm:00') as acquire_timestamp_format  , -- 数据产生时间，爬虫采集的时间减 ELAPSED 经过的时间 ，格式化到分钟
    SHIPNAME as name                                  , -- 船名称
    cast(ROT as double) as rateOfTurn                                 , -- 转向率
    cast(COURSE as int) as orientation                             , -- 方向
    case when 1=2 then 0 end as masterImageId        , -- 主图像ID
    cast(LON as double) as lng,
    cast(LAT as double) as lat,
    case when 1=2 then '' end source                  , -- 数据来源简称
    cast(SPEED as double)/10 as speed                                    , -- 速度
    t2.vessel_class as vesselClass                    , -- 船类别,
    t2.vessel_class_name as vessel_class_name ,
    cast(L_FORE as double) as draught                                 , -- 吃水深度
    case when FLAG <> '--' then FLAG end  as cnIso2   , -- 国家
    case when 1=2 then 1.1 end as navStatus            , -- 导航状态
    case when 1=2 then '' end as  dimensions,
    t2.vessel_type as vessel_type,
    t2.vessel_type_name as vessel_type_name   ,
    t2.c_name           as c_name             ,
    t2.imo              as imo                ,
    cast(t2.mmsi as string)   as mmsi               ,
    t2.callsign          as callsign            ,
    case
        when FLAG in('IN','US','JP','AU') and t2.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR') then 'ENEMY' -- 敌
        when FLAG ='CN' and t2.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then 'OUR_SIDE' -- 我
        when FLAG ='CN' and t2.vessel_type not in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then 'FRIENDLY_SIDE' -- 友
        else 'NEUTRALITY'
        end as friend_foe,
    t1.proctime
from (select * from marinetraffic_ship_list where `timeStamp` is not null) t1
         left join dim_mt_fm_id_relation
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on cast(t1.SHIP_ID as bigint) = t2.ship_id
where t2.ship_id is not null
  and ELAPSED < 60;

-- 过滤fleetmon 感知的数据
drop table if exists tmp_marinetraffic_ship_list_02;
create view tmp_marinetraffic_ship_list_02 as
select
    a.*
from tmp_marinetraffic_ship_list_01 a
         left join dws_ais_vessel_status_info
    FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.vesselId = b.vessel_id
where b.source is null
   or abs(timestampdiff(MINUTE,to_timestamp(a.acquire_timestamp_format),b.acquire_timestamp_format)) >10;



-- 解析数据第1步
drop table if exists tem_ais_kafka_01_pre_00;
create view tem_ais_kafka_01_pre_00 as
select
    t1.vesselId        as vessel_id,
    t1.acquire_timestamp_format as acquire_timestamp_format,
    unix_timestamp(t1.acquire_timestamp_format)	 as acquire_timestamp,
    t1.name            as vessel_name,
    t1.rateOfTurn      as rate_of_turn,
    t1.orientation     ,
    t1.masterImageId   as master_image_id,
    t1.lng  as lng,
    t1.lat  as lat,
    t1.source          ,
    t1.speed           ,
    t1.vesselClass as vessel_class,
    t1.vessel_class_name        as vessel_class_name,
    t1.draught,
    t1.cnIso2 as cn_iso2,
    t3.country_name as country_name,
    if(t3.minio_url_jpg is not null,t3.minio_url_jpg,'/ja-acquire-images/ja-vessels-nation-flag-jpg/none.jpg') as nation_flag_minio_url_jpg,
    t1.navStatus as nav_status      ,
    t1.vessel_type as vessel_type,
    t1.vessel_type_name as vessel_type_name   ,
    t1.c_name           as c_name             ,
    t1.imo              as imo                ,
    t1.mmsi             as mmsi               ,
    t1.callsign          as callsign            ,
    case when 1=2 then '' end as nav_status_name,
    case when 1=2 then 1.1 end as dimensions_01,
    case when 1=2 then 1.1 end as dimensions_02,
    case when 1=2 then 1.1 end as dimensions_03,
    case when 1=2 then 1.1 end as dimensions_04,
    case when 1=2 then 1 end as block_map_index               ,
    case when 1=2 then 1.1 end as block_range_x                 ,
    case when 1=2 then 1.1 end as block_range_y                 ,
    proctime                         ,
    getCountry(t1.lng,t1.lat) as country_code3, -- 经纬度位置转换国家
    t1.vessel_type as vessel_type,
    t1.friend_foe as  friend_foe -- 敌我类型
    -- vessel_detail.data.vessels[1] as tmp_object_data
from tmp_marinetraffic_ship_list_02 as t1

         -- left join dim_vessel_nav_status_list
-- FOR SYSTEM_TIME AS OF t1.proctime as t2
-- on cast(t1.navStatus as string) = t2.nav_status_num_code

         left join dim_vessel_country_code_list
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.cnIso2  = t3.flag_country_code
                       and 'FLEETMON' = t3.belong_type
;


-- select * from tem_ais_kafka_01_pre_00;

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



drop table if exists tem_ais_kafka_01_pre_01;
create view tem_ais_kafka_01_pre_01 as
select
    *,
    if(position_country_2code is null
           and ((lng between 107.491636 and 124.806089 and lat between 20.522241 and 40.799277)
            or
                (lng between 107.491636 and 121.433286 and lat between 3.011639 and 20.522241)
           )
        ,'CN', position_country_2code) as position_country_code2
from tem_ais_kafka_01_pre;

drop table if exists tem_ais_kafka_01;
create view tem_ais_kafka_01 as
select
    a.*,
    b.c_name as sea_name
from (
         select
             *,
             getSeaArea(lng,lat) as sea_id
         from tem_ais_kafka_01_pre_01
     ) a left join dim_sea_area
    FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.sea_id = b.id;

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
    c_name                                  ,
    imo                                     ,
    mmsi                                    ,
    callsign                                 ,
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
    vessel_type							    ,
    vessel_type_name                        ,
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
    sea_id,
    sea_name,
    from_unixtime(unix_timestamp()) as update_time
from tem_ais_kafka_01;


-- ais的全量状态数据入库
insert into ais_status_info_doris
select
    vessel_id      	 					    ,
    acquire_timestamp_format				,
    acquire_timestamp						,
    vessel_name								,
    c_name                                  ,
    imo                                     ,
    mmsi                                    ,
    callsign                                 ,
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
    vessel_type							    ,
    vessel_type_name                        ,
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
    sea_id,
    sea_name,
    from_unixtime(unix_timestamp()) as update_time
from tem_ais_kafka_01;

end;
