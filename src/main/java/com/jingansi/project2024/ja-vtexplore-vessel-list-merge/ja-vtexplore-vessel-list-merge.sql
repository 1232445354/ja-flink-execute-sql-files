--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/3/22 19:52:04
-- description: vt数据入库船舶融合表
--********************************************************************--
set 'pipeline.name' = 'ja-vtexplore-vessel-list-merge';


set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '15';
SET 'sql-client.execution.result-mode'='TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '300000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-vtexplore-vessel-list-merge';
set 'execution.checkpointing.timeout' = '3600000';

-- 空闲分区不用等待
-- set 'table.exec.source.idle-timeout' = '3s';

-- set 'execution.type' = 'streaming';
-- set 'table.planner' = 'blink';
-- set 'table.exec.state.ttl' = '600000';
-- set 'sql-client.execution.result-mode' = 'TABLEAU';


-----------------------

 -- 数据结构

 -----------------------


-- 创建kafka全量vt数据来源的表（Source：kafka）
drop table if exists ais_vtexplorer_ship_list;
create table ais_vtexplorer_ship_list(
                                         MMSI                 string, -- mmsi
                                         IMO                  string, -- imo
                                         name                 string, -- 名称
                                         callsign             string, -- 呼号
                                         country              string, -- 国家
                                         is_satellite         boolean,-- 是否卫星数据
                                         type                 bigint, -- 类型代码
                                         size                 string, -- 尺寸
                                         destination_code     string, -- 目的地代码
                                         destination_name     string, -- 目的地名称
                                         longitude            double, -- 经度
                                         latitude             double, -- 纬度
                                         speed                double, -- 速度/节
                                         course               int,   -- 航向
                                         draught              double, -- 吃水
                                         ETA                  string, -- 预计到岗时间
                                         `timestamp`          bigint, -- 采集时间戳
                                         ais_time             string,
                                         proctime             as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'ais_vtexplorer_ship_list2',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-vtexplore-vessel-list-merge-idc',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 创建映射doris的全量数据表(Sink:doris)
drop table if exists dwd_ais_vessel_all_info;
create table dwd_ais_vessel_all_info(
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
                                        lng 					            double 		comment '经度',
                                        lat 					            double 		comment '纬度',
                                        source								string 		comment	'来源类型',
                                        speed								double 		comment '速度',
                                        speed_km		                    double      comment '速度 单位 km/h ',
                                        vessel_class						string		comment '船类型',
                                        vessel_class_name                   string      comment '船类型中文',
                                        vessel_type                         string      comment '船小类别',
                                        vessel_type_name                    string      comment '船类型中文名-小类',
                                        cn_iso2								string		comment '国家code',
                                        country_name                        string      comment '国家中文',
                                        position_country_code2              string      comment '位置所在的国家',
                                        friend_foe 					        string      comment '敌我类型',
                                        sea_id 					            string      comment '海域编号',
                                        sea_name 					        string      comment '中文名称',
    -- block_map_index             		bigint      comment '图层层级',
    -- block_range_x             			double      comment '块x',
    -- block_range_y              			double      comment '块y',
    -- master_image_id						bigint		comment '主图像ID',
    -- nav_status 							double 		comment '航行状态',
    -- nav_status_name                     string      comment '航行状态中文',
                                        draught								double 		comment '吃水深度',
    -- dimensions_01						double 		comment '',
    -- dimensions_02						double 		comment '',
    -- dimensions_03						double 		comment '',
    -- dimensions_04						double 		comment '',
    -- nation_flag_minio_url_jpg        string      comment '国旗',
                                        update_time             			string      comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'sa.dwd_ais_vessel_all_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='5',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='100000',
     'sink.batch.interval'='20s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 创建映射doris的状态数据表(Sink:doris)
drop table  if exists dws_ais_vessel_status_info;
create table dws_ais_vessel_status_info(
                                           vessel_id      	 				bigint		comment '船ID',
                                           acquire_timestamp_format			string      comment '时间戳格式化',
                                           acquire_timestamp					bigint		comment '时间戳',
                                           vessel_name					    string      comment '船名称',
                                           c_name                            string      comment '船中文名',
                                           imo                               string      comment 'imo',
                                           mmsi                              string      comment 'mmsi',
                                           callsign                          string      comment '呼号',
                                           rate_of_turn						double  	comment '转向率',
                                           orientation						double 		comment '方向',
                                           lng 					            double 		comment '经度',
                                           lat 					            double 		comment '纬度',
                                           source							string 		comment	'来源类型',
                                           speed								double 		comment '速度',
                                           speed_km		                    double      comment '速度 单位 km/h ',
                                           vessel_class						string		comment '船类别',
                                           vessel_class_name                 string      comment '船类型中文',
                                           vessel_type                       string      comment '船小类别',
                                           vessel_type_name                  string      comment '船类型中文名-小类',
                                           cn_iso2						    string		comment '国家code',
                                           country_name                      string      comment '国家中文',
                                           position_country_code2            string      comment '位置所在的国家',
                                           friend_foe 					    string      comment '敌我类型',
                                           sea_id 					        string      comment '海域编号',
                                           sea_name 					        string      comment '中文名称',
    -- block_map_index             	    bigint      comment '图层层级',
    -- block_range_x             		double      comment '块x',
    -- block_range_y              		double      comment '块y',
    -- master_image_id						bigint		comment '主图像ID',
    -- nav_status 							double 		comment '航行状态',
    -- nav_status_name                     string      comment '航行状态中文',
                                           draught								double 		comment '吃水深度',
    -- dimensions_01						double 		comment '',
    -- dimensions_02						double 		comment '',
    -- dimensions_03						double 		comment '',
    -- dimensions_04						double 		comment '',
    -- nation_flag_minio_url_jpg        string      comment '国旗',
                                           update_time             			string      comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'sa.dws_ais_vessel_status_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='5',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='100000',
     'sink.batch.interval'='20s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 实体详情 - 合并的实体表
drop table if exists dws_ais_vessel_detail_static_attribute;
create table dws_ais_vessel_detail_static_attribute (
                                                        vessel_id      	 			bigint		comment '船ID',
                                                        imo      					bigint      comment 'IMO',
                                                        mmsi						bigint      comment 'mmsi',
                                                        callsign 					string      comment '呼号',
                                                        `name` 						string      comment '船名',
                                                        length                   	double		comment '长度',
                                                        width                    	double      comment '宽度',
                                                        flag_country_code          	string      comment '标志国家代码',
                                                        country_name                string      comment '国家中文',
                                                        source         				string      comment '数据来源',
                                                        update_time					string		comment '数据入库时间'
    --    vessel_type               	string	 	comment '船类型',
    --  	vessel_type_name            string      comment '船类别-小类中文',
    -- vessel_class               	string	 	comment '船类别',
    --  	vessel_class_name           string      comment '船类型-大类中文',
    -- year_built                	double      comment '建成年份',
    -- service_status            	string	 	comment '服务状态',
    -- service_status_name         string      comment '服务状态中文',
    -- gross_tonnage             	double      comment '总吨位',
    -- deadweight               	double      comment '重物，载重吨位',
    -- `timestamp`					bigint 		comment '采集船只详情数据时间',
    -- height                   	double      comment '高度',
    -- draught_average           	double      comment '吃水平均值',
    -- speed_average             	double      comment '速度平均值',
    -- speed_max                 	double      comment '速度最大值',
    --  	`owner`             		string      comment '所有者',
    -- risk_rating               	string	 	comment '风险评级',
    --  	risk_rating_name            string      comment '风险评级中文',
    -- rate_of_turn 				double		comment '转向率',
    -- is_on_my_fleet				boolean		comment '是我的船队',
    -- is_on_shared_fleet			boolean		comment '是否共享船队',
    -- is_on_own_fleet				boolean 	comment '是否拥有船队',
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_ais_vessel_detail_static_attribute',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'sink.batch.size'='50000',
      'sink.batch.interval'='20s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 海域
drop table if exists dim_sea_area;
create table dim_sea_area (
                              id 			string,  -- 海域编号
                              name 		string,  -- 名称
                              c_name 		string,  -- 中文名称
                              primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_sea_area',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- 军事船舶数据
drop table if exists dws_vessle_nato_malitary;
create table dws_vessle_nato_malitary (
                                          id 			    bigint     ,
                                          navy_class 		string , -- 名称
                                          reletion        string , -- 对应的id
                                          remark          string , -- 备注 没有关联、关联上
                                          primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_vessle_nato_malitary',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- 船国家数据匹配库（Source：doris）
drop table if exists dim_vt_country_code_info;
create table dim_vt_country_code_info (
                                          id              string  comment 'id',
                                          source          string  comment '来源',
                                          e_name          string  comment '英文名称',
                                          c_name          string  comment '中文名称',
                                          vt_c_name       string  comment 'vt的中文名称',
                                          country_code2   string  comment '国家2字代码',
                                          country_code3   string  comment '国家3字代码',
                                          flag_url        string  comment '国旗url',
                                          primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_vt_country_code_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 实体信息表数据
drop table if exists dws_ais_vessel_detail_static_attribute_source;
create table dws_ais_vessel_detail_static_attribute_source (
                                                               vessel_id 	     bigint,
                                                               imo              bigint,
                                                               mmsi             bigint,
                                                               callsign         string,
                                                               name             string,
                                                               c_name           string,
                                                               vessel_type      string,
                                                               vessel_type_name string,
                                                               vessel_class     string,
                                                               vessel_class_name string,
                                                               source           string,
                                                               length           double,
                                                               width            double,
                                                               height           double,
                                                               primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_ais_vessel_detail_static_attribute',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- marinetraffic、vt的船舶对应关系
drop table if exists dim_mtf_vt_reletion_info;
create table dim_mtf_vt_reletion_info (
                                          vessel_id    string        comment '编号',
                                          vt_mmsi      string        comment 'vt数据的mmsi',
                                          merge_way    string        comment '融合方式',
                                          primary key (vt_mmsi) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_mtf_vt_reletion_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );




-- ****************************规则引擎写入数据******************************** --

drop table if exists vessel_source;
create table vessel_source(
                              id                    bigint, -- id
                              acquireTime           string, -- 采集事件年月日时分秒
                              acquireTimestamp      bigint, -- 采集时间戳
                              vesselName            string, -- 船舶名称
                              mmsi                  string, -- mmsi
                              imo                   string, -- imo
                              callsign              string, -- 呼号
                              cnIso2                string, -- 国家代码
                              countryName           string, -- 国家名称
                              source                string, -- 来源
                              vesselClass           string, -- 大类型编码
                              vesselClassName       string, -- 大类型名称
                              vesselType            string, -- 小类型编码
                              vesselTypeName        string, -- 小类型名称
                              friendFoe             string, -- 敌我代码
                              positionCountryCode2  string, -- 所处国家
                              seaId                 string, -- 海域id
                              seaName               string, -- 海域名称
                              lng                   double, -- 经度
                              lat                   double, -- 纬度
                              orientation           double, -- 方向
                              speed                 double, -- 速度 节
                              speedKm               double, -- 速度 km
                              rateOfTurn            double, -- 转向率
                              draught               double, -- 吃水
                              length                double,
                              width                 double,
                              height                double,
                              targetType            string, -- 实体类型 固定值 VESSEL
                              updateTime            string -- flink处理时间
) with (
      'connector' = 'kafka',
      'topic' = 'vessel_source',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'vessel_source_idc1',
      'key.format' = 'json',
      'key.fields' = 'id',
      'format' = 'json'
      );





-----------------------

-- 数据处理

-----------------------

-- create function getCountry as 'GetCountryFromLngLat.getCountryFromLngLat' language python ;
create function getCountry as 'com.jingan.udf.sea.GetCountryFromLngLat';
create function getSeaArea as 'com.jingan.udf.sea.GetSeaArea';



-- 关联数据 - 查看是否已经融合上的
drop table if exists tmp_marinetraffic_ship_list_01;
create view tmp_marinetraffic_ship_list_01 as
select
    t1.MMSI  as mmsi,                                                 -- mmsi
    if(t1.IMO='0',cast(null as string),t1.IMO)   as imo,                                                  -- imo
    if(t1.callsign <> '',t1.callsign,cast(null as varchar)) as callsign,-- 呼号
    t1.`timestamp` as acquire_timestamp,                              -- 采集时间
    from_unixtime(`timestamp`,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format, -- 采集时间戳格式化
    if(t1.name <> '',t1.name,cast(t1.name as varchar)) as name,        -- vt船舶名称
    -- t1.country,                                                    -- vt国家中文
    t1.longitude as lng,                                              -- 经度
    t1.latitude as lat,                                               -- 纬度
    t1.speed,                                                         -- 船舶的当前速度，以节（knots）为单位
    t1.speed * 1.852 as speed_km,                                     -- 速度km/h
    t1.course as orientation,                                         -- 船舶的当前航向
    t1.draught,                                                       -- 吃水
    t1.type,                                                          -- 船舶类型代码 - 未对应
    cast(trim(split_index(t1.`size`,'*',0)) as double) as length,     -- 船舶的长度，以米为单位
    cast(trim(split_index(t1.`size`,'*',1)) as double) as width,      -- 船舶的宽度，以米为单位
    if(t1.is_satellite,1,0) as is_satellite,                          -- 是否卫星
    t1.ETA as eta,                                                    -- 预计到岗时间
    t1.destination_code as dest_code,                                 -- 目的地代码
    t1.destination_name as dest_name,                                 -- 目的地名称
    t1.ais_time,                                                      -- ais采集时间 - 自己加的
    t1.proctime,
    t3.country_code2                 as cn_iso2,                      -- 船舶的国家或地区旗帜标识
    t3.e_name                        as country_e_name,               -- 国家英文名称
    coalesce(t3.c_name,t1.country)   as country_c_name,               -- 国家中文名称
    coalesce(cast(t2.vessel_id as bigint),cast(t1.MMSI as bigint) + 4000000000) as vessel_id,   -- 之后入库的船舶id bigint
    -- getCountry(t1.longitude,t1.latitude) as position_country_3code,  -- 计算所处国家 - 经纬度位置转换国家
    -- getSeaArea(t1.longitude,t1.latitude) as sea_id, -- 计算海域id
    -- cast(null as string) as position_country_3code,
    -- cast(null as string) as sea_id,
    t4.navy_class

from (
         select
             *
         from (
                  select
                      * -- ,
                      -- count(*) over (partition by MMSI,`timestamp` order by proctime) as cnt
                  from ais_vtexplorer_ship_list
                  where `timestamp` is not null
                  -- and unix_timestamp(ais_time)-`timestamp` <300
              ) a
-- where cnt=1
     ) t1
         left join dim_mtf_vt_reletion_info   -- VT对应关系
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.MMSI = t2.vt_mmsi

         left join dim_vt_country_code_info    -- 国家表
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.country  = t3.vt_c_name

         left join dws_vessle_nato_malitary    -- 军事船舶名单表
    FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t2.vessel_id = t4.reletion
                       and '关联上' = t4.remark;




-- 关联国家表，3字代码转换2字代码
-- 关联海域表，将海域id转换成海域名称
drop table if exists tmp_marinetraffic_ship_list_02;
create view tmp_marinetraffic_ship_list_02 as
select
    t1.vessel_id, -- bigint
    acquire_timestamp_format,
    acquire_timestamp,
    coalesce(t1.navy_class,t4.name,t1.name)          as vessel_name,
    t4.c_name                                        as c_name,
    t1.mmsi                                          as mmsi,
    coalesce(cast(t4.imo as varchar),t1.imo)         as imo,
    coalesce(t4.callsign,t1.callsign)                as callsign,
    cast(null as double) as rate_of_turn,
    orientation,
    lng,
    lat,
    t1.length,
    t1.width,
    'vt'                           as source,             -- 数据来源简称
    speed,
    draught,
    speed_km,                                              -- 速度 km/h
    t4.vessel_class,                                       -- 大类型编码
    t4.vessel_class_name,                                  -- 大类型名称
    t4.vessel_type,                                        -- 小类型编码
    t4.vessel_type_name,                                   -- 小类型名称
    t4.length as detail_length,
    t4.width as detail_width,
    t4.height as detail_height,
    cn_iso2,
    if(t1.country_c_name <> '',t1.country_c_name,cast(null as varchar)) as country_name,
    -- if(t2.country_code2 is null and ((lng between 107.491636 and 124.806089 and lat between 20.522241 and 40.799277)
    --      or (lng between 107.491636 and 121.433286 and lat between 3.011639 and 20.522241)
    --     ),'CN',t2.country_code2
    -- ) as position_country_code2,      -- 经纬度位置转换国家

    case
        when cn_iso2 in('IN','US','JP','AU') and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR') then 'ENEMY' -- 敌 - 美印日澳 军事
        when cn_iso2 ='CN' and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then 'OUR_SIDE'               -- 我 中国 军事
        when cn_iso2 ='CN' and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then 'FRIENDLY_SIDE'      -- 友 中国 非军事
        else 'NEUTRALITY'
        end as friend_foe,     -- 敌我
    proctime,
    -- sea_id,
    -- t3.c_name as sea_name,
    t4.vessel_id as t4_vessel_id

from tmp_marinetraffic_ship_list_01 t1
         -- left join dim_vt_country_code_info
         -- FOR SYSTEM_TIME AS OF t1.proctime as t2
         -- on t1.position_country_3code = t2.country_code3

         -- left join dim_sea_area   -- 海域名称
         -- FOR SYSTEM_TIME AS OF t1.proctime as t3
         -- on t1.sea_id = t3.id

         left join dws_ais_vessel_detail_static_attribute_source   -- 实体信息
    FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.vessel_id = t4.vessel_id;



drop view if exists tmp_marinetraffic_ship_list_03;
create view tmp_marinetraffic_ship_list_03 as
select
    *,
    proctime,
    getCountry(t1.lng,t1.lat) as position_country_3code,  -- 计算所处国家 - 经纬度位置转换国家
    getSeaArea(t1.lng,t1.lat) as sea_id -- 计算海域id
from tmp_marinetraffic_ship_list_02 t1;




drop view if exists tmp_marinetraffic_ship_list_04;
create view tmp_marinetraffic_ship_list_04 as
select
    *,
    t3.c_name as sea_name,
    if(t2.country_code2 is null and ((lng between 107.491636 and 124.806089 and lat between 20.522241 and 40.799277)
        or (lng between 107.491636 and 121.433286 and lat between 3.011639 and 20.522241)
        ),'CN',t2.country_code2
        ) as position_country_code2      -- 经纬度位置转换国家
from tmp_marinetraffic_ship_list_03 t1
         left join dim_vt_country_code_info
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.position_country_3code = t2.country_code3

         left join dim_sea_area   -- 海域名称
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.sea_id = t3.id;

-----------------------

-- 数据插入

-----------------------

begin statement set;

-- ais的全量数据入库
insert into dwd_ais_vessel_all_info
select
    vessel_id      	 					    ,
    acquire_timestamp_format				,
    acquire_timestamp						,
    vessel_name								,
    c_name                                  ,
    imo                                     ,
    mmsi                                    ,
    callsign                                ,
    rate_of_turn							,
    orientation								,
    lng 						            ,
    lat 						            ,
    source									,
    speed									,
    speed_km                                ,
    vessel_class							,
    vessel_class_name                       ,
    vessel_type							    ,
    vessel_type_name                        ,
    cn_iso2									,
    country_name                            ,
    position_country_code2                  ,
    friend_foe                              ,
    sea_id,
    sea_name,
    draught,
    from_unixtime(unix_timestamp()) as update_time
from tmp_marinetraffic_ship_list_04;


-- ais的全量状态数据入库
insert into dws_ais_vessel_status_info
select
    vessel_id      	 					    ,
    acquire_timestamp_format				,
    acquire_timestamp						,
    vessel_name								,
    c_name                                  ,
    imo                                     ,
    mmsi                                    ,
    callsign                                ,
    rate_of_turn							,
    orientation								,
    lng 						            ,
    lat 						            ,
    source									,
    speed									,
    speed_km                                ,
    vessel_class							,
    vessel_class_name                       ,
    vessel_type							    ,
    vessel_type_name                        ,
    cn_iso2									,
    country_name                            ,
    position_country_code2                  ,
    friend_foe                              ,
    sea_id,
    sea_name,
    draught,
    from_unixtime(unix_timestamp()) as update_time
from tmp_marinetraffic_ship_list_04;



insert into dws_ais_vessel_detail_static_attribute
select
    vessel_id,
    cast(imo as bigint) as imo,
    cast(mmsi as bigint) as mmsi,
    callsign,
    vessel_name as name,
    length,
    width,
    cn_iso2 as flag_country_code,
    country_name,
    source,
    from_unixtime(unix_timestamp()) as update_time
from tmp_marinetraffic_ship_list_02
where t4_vessel_id is null;




-- ****************************规则引擎写入数据******************************** --

insert into vessel_source
select
    vessel_id      	       as id,
    acquire_timestamp_format as acquireTime,
    acquire_timestamp		   as acquireTimestamp,
    vessel_name	           as vesselName,
    mmsi                     as mmsi,
    imo                      as imo,
    callsign,
    cn_iso2				   as cnIso2,
    country_name             as countryName,
    source,
    vessel_class			   as vesselClass,
    vessel_class_name        as vesselClassName,
    vessel_type			   as vesselType,
    vessel_type_name         as vesselTypeName,
    friend_foe               as friendFoe,
    position_country_code2   as positionCountryCode2,
    sea_id                   as seaId,
    sea_name                 as seaName,
    lng,
    lat,
    orientation,
    speed,
    speed_km,
    rate_of_turn            as rateOfTurn,
    draught,
    coalesce(detail_length,length)    as length,
    coalesce(detail_width,width)      as  width,
    detail_height                     as height,
    'VESSEL'                          as targetType,
    from_unixtime(unix_timestamp()) as updateTime
from tmp_marinetraffic_ship_list_04;



end;




