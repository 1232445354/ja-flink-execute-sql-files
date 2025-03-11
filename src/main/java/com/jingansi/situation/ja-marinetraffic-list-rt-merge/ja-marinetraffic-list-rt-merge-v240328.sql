--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/3/1 16:47:59
-- description: marinetraffic的采集
--********************************************************************--

set 'pipeline.name' = 'ja-marinetraffic-list-rt-merge';


set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '10';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '300000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-marinetraffic-list-rt-merge';

-- 空闲分区不用等待
-- set 'table.exec.source.idle-timeout' = '3s';

-- set 'execution.type' = 'streaming';
-- set 'table.planner' = 'blink';
-- set 'table.exec.state.ttl' = '600000';
-- set 'sql-client.execution.result-mode' = 'TABLEAU';


-----------------------

 -- 数据结构

 -----------------------


-- 创建kafka全量marineTraffic数据来源的表（Source：kafka）
create table marinetraffic_ship_list(
                                        SHIP_ID              string, -- 船舶的唯一标识符
                                        SHIPNAME             string, -- 船舶的名称
                                        SPEED                string, -- 船舶的当前速度，以节（knots）为单位 10倍
                                        ROT                  string, -- 船舶的旋转率，转向率
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
                                        INVALID_DIMENSIONS   string,    -- 无效_维度
                                        LENGTH               string, -- 船舶的长度，以米为单位
                                        WIDTH                string, -- 船舶的宽度，以米为单位。
                                        DWT                  string, -- 船舶的载重吨位
                                        block_map_index      bigint, -- 地图分层
                                        block_range_x        bigint, -- x块
                                        block_range_y        bigint, -- y块
                                        typeFlag             string, -- 回流的类型标志
                                        proctime             as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'marinetraffic_ship_list',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'marinetraffic_ship_list_idc',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1738796400000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 新入的船舶没有详情的id打入kafka
drop table if exists marinetraffic_no_detail_list;
create table marinetraffic_no_detail_list(
                                             vessel_id            string, -- 船舶id
                                             update_time          string,  -- 时间
                                             primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'upsert-kafka',
      'topic' = 'marinetraffic_no_detail_list',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'marinetraffic_ship_list_3',
      'key.format' = 'json',
      'value.format' = 'json'
      );



-- 船国家数据匹配库（Source：doris）
drop table if exists dim_country_code_name_info;
create table dim_country_code_name_info (
                                            id              string  comment 'id',
                                            source          string  comment '来源',
                                            e_name          string  comment '英文名称',
                                            c_name          string  comment '中文名称',
                                            country_code2   string  comment '国家2字代码',
                                            country_code3   string  comment '国家3字代码',
                                            flag_url        string  comment '国旗url',
                                            primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_code_name_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- fleetmon、marinetraffic的船舶对应关系
create table dim_mt_fm_id_relation (
                                       ship_id                        bigint        comment '船编号',
                                       vessel_id                      bigint        comment '编号',
    -- vessel_type                    string        comment 'FleetMon 船大类',
    -- vessel_type_name               string        comment 'FleetMon 船大类中文',
    -- vessel_class                   string        comment 'FleetMon 船小类',
    -- vessel_class_name              string        comment 'FleetMon 船小类中文',
    -- c_name                         string        comment '船中文名',
    -- imo                            string        comment 'imo',
    -- mmsi                           bigint        comment 'mmsi',
    -- callsign                       string        comment '呼号',
                                       primary key (ship_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_mt_fm_id_relation',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
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
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- 实体信息表数据
drop table if exists dws_ais_vessel_detail_static_attribute;
create table dws_ais_vessel_detail_static_attribute (
                                                        vessel_id 	       bigint,
                                                        imo                bigint,
                                                        mmsi               bigint,
                                                        callsign           string,
                                                        name               string,
                                                        c_name             string,
                                                        vessel_type        string,
                                                        vessel_type_name   string,
                                                        vessel_class       string,
                                                        vessel_class_name  string,
                                                        flag_country_code  string,
                                                        country_name       string,
                                                        source             string,
                                                        length             double,
                                                        width              double,
                                                        height             double,
                                                        primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_ais_vessel_detail_static_attribute',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );

-- mtf 的船舶信息表
drop table if exists dwd_mtf_ship_info;
create table dwd_mtf_ship_info (
                                   ship_id                      bigint        comment '船ID',
                                   mmsi                         bigint        comment 'mmsi',
                                   imo                          bigint        comment 'imo',
                                   callsign                     string        comment '呼号',
                                   primary key (ship_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dwd_mtf_ship_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '500000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );


-- ****************************数据单独入库******************************** --

-- 创建写入doris全量数据表（Sink：doris）
drop table if exists dwd_vessel_list_all_rt;
create table dwd_vessel_list_all_rt (
                                        vessel_id              			string					, -- 船舶的唯一标识符
                                        acquire_timestamp_format		string    			    , -- 采集时间戳格式化
                                        acquire_timestamp          		bigint				    , -- 采集时间戳
                                        vessel_name            			string 					, -- 船舶的名称
                                        speed                		    string					, -- 船舶的当前速度，以节（knots）为单位 10倍
                                        destination          			string					, -- 船舶的目的地
                                        country_code                 	string					, -- 船舶的国家或地区旗帜标识
                                        country_name                 	string					, -- 船舶国家中文
                                        lng                  			double					, -- 船舶当前位置的经度值
                                        lat                  			double					, -- 船舶当前位置的经度值
                                        elapsed              			string					, -- 自上次位置报告以来经过的时间，以分钟为单位
                                        course               			int					    , -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方
                                        big_type_num_code      			string					, -- 船舶类型代码--大类
                                        small_type_num_code				string					, -- 船舶类型代码--小类
                                        gt_ship_type                    string                  , -- 一个类型
                                        heading              			string					, -- 船舶的船首朝向
                                        length               			string					, -- 船舶的长度，以米为单位
                                        width                			string					, -- 船舶的宽度，以米为单位
                                        dwt                  			string					, -- 船舶的载重吨位
                                        invalid_dimensions				string					, -- 无效_维度
                                        rot                  		    string					, -- 船舶的旋转率
                                        w_left               			string					, -- 待定--舶的左舷吃水线宽度
                                        l_fore               			string					, -- 待定--船舶的前吃水线长度
                                        block_map_index                 bigint                  , -- 地图层级
                                        block_range_x                   bigint                  , -- 块x
                                        block_range_y                   bigint                  , -- 块y
                                        update_time                     string  				  -- 数据入库时间

) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.244:8030,172.21.30.245:8030,172.21.30.246:8030',
      'table.identifier' = 'sa.dwd_vessel_list_all_rt',
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



-- 创建写入doris状态数据表（Sink：doris）
drop table if exists dws_vessel_list_status_rt;
create table dws_vessel_list_status_rt (
                                           vessel_id              			string					, -- 船舶的唯一标识符
                                           acquire_timestamp_format		string    			    , -- 采集时间戳格式化
                                           acquire_timestamp          		bigint				    , -- 采集时间戳
                                           vessel_name            			string 					, -- 船舶的名称
                                           speed                		    string					, -- 船舶的当前速度，以节（knots）为单位 10倍
                                           destination          			string					, -- 船舶的目的地
                                           country_code                 	string					, -- 船舶的国家或地区旗帜标识
                                           country_name                 	string					, -- 船舶国家中文
                                           lng                  			double					, -- 船舶当前位置的经度值
                                           lat                  			double					, -- 船舶当前位置的经度值
                                           elapsed              			string					, -- 自上次位置报告以来经过的时间，以分钟为单位
                                           course               			int					    , -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方
                                           big_type_num_code      			string					, -- 船舶类型代码--大类
                                           small_type_num_code				string					, -- 船舶类型代码--小类
                                           gt_ship_type                    string                  , -- 一个类型
                                           heading              			string					, -- 船舶的船首朝向
                                           length               			string					, -- 船舶的长度，以米为单位
                                           width                			string					, -- 船舶的宽度，以米为单位
                                           dwt                  			string					, -- 船舶的载重吨位
                                           invalid_dimensions				string					, -- 无效_维度
                                           rot                  		    string					, -- 船舶的旋转率
                                           w_left               			string					, -- 待定--舶的左舷吃水线宽度
                                           l_fore               			string					, -- 待定--船舶的前吃水线长度
                                           update_time                     string  				 -- 数据入库时间

) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.244:8030,172.21.30.245:8030,172.21.30.246:8030',
      'table.identifier' = 'sa.dws_vessel_list_status_rt',
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

-- 缓存表
drop table if exists dwd_vessel_mtf_cache_list_rt;
create table dwd_vessel_mtf_cache_list_rt (
                                              SHIP_ID              string, -- 船舶的唯一标识符
                                              SHIPNAME             string, -- 船舶的名称
                                              SPEED                string, -- 船舶的当前速度，以节（knots）为单位 10倍
                                              ROT                  string, -- 船舶的旋转率，转向率
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
                                              INVALID_DIMENSIONS   string,    -- 无效_维度
                                              LENGTH               string, -- 船舶的长度，以米为单位
                                              WIDTH                string, -- 船舶的宽度，以米为单位。
                                              DWT                  string, -- 船舶的载重吨位
                                              block_map_index      bigint, -- 地图分层
                                              block_range_x        bigint, -- x块
                                              block_range_y        bigint -- y块
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.244:8030,172.21.30.245:8030,172.21.30.246:8030',
      'table.identifier' = 'sa.dwd_vessel_mtf_cache_list_rt',
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



-- ****************************规则引擎写入数据******************************** --

drop table if exists vessel_source;
create table vessel_source(
                              id                    bigint, -- id
                              source                string, -- 来源
                              acquireTime           string, -- 采集事件年月日时分秒
                              acquireTimestamp      bigint, -- 采集时间戳
                              vesselName            string, -- 船舶名称,写出给规则引擎
                              cName                 string, -- 船舶中文名称
                              mmsi                  string, -- mmsi
                              imo                   string, -- imo
                              callsign              string, -- 呼号
                              cnIso2                string, -- 国家代码
                              countryName           string, -- 国家名称
                              vesselClass           string, -- 大类型编码
                              vesselClassName       string, -- 大类型名称
                              vesselType            string, -- 小类型编码
                              vesselTypeName        string, -- 小类型名称
                              friendFoe             string, -- 敌我代码
                              positionCountryCode2  string, -- 所处国家
                              seaId                 string, -- 海域id
                              seaName               string, -- 海域名称
                              navStatus             string, -- 航行状态
                              navStatusName         string, -- 航行状态名称
                              lng                   double, -- 经度
                              lat                   double, -- 纬度
                              orientation           double, -- 方向
                              speed                 double, -- 速度 节
                              speedKm               double, -- 速度 km
                              rateOfTurn            double, -- 转向率
                              draught               double, -- 吃水
                              length                double, -- 长度
                              width                 double, -- 宽度
                              height                double, -- 高度
                              sourceShipname        string,
                              sourceCountryCode     string,
                              sourceCountryName     string,
                              sourceMmsi            string,
                              sourceImo             string,
                              sourceCallsign        string,
                              sourceVesselClass     string,
                              sourceVesselClassName string,
                              sourceVesselType      string,
                              sourceVesselTypeName  string,
                              sourceLength          double,
                              sourceWidth           double,
                              sourceHeight          double,
    -- masterImageId         bigint, -- 图像id
    -- dimensions01          double,
    -- dimensions02          double,
    -- dimensions03          double,
    -- dimensions04          double,
                              blockMapIndex         bigint, -- 图层层级
                              blockRangeX           bigint, -- 块x
                              blockRangeY           bigint, -- 块y
                              targetType            string, -- 实体类型 固定值 VESSEL
                              ex_info               map<String,String>, -- 扩展信息
                              updateTime            string -- flink处理时间
) with (
      'connector' = 'kafka',
      'topic' = 'vessel_source',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'marinetraffic_vessel_source_sink',
      'key.format' = 'json',
      'key.fields' = 'mmsi',
      'format' = 'json'
      );




-----------------------

-- 数据处理

-----------------------

create function getCountry as 'com.jingan.udf.sea.GetCountryFromLngLat';
create function getSeaArea as 'com.jingan.udf.sea.GetSeaArea';

-- create function passThrough as 'com.jingan.udtf.PassThroughUdtf';


drop view if exists tmp_mtf_list;
create view tmp_mtf_list as
select
    a.*,
    cast(LON as double) as lon_double,
    cast(LAT as double) as lat_double,
    cast(SHIP_ID as bigint) as ship_id_bigint,
    b.ship_id as b_ship_id,
    b.mmsi as mmsi,
    b.imo as imo,
    b.callsign as callsign
from marinetraffic_ship_list a
         left join dwd_mtf_ship_info
    FOR SYSTEM_TIME AS OF a.proctime as b
                   on cast(a.SHIP_ID as bigint) = b.ship_id
where `timeStamp` is not null
  and ELAPSED < 60
  and CHAR_LENGTH(SHIP_ID) <= 30;


-- 1.关联fleetmon表、2.关联国家名称表、3.整理字段、4.计算国家海域
drop table if exists tmp_marinetraffic_ship_list_01;
create view tmp_marinetraffic_ship_list_01 as
select
    t1.SHIP_ID                                as ship_id,              -- 船舶的唯一标识符
    t1.SHIPNAME                               as shipname,             -- 船舶的名称
    t1.`timeStamp`                            as acquire_timestamp,    -- 采集时间
    t1.ELAPSED                                as elapsed,               -- 自上次位置报告以来经过的时间，以分钟为单位。
    from_unixtime(`timeStamp`-(cast(ELAPSED as int)*60),'yyyy-MM-dd HH:mm:01') as acquire_timestamp_format, -- 数据产生时间，爬虫采集的时间减 ELAPSED 经过的时间 ，格式化到分钟
    cast(t1.ROT as double)                    as rate_of_turn,         -- 船舶的旋转率，转向率
    cast(t1.COURSE as int)                    as orientation,          -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方，
    t1.lon_double                             as lng,                  -- 船舶当前位置的经度值
    t1.lat_double                             as lat,                  -- 船舶当前位置的纬度值
    t1.SPEED                                  as source_speed,         -- 为了直接入库的
    cast(t1.SPEED as double)/10               as speed,                -- 船舶的当前速度，以节（knots）为单位 10倍
    t1.FLAG                                   as cn_iso2,              -- 船舶的国家或地区旗帜标识
    t1.GT_SHIPTYPE                            as gt_shiptype,          -- 船舶的全球船舶类型码（小类）
    t1.SHIPTYPE                               as shiptype,             -- 船舶的类型码，表示船舶所属的船舶类型 （大类）
    t1.HEADING                                as heading,              -- 船舶的船首朝向
    t1.LENGTH                                 as length,               -- 船舶的长度，以米为单位
    t1.WIDTH                                  as width,                -- 船舶的宽度，以米为单位
    t1.DWT                                    as dwt,                  -- 船舶的载重吨位
    t1.DESTINATION                            as destination,          -- 船舶的目的地
    t1.W_LEFT                                 as w_left,               -- 舶的左舷吃水线宽度
    t1.L_FORE                                 as l_fore,               -- 船舶的前吃水线长度
    t1.INVALID_DIMENSIONS                     as invalid_dimensions,
    t1.ROT                                    as rot,
    t1.block_map_index,
    t1.block_range_x,
    t1.block_range_y,
    t2.vessel_id                      as fleetmon_vessel_id,
    t3.e_name                         as country_e_name,
    t3.c_name                         as country_c_name,
    coalesce(t2.vessel_id,ship_id_bigint + 1000000000) as vessel_id,
    getCountry(lon_double,lat_double) as position_country_3code,  -- 计算所处国家 - 经纬度位置转换国家
    getSeaArea(lon_double,lat_double) as sea_id, -- 计算海域id
    t1.mmsi as mmsi,
    t1.imo as imo,
    t1.callsign as callsign,
    MAP['SHIP_ID', SHIP_ID,
    'W_LEFT', W_LEFT,
    'L_FORE', L_FORE,
    'DESTINATION', DESTINATION,
    'ELAPSED', ELAPSED,
    'GT_SHIPTYPE', GT_SHIPTYPE,
    'SHIPTYPE', SHIPTYPE,
    'HEADING', HEADING,
    'INVALID_DIMENSIONS', INVALID_DIMENSIONS,
    'DWT', DWT
    ]   as ex_info,
    t1.proctime
from tmp_mtf_list t1
  left join dim_mt_fm_id_relation   -- fleetmon对应关系
    FOR SYSTEM_TIME AS OF t1.proctime as t2
    on ship_id_bigint = t2.ship_id

  left join dim_country_code_name_info    -- 国家表
    FOR SYSTEM_TIME AS OF t1.proctime as t3
      on t1.FLAG  = t3.country_code2
      and 'COMMON' = t3.source
where b_ship_id is not null -- 关联上的或者是回流的数据入库
   or typeFlag='reissue' ;



-- 1.关联实体表 2.关联国家表取出所处国家二字代码 3. 关联海域表取海域名称 4. 整合字段
drop view if exists tmp_marinetraffic_ship_list_02;
create view tmp_marinetraffic_ship_list_02 as
select
    t1.vessel_id                     as id,
    acquire_timestamp_format         as acquireTime,
    acquire_timestamp                as acquireTimestamp,
    coalesce(t4.name,t1.shipname)    as vesselName,
    t4.c_name                        as cName,
    cast(coalesce(t1.imo, t4.imo) as varchar)          as imo,
    cast(coalesce(t1.mmsi, t4.mmsi) as varchar)        as mmsi,
    coalesce(t1.callsign, t4.callsign) as callsign,
    coalesce(t4.flag_country_code,t1.cn_iso2)      as cnIso2,
    coalesce(t4.country_name,t1.country_c_name)    as countryName,
    '2'                              as source,
    t4.vessel_class                  as vesselClass,     -- 大类型编码
    t4.vessel_class_name             as vesselClassName, -- 大类型名称
    t4.vessel_type                   as vesselType,      -- 小类型编码
    t4.vessel_type_name              as vesselTypeName,  -- 小类型名称
    sea_id                           as seaId,
    t3.c_name                        as seaName,
    cast(null as varchar)            as navStatus,
    cast(null as varchar)            as navStatusName,
    lng,
    lat,
    orientation,
    speed,
    speed * 1.852                    as speedKm,-- 速度 km/h
    rate_of_turn                     as rateOfTurn,
    cast(null as double)             as draught,
    coalesce(t4.length,cast(t1.length as double))    as length,
    coalesce(t4.width,cast(t1.width as double))      as width,
    t4.height                        as height,
    block_map_index                  as blockMapIndex,
    block_range_x                    as blockRangeX,
    block_range_y                    as blockRangeY,
    t2.country_code2 as positionCountry2code,
    t4.vessel_id as t4_vessel_id,
    t1.shipname                     as sourceShipname,
    t1.cn_iso2                      as sourceCountryCode,
    t1.country_c_name               as sourceCountryName,
    cast(t1.mmsi as varchar)           as sourceMmsi,
    cast(t1.imo as varchar)           as sourceImo,
    cast(t1.callsign as varchar)           as sourceCallsign,
    cast(null as varchar)           as sourceVesselClass,
    cast(null as varchar)           as sourceVesselClassName,
    cast(null as varchar)           as sourceVesselType,
    cast(null as varchar)           as sourceVesselTypeName,
    cast(t1.length as double)       as sourceLength,
    cast(t1.width as double)        as sourceWidth,
    cast(null as double)            as sourceHeight,
    case
        when cn_iso2 in('IN','US','JP','AU') and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR') then 'ENEMY' -- 敌 - 美印日澳 军事
        when cn_iso2 ='CN' and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then 'OUR_SIDE'               -- 我 中国 军事
        when cn_iso2 ='CN' and t4.vessel_type in ('PTA','FRT','SRV','CRO','AMT','FPS','MOU','DMN','SMN','PTH','ICN','ESC','LCR','VDO','CGT','COR','DES','AMR')  then 'FRIENDLY_SIDE'      -- 友 中国 非军事
        else 'NEUTRALITY'
        end as friendFoe,

    if(t2.country_code2 is null
           and ((lng between 107.491636 and 124.806089 and lat between 20.522241 and 40.799277)
            or (lng between 107.491636 and 121.433286 and lat between 3.011639 and 20.522241)
           ),'CN',t2.country_code2
        ) as positionCountryCode2,
    ex_info

from tmp_marinetraffic_ship_list_01 t1
         left join dws_ais_vessel_detail_static_attribute  FOR SYSTEM_TIME AS OF t1.proctime as t4  -- 实体信息
                   on t1.vessel_id = t4.vessel_id

         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.position_country_3code = t2.country_code3

         left join dim_sea_area
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.sea_id = t3.id;




-----------------------

-- 数据插入

-----------------------

begin statement set;



-- 数据直接入库
insert into dwd_vessel_list_all_rt
select
    ship_id                  as vessel_id,
    from_unixtime(`acquire_timestamp`,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    acquire_timestamp,
    shipname          as vessel_name,
    source_speed      as speed,
    destination,
    cn_iso2           as country_code,
    country_c_name    as country_name,
    lng,
    lat,
    elapsed,
    orientation       as course,
    shiptype          as big_type_num_code,
    gt_shiptype       as small_type_num_code,
    gt_shiptype,
    heading,
    length,
    width,
    dwt,
    invalid_dimensions,
    rot,
    w_left,
    l_fore,
    block_map_index      , -- 地图层级
    block_range_x        , -- 块x
    block_range_y        , -- 块y
    from_unixtime(unix_timestamp()) as update_time
from tmp_marinetraffic_ship_list_01;



-- 数据直接入库
insert into dws_vessel_list_status_rt
select
    ship_id                  as vessel_id,
    from_unixtime(`acquire_timestamp`,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    acquire_timestamp,
    shipname          as vessel_name,
    source_speed      as speed,
    destination,
    cn_iso2           as country_code,
    country_c_name    as country_name,
    lng,
    lat,
    elapsed,
    orientation       as course,
    shiptype          as big_type_num_code,
    gt_shiptype       as small_type_num_code,
    gt_shiptype,
    heading,
    length,
    width,
    dwt,
    invalid_dimensions,
    rot,
    w_left,
    l_fore,
    from_unixtime(unix_timestamp()) as update_time
from tmp_marinetraffic_ship_list_01;


-- 没有实体详情的入kafka
-- insert into marinetraffic_no_detail_list
-- select
--     cast(id - 1000000000 as varchar) as vessel_id,
--     from_unixtime(unix_timestamp()) as update_time
-- from tmp_marinetraffic_ship_list_02
-- where t4_vessel_id is null
--   and id > 1000000000;

insert into marinetraffic_no_detail_list
select
    SHIP_ID as vessel_id,
    from_unixtime(unix_timestamp()) as update_time
from tmp_mtf_list
where b_ship_id is null  -- 没关联上详情并且不是回流的数据
  and typeFlag is null ;

-- ****************************规则引擎写入数据******************************** --
insert into vessel_source
select
    id,
    source,
    acquireTime,
    acquireTimestamp,
    vesselName,
    cName,
    mmsi,
    imo,
    callsign,
    cnIso2,
    countryName,
    vesselClass,
    vesselClassName,
    vesselType,
    vesselTypeName,
    friendFoe,
    positionCountryCode2,
    seaId,
    seaName,
    navStatus,
    navStatusName,
    lng,
    lat,
    orientation,
    speed,
    speedKm,
    rateOfTurn,
    draught,
    length,
    width,
    height,
    sourceShipname,
    sourceCountryCode,
    sourceCountryName,
    sourceMmsi,
    sourceImo,
    sourceCallsign,
    sourceVesselClass,
    sourceVesselClassName,
    sourceVesselType,
    sourceVesselTypeName,
    sourceLength,
    sourceWidth,
    sourceHeight,
    blockMapIndex,
    blockRangeX,
    blockRangeY,
    'VESSEL'                        as targetType,
    ex_info,
    from_unixtime(unix_timestamp()) as updateTime
from tmp_marinetraffic_ship_list_02
where t4_vessel_id is not null ;


insert into dwd_vessel_mtf_cache_list_rt
select
    SHIP_ID              , -- 船舶的唯一标识符
    SHIPNAME             , -- 船舶的名称
    SPEED                , -- 船舶的当前速度，以节（knots）为单位 10倍
    ROT                  , -- 船舶的旋转率，转向率
    W_LEFT               , -- 舶的左舷吃水线宽度
    L_FORE               , -- 船舶的前吃水线长度
    `timeStamp`          , -- 采集时间
    DESTINATION          , -- 船舶的目的地
    LON                  , -- 船舶当前位置的经度值
    ELAPSED              , -- 自上次位置报告以来经过的时间，以分钟为单位。
    COURSE               , -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方，
    GT_SHIPTYPE          , -- 船舶的全球船舶类型码。
    FLAG                 , -- 船舶的国家或地区旗帜标识。
    LAT                  , -- 船舶当前位置的经度值。
    SHIPTYPE             , -- 船舶的类型码，表示船舶所属的船舶类型。
    HEADING              , -- 船舶的船首朝向
    INVALID_DIMENSIONS   ,    -- 无效_维度
    LENGTH               , -- 船舶的长度，以米为单位
    WIDTH                , -- 船舶的宽度，以米为单位。
    DWT                  , -- 船舶的载重吨位
    block_map_index      , -- 地图分层
    block_range_x        , -- x块
    block_range_y          -- y块
from tmp_mtf_list
where b_ship_id is null -- 没关联上详情并且不是回流的数据
  and typeFlag is null ;

end;
