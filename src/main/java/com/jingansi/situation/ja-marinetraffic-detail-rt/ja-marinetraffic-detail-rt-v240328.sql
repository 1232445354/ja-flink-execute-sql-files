--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/3/14 11:36:13
-- description: marinetraffic的详情数据入库
--********************************************************************--
set 'pipeline.name' = 'ja-marinetraffic-detail-rt';


-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-marinetraffic-detail-rt';

set 'parallelism.default' = '1';
-- set 'execution.type' = 'streaming';
-- set 'table.planner' = 'blink';
-- set 'table.exec.state.ttl' = '600000';
-- set 'sql-client.execution.result-mode' = 'TABLEAU';


 -- -----------------------

 -- 数据结构

 -- -----------------------


-- 创建数据来源kafka映射表
drop table if exists marinetraffic_ship_info;
create table marinetraffic_ship_info(
                                        shipId                           bigint,  -- 船ID
                                        aisName                          string,  -- ais船名
                                        name                             string,  -- 船名称
                                        imo                              bigint,  -- imo
                                        eni                              string,  -- eni
                                        mmsi                             bigint,  -- mmsi
                                        length                           double,  -- 长
                                        width                            double,  -- 宽
                                        callsign                         string,  -- callsign
                                        country                          string,  -- 国家 - 英文名称
                                        countryCode                      string,  -- 国家2字代码

                                        type                             string,  -- ais船舶类型名称
                                        typeId                           string,  -- ais船舶类型id

                                        typeColorId                      string,  -- 大类型代码
                                        typeSpecificId                   string,  -- 小类型代码
                                        subtype                          string,  -- 小类型英文名称(但是种类很多，和id对不上) 原本是typeSpecific

                                        yearBuilt                        bigint,  -- 建造年份
                                        homePort                         string,  -- 母港
                                        correspondingRoamingStationId    string,  -- 对应的漫游站Id
                                        isNavigationalAid                boolean,-- 是否导航辅助
                                        serviceStatus                    string,  -- 状态
                                        aisTransponderClass              string,  -- AIS应答器类
                                        proctime             as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'marinetraffic_ship_info',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'marinetraffic_ship_info_1_idc',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


drop table if exists marinetraffic_ship_info_error;
create table marinetraffic_ship_info_error(
                                              vessel_id                        string,
                                              update_time                      string,
                                              status                           string,
                                              message                          string,
                                              proctime                         as PROCTIME(),
                                              event_time_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
                                              WATERMARK FOR event_time_ts AS event_time_ts - INTERVAL '25' SECOND
) with (
      'connector' = 'kafka',
      'topic' = 'marinetraffic_ship_info_error',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'marinetraffic_ship_info_1_idc',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1724653800000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 创建doris的映射表 - 单独的实体表
drop table if exists dwd_mtf_ship_info;
create table dwd_mtf_ship_info (
                                   ship_id                            bigint        comment '船ID',
                                   name                               string        comment '船名',
                                   name_ais                           string        comment 'ais船名',
                                   mmsi                               bigint        comment 'mmsi',
                                   imo                                bigint        comment 'imo',
                                   eni                                string        comment 'eni',
                                   callsign                           string        comment '呼号',
                                   country                            string        comment '国家英文名称',
                                   country_code                       string        comment '国家代码',
                                   type_id                            string        comment 'ais类型id',
                                   type                               string        comment 'ais类型名称',
                                   type_color_id                      string        comment '大类型代码',
                                   type_specific_id                   string        comment '小类型编码',
                                   type_specific                      string        comment '小类型英文名称',
                                   length                             double        comment '长',
                                   width                              double        comment '宽',
                                   year_built                         bigint        comment '建造年份',
                                   service_status                     string        comment '服务状态',
                                   service_status_name                string        comment '服务状态名称',
                                   ais_transponder_class              string        comment 'AIS应答器类',
                                   is_navigational_aid                int           comment '是否导航辅助',
                                   corresponding_roaming_station_id   string        comment '对应的漫游站Id',
                                   homeport                           string        comment '母港',
                                   update_time                        string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030,172.21.30.244:8030,172.21.30.246:8030',
      'table.identifier' = 'sa.dwd_mtf_ship_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'sink.batch.size'='50000',
      'sink.batch.interval'='1s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 创建doris的映射表 - 合并的实体表
drop table if exists dws_ais_vessel_detail_static_attribute;
create table dws_ais_vessel_detail_static_attribute (
                                                        vessel_id      	 			bigint		comment '船ID',
                                                        imo      					bigint      comment 'IMO',
                                                        mmsi						bigint      comment 'mmsi',
                                                        callsign 					string      comment '呼号',
                                                        year_built                	double      comment '建成年份',
                                                        vessel_type               	string	 	comment '船类型',
                                                        vessel_type_name            string      comment '船类别-小类中文',
                                                        vessel_class               	string	 	comment '船类别',
                                                        vessel_class_name           string      comment '船类型-大类中文',
                                                        `name` 						string      comment '船名',
                                                        length                   	double		comment '长度',
                                                        width                    	double      comment '宽度',
                                                        flag_country_code          	string      comment '标志国家代码',
                                                        country_name                string      comment '国家中文',
                                                        source         				string      comment '数据来源',
                                                        service_status            	string	 	comment '服务状态',
                                                        service_status_name         string      comment '服务状态中文',
                                                        update_time					string		comment '数据入库时间'

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
      'fenodes' = '172.21.30.245:8030,172.21.30.244:8030,172.21.30.246:8030',
      'table.identifier' = 'sa.dws_ais_vessel_detail_static_attribute',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'sink.batch.size'='50000',
      'sink.batch.interval'='1s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


drop table if exists dwd_vessel_mtf_error_ship_id_rt;
create table dwd_vessel_mtf_error_ship_id_rt (
                                                 vessel_id            bigint,
                                                 src_update_time      string,
                                                 status               string,
                                                 message              string,
                                                 update_time		     string
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030,172.21.30.244:8030,172.21.30.246:8030',
      'table.identifier' = 'sa.dwd_vessel_mtf_error_ship_id_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'sink.batch.size'='50000',
      'sink.batch.interval'='1s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- marinetraffic数据大类型对应关系（Source：doris）
drop table if exists dim_mtf_fm_class;
create table dim_mtf_fm_class (
                                  type_id        int,       -- marinetraffic 大类型id
                                  type_e_name    string,    -- marinetraffic 大类型英文名称
                                  type_name      string,    -- marinetraffic 大类型中文名称
                                  fm_type_id     int,       -- fleetmon 类型id
                                  fm_class_code  string,    -- fleetmon 大类型英文编码
                                  fm_class_name  string,    -- fleetmon 大类型中文编码
                                  fm_type_code   string,    -- fleetmon 小类型编码
                                  fm_type_name   string,    -- fleetmon 小类型名称
                                  priority_flag  bigint,    -- 优先级标志，如果为1 则应该取该条数据的小类型
                                  primary key (type_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030,172.21.30.244:9030,172.21.30.246:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_mtf_fm_class',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- marinetraffic数据小类型对应关系（Source：doris）
drop table if exists dim_mtf_fm_type;
create table dim_mtf_fm_type (
                                 type_specific_id      string,  -- marinetraffic 具体小类型编码
                                 type_specific_name    string,  -- marinetraffic 具体小类型名称
                                 type_id               string,  -- marinetraffic 大类型编码
                                 type_name             string,  -- marinetraffic 大类型名称
                                 fm_class_code         string,  -- fleetmon 大类型英文名称
                                 fm_class_name         string,  -- fleetmon 大类型名称
                                 fm_type_code          string,  -- fleetmon 小类型编码
                                 fm_type_name          string,  -- fleetmon 小类型名称
                                 priority_flag         bigint,   -- 优先级标志，如果为1 则应该取该条数据的大类型
                                 primary key (type_specific_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030,172.21.30.244:9030,172.21.30.246:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_mtf_fm_type',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
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
      'url' = 'jdbc:mysql://172.21.30.245:9030,172.21.30.244:9030,172.21.30.246:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_code_name_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- 船服务状态表（Source：doris）
drop table if exists dim_mtf_service_status_info;
create table dim_mtf_service_status_info (
                                             id              string  comment 'id',
                                             e_name          string  comment '服务状态英文名称',
                                             c_name          string  comment '服务状态中文名称',
                                             primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030,172.21.30.244:9030,172.21.30.246:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_mtf_service_status_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );

-- marinetraffic 最新数据状态表（Source：doris）
drop table if exists dws_vessel_list_status_rt;
create table dws_vessel_list_status_rt (
                                           vessel_id              		string					, -- 船舶的唯一标识符
                                           vessel_name            		string 					, -- 船舶的名称
                                           country_code                 	string					, -- 船舶的国家或地区旗帜标识
                                           big_type_num_code      		string					, -- 船舶类型代码--大类
                                           small_type_num_code			string					, -- 船舶类型代码--小类
                                           gt_ship_type                  string                  , -- 一个类型
                                           length               			string					, -- 船舶的长度，以米为单位
                                           width                			string					, -- 船舶的宽度，以米为单位
                                           dwt                  			string					, -- 船舶的载重吨位
                                           invalid_dimensions			string					, -- 无效_维度
                                           w_left               			string					, -- 待定--舶的左舷吃水线宽度
                                           l_fore               			string					, -- 待定--船舶的前吃水线长度
                                           primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030,172.21.30.244:9030,172.21.30.246:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_vessel_list_status_rt',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );

create function sendCacheVessel as 'com.jingan.udf.vessel.SendCacheVessel';

-- -----------------------

-- 数据处理

-- -----------------------


-- 获取不到详情的数据处理
drop view if exists tmp_marinetraffic_ship_info_error_01;
create view tmp_marinetraffic_ship_info_error_01 as
select
    cast(a.vessel_id as bigint)  as shipId          ,  -- 船ID
    vessel_name as aisName                          ,  -- ais船名
    vessel_name as name                             ,  -- 船名称
    cast(null as bigint) as imo                              ,  -- imo
    cast(null as string) as eni                              ,  -- eni
    cast(null as bigint) as mmsi                             ,  -- mmsi
    cast(length as double) as length                           ,  -- 长
    cast(width as double) as width                            ,  -- 宽
    cast(null as string) as callsign                         ,  -- callsign
    cast(null as string) as country                          ,  -- 国家 - 英文名称
    country_code as countryCode              ,  -- 国家2字代码
    cast(null as string) as type                             ,  -- ais船舶类型名称
    cast(null as string) as typeId                           ,  -- ais船舶类型id
    big_type_num_code as typeColorId         ,  -- 大类型代码
    cast(null as string) as typeSpecificId                   ,  -- 小类型代码
    cast(null as string) as subtype                          ,  -- 小类型英文名称(但是种类很多，和id对不上) 原本是typeSpecific
    cast(null as bigint) as yearBuilt                        ,  -- 建造年份
    cast(null as string) as homePort                         ,  -- 母港
    cast(null as string) as correspondingRoamingStationId    ,  -- 对应的漫游站Id
    cast(null as boolean) as isNavigationalAid                ,  -- 是否导航辅助
    cast(null as string) as serviceStatus                    ,  -- 状态
    cast(null as string) as aisTransponderClass              ,  -- AIS应答器类
    proctime
from (select vessel_id,proctime from marinetraffic_ship_info_error where status='Error') a
         left join dws_vessel_list_status_rt
    FOR SYSTEM_TIME AS OF a.proctime as b
                   on a.vessel_id = b.vessel_id;

-- 合并正常的详情和异常详情
drop view if exists tmp_marinetraffic_detail_00;
create view tmp_marinetraffic_detail_00 as
select
    *
from (
         select * from marinetraffic_ship_info
         union all
         select * from tmp_marinetraffic_ship_info_error_01
     );



drop view if exists tmp_marinetraffic_detail_01;
create view tmp_marinetraffic_detail_01 as
select
    t1.shipId as ship_id,
    t1.shipId + 1000000000 as vessel_id,
    t1.imo,
    t1.mmsi,
    t1.eni,
    t1.country,
    t1.callsign,
    t1.yearBuilt as year_built,
    t1.aisName as name_ais,
    t1.`name`,
    t1.length,
    t1.width,

    t1.typeId as type_id, -- Ais类型id
    t1.type,  -- Ais类型名称
    t1.typeColorId as type_color_id, -- 大类型id
    t1.typeSpecificId as type_specific_id, -- 小类型id,
    t1.subtype as type_specific, -- 小类型名称

    if(t1.isNavigationalAid,1,0) as  is_navigational_aid,  -- 是否导航辅助
    t1.correspondingRoamingStationId as corresponding_roaming_station_id, -- 对应的漫游站Id
    t1.aisTransponderClass as ais_transponder_class, -- AIS应答器类
    t1.homePort as homeport,                                -- 母港
    t1.serviceStatus as service_status,
    t5.c_name as service_status_name,

    if(t3.priority_flag = 1,t3.fm_class_code,t2.fm_class_code) as vessel_class, -- 大类型转换的编码
    if(t3.priority_flag = 1,t3.fm_class_name,t2.fm_class_name) as vessel_class_name, -- 大类型转换的名称
    if(t2.priority_flag = 1,t2.fm_type_code,t3.fm_type_code) as vessel_type, -- 小类型转换的编码
    if(t2.priority_flag = 1,t2.fm_type_name,t3.fm_type_name) as vessel_type_name, -- 小类型转换的民称

    t1.countryCode as country_code,
    t4.c_name as country_name

from tmp_marinetraffic_detail_00 as t1
         left join dim_mtf_fm_class   -- 大类型
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on cast(t1.typeColorId as int) = t2.type_id

         left join dim_mtf_fm_type   -- 小类型
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.typeSpecificId = t3.type_specific_id

         left join dim_country_code_name_info   -- 国家表
    FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.countryCode = t4.country_code2

         left join dim_mtf_service_status_info   -- 服务状态表
    FOR SYSTEM_TIME AS OF t1.proctime as t5
                   on t1.serviceStatus = t5.id;



-- 开窗是为了做延迟出发，缓存的数据不回漏掉
drop view if exists tmp_error_01;
create view tmp_error_01 as
select
    cast(vessel_id as bigint) as vessel_id            ,
    update_time as src_update_time      ,
    status               ,
    message              ,
    TUMBLE_START(event_time_ts, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_time_ts, INTERVAL '1' MINUTE) AS window_end
from marinetraffic_ship_info_error
group by
    vessel_id,
    update_time,
    status,
    message,
    TUMBLE(event_time_ts, INTERVAL '1' MINUTE);


-- -----------------------

-- 数据写入

-- -----------------------

begin statement set;

insert into dwd_mtf_ship_info
select
    sendCacheVessel(ship_id) as ship_id,      -- 船ID 该函数会发送缓存的数据
    name,                                     -- 船名
    name_ais,                                 -- ais船名
    mmsi,                                     -- mmsi
    imo,                                      -- imo
    eni,                                      -- eni
    callsign,                                 -- 呼号
    country,                                  -- 国家
    country_code,                             -- 国家代码
    type_id, -- ais类型id
    type, -- ais类型名称
    type_color_id,              -- 大类型代码
    type_specific_id,                         -- 小类型代码
    type_specific,                            -- 小类型英文名称(但是种类很多，和id对不上)
    length,                                   -- 长
    width,                                    -- 宽
    year_built,                               -- 建造年份
    service_status,                           -- 服务状态
    service_status_name,                      -- 服务状态名称
    ais_transponder_class,                    -- AIS应答器类
    is_navigational_aid,                      -- 是否导航辅助
    corresponding_roaming_station_id,         -- 对应的漫游站Id
    homeport,                                 -- 母港
    from_unixtime(unix_timestamp())    as  update_time  -- 数据入库时间
from tmp_marinetraffic_detail_01;



insert into dws_ais_vessel_detail_static_attribute
select
    vessel_id,
    imo,
    mmsi,
    callsign,
    year_built,
    vessel_type,
    vessel_type_name,
    vessel_class,
    vessel_class_name,
    name_ais as `name`,
    length,
    width,
    country_code as flag_country_code,
    country_name,
    '2' as source,
    service_status,
    service_status_name,
    from_unixtime(unix_timestamp()) as update_time
from tmp_marinetraffic_detail_01;

insert into dwd_vessel_mtf_error_ship_id_rt
select
    if(status='Repeat',sendCacheVessel(vessel_id),vessel_id) as vessel_id            ,
    src_update_time as src_update_time      ,
    status               ,
    message              ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_error_01;

end;






