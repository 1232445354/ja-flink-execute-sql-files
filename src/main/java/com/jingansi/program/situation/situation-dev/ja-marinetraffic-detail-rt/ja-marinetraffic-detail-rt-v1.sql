--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/3/14 11:36:13
-- description: marinetraffic的详情数据入库
-- version: v0.0.1.240314
--********************************************************************--
set 'pipeline.name' = 'ja-marinetraffic-detail-rt';


-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '100000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-marinetraffic-detail-rt';

-- set 'parallelism.default' = '2';
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
                                        ship_id                          bigint,  -- 船ID
                                        nameAis                          string,  -- ais船名
                                        name                             string,  -- 船名称
                                        type                             string,  -- 大类型英文名称
                                        typeColor                        string,  -- 大类型代码
                                        typeSpecific                     string,  -- 小类型英文名称
                                        typeSpecificId                   string,  -- 小类型代码
                                        mmsi                             bigint,  -- mmsi
                                        callsign                         string,  -- callsign
                                        imo                              bigint,  -- imo
                                        eni                              string,  -- eni
                                        country                          string,  -- 国家 - 英文名称
                                        countryCode                      string,  -- 国家2字代码
                                        status                           string,  -- 状态
                                        grossTonnage                     double,  -- 总吨位
                                        liquidGas                        double,  -- 液化气
                                        yearBuilt                        bigint,  -- 建造年份
                                        deadweight                       bigint,  -- 载重吨位
                                        correspondingRoamingStationId    string,  -- 对应的漫游站Id
                                        teu                              string,  -- 标准箱
                                        length                           double,  -- 长
                                        breadth                          double,  -- 宽
                                        homeport                         string,  -- 母港
                                        isNavigationalAid                boolean,-- 是否导航辅助
                                        proctime             as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'marinetraffic_ship_info',
      'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
      'properties.group.id' = 'marinetraffic_ship_info_1',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
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
                                   country                            string        comment '国家',
                                   country_code                       string        comment '国家代码',
                                   type_specific_id                   string        comment '具体类型编码',
                                   type                               string        comment '类型',
                                   type_specific                      string        comment '具体类型',
                                   type_color                         string        comment '类型颜色',
                                   gross_tonnage                      double        comment '总吨位',
                                   deadweight                         double        comment '载重吨位',
                                   teu                                string        comment '标准箱',
                                   liquid_gas                         double        comment '液化气',
                                   length                             double        comment '长',
                                   breadth                            double        comment '宽',
                                   year_built                         bigint        comment '建造年份',
                                   status                             string        comment '状态',
                                   is_navigational_aid                int           comment '是否导航辅助',
                                   corresponding_roaming_station_id   string        comment '对应的漫游站Id',
                                   homeport                           string        comment '母港',
                                   update_time                        string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'table.identifier' = 'sa.dwd_mtf_ship_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'flase',
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
                                                        gross_tonnage             	double      comment '总吨位',
                                                        deadweight               	double      comment '重物，载重吨位',
                                                        source         				string      comment '数据来源',
                                                        update_time					string		comment '数据入库时间'
    -- `timestamp`					bigint 		comment '采集船只详情数据时间',
    -- height                   	double      comment '高度',
    -- draught_average           	double      comment '吃水平均值',
    -- speed_average             	double      comment '速度平均值',
    -- speed_max                 	double      comment '速度最大值',
    --  	`owner`             		string      comment '所有者',
    -- risk_rating               	string	 	comment '风险评级',
    --  	risk_rating_name            string      comment '风险评级中文',
    -- service_status            	string	 	comment '服务状态',
    -- service_status_name         string      comment '服务状态中文',
    -- rate_of_turn 				double		comment '转向率',
    -- is_on_my_fleet				boolean		comment '是我的船队',
    -- is_on_shared_fleet			boolean		comment '是否共享船队',
    -- is_on_own_fleet				boolean 	comment '是否拥有船队',
) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'table.identifier' = 'sa.dws_ais_vessel_detail_static_attribute',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'flase',
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
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_mtf_fm_class',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
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
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_mtf_fm_type',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
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
      'url' = 'jdbc:mysql://doris-fe-service.bigdata-doris.svc.cluster.local:8888/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_code_name_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );



-- -----------------------

-- 数据处理

-- -----------------------
drop view if exists tmp_marinetraffic_detail_01;
create view tmp_marinetraffic_detail_01 as
select
        t1.ship_id + 1000000000 as vessel_id,
        t1.imo,
        t1.mmsi,
        t1.callsign,
        t1.yearBuilt as year_built,

        if(t3.priority_flag = 1,t3.fm_class_code,t2.fm_class_code) as vessel_class, -- 大类型转换的编码
        if(t3.priority_flag = 1,t3.fm_class_name,t2.fm_class_name) as vessel_class_name, -- 大类型转换的名称
        if(t2.priority_flag = 1,t2.fm_type_code,t3.fm_type_code) as vessel_type, -- 小类型转换的编码
        if(t2.priority_flag = 1,t2.fm_type_name,t3.fm_type_name) as vessel_type_name, -- 小类型转换的民称

        nameAis as `name`,
        t1.length,
        t1.breadth as width,
        countryCode as flag_country_code,
        t4.c_name as country_name,
        t1.grossTonnage as gross_tonnage,
        t1.deadweight as deadweight

from marinetraffic_ship_info as t1
         left join dim_mtf_fm_class   -- 大类型
    FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on cast(t1.typeColor as int) = t2.type_id

         left join dim_mtf_fm_type   -- 小类型
    FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.typeSpecificId = t3.type_specific_id

         left join dim_country_code_name_info   -- 国家表
    FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.countryCode = t4.country_code2;



-- -----------------------

-- 数据写入

-- -----------------------

begin statement set;

insert into dwd_mtf_ship_info
select
    ship_id as ship_id,                       -- 船ID
    name,                                     -- 船名
    nameAis as name_ais,                      -- ais船名
    mmsi,                                     -- mmsi
    imo as imo,                               -- imo
    eni,                                      -- eni
    callsign,                                 -- 呼号
    country,                                  -- 国家
    countryCode as country_code,              -- 国家代码
    typeSpecificId as type_specific_id,       -- 具体类型编码
    type,                                     -- 类型
    typeSpecific as type_specific,            -- 具体类型
    typeColor as type_color,                  -- 类型颜色
    grossTonnage as gross_tonnage,            -- 总吨位
    deadweight as deadweight,                 -- 载重吨位
    teu,                                      -- 标准箱
    liquidGas as liquid_gas,                  -- 液化气
    length,                                   -- 长
    breadth,                                  -- 宽
    yearBuilt as year_built,                  -- 建造年份
    status,                                   -- 状态
    if(isNavigationalAid,1,0) as  is_navigational_aid,  -- 是否导航辅助
    correspondingRoamingStationId as corresponding_roaming_station_id, -- 对应的漫游站Id
    homeport,                                -- 母港
    from_unixtime(unix_timestamp())    as  update_time  -- 数据入库时间
from marinetraffic_ship_info;


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
    `name`,
    length,
    width,
    flag_country_code,
    country_name,
    gross_tonnage,
    deadweight,
    '2' as source,
    from_unixtime(unix_timestamp()) as update_time
from tmp_marinetraffic_detail_01;

end;






