--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/8/2 14:16:03
-- description: 船舶静态属性投票
--version: ja-vessel-info-vote-v250325
--********************************************************************--
set 'pipeline.name' = 'ja-vessel-info-vote';

SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '4';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-vessel-info-vote';


create function atr_vote_udf as 'com.jingan.udf.vote.MergeVesselAtrUdf';

create function map_repeated_call_udtf as 'com.jingan.udtf.MapRepeatedCallUdtf';


-- -----------------------

-- 数据结构

-- -----------------------

-- 数据来源（Source：Kafka）
drop table if exists kafka_source_info;
create table kafka_source_info(
                                  newVesselId                        bigint, -- 入库的船舶ID
                                  mmsi                               string, -- mmsi

                                  marinetrafficInfo array<
                                      row(
                                      shipId                           bigint,  -- 船ID
                                      aisName                          string,  -- ais船名
                                      imo                              string,  -- imo
                                      mmsi                             string,  -- mmsi
                                      length                           double,  -- 长
                                      width                            double,  -- 宽
                                      callsign                         string,  -- 呼号
                                      type                             string,  -- ais类型名英文称
                                      typeId                           string,  -- ais类型id
                                      typeColorId                      string,  -- 大类型id
                                      subtype                          string,  -- 小类型英文名称
                                      countryCode                      string,  -- 国家2字代码
                                      yearBuilt                        string,  -- 建造年份
                                      homePort                         string,  -- 母港
                                      serviceStatus                    string,  -- 状态

                                      name                             string,  -- 船名称
                                      country                          string,  -- 国家英文名称
                                      eni                              string,  -- eni
                                      correspondingRoamingStationId    string,  -- 对应的漫游站Id
                                      isNavigationalAid                boolean,-- 是否导航辅助
                                      aisTransponderClass              string  -- AIS应答器类
                                      )
                                      >,
                                  proctime  as PROCTIME()

) with (
      'connector' = 'kafka',
      'topic' = 'vessel_entity_info',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'vessel_entity_info_group_id',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1741230000000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- vt数据最后一条
create table dws_vt_vessel_status_info (
                                           mmsi                  string, -- mmsi主键
                                           imo                   string, -- imo
                                           name                  string, -- name
                                           length                double, -- 长度
                                           width                 double, -- 宽度
                                           callsign              string, -- 呼号
                                           type                  bigint, -- ais类型代码
                                           country               string, -- 国家名称
                                           acquire_timestamp_format     timestamp, -- 时间
                                           primary key (mmsi) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_vt_vessel_status_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- lb数据最后一条
create table dws_ais_landbased_vessel_status (
                                                 mmsi                  bigint, -- mmsi主键
                                                 imo                   bigint, -- imo
                                                 ship_name             string, -- name
                                                 length                double, -- 长度
                                                 width                 double, -- 宽度
                                                 call_no               string, -- 呼号
                                                 ship_and_carg_type    int, -- ais类型代码
                                                 acquire_time          bigint, -- 时间
                                                 primary key (mmsi) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_ais_landbased_vessel_status',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- ais类型对应关系表（Source：doris）
create table dim_vessel_ais_type_rel (
                                         id                         bigint, -- id
                                         ais_type_code              string, -- ais类型代码
                                         ais_type_name              string, -- ais类型名称
                                         standard_class_code        string, -- 对应标准的大类型代码
                                         standard_class_c_name      string, -- 对应标准的大类型中文名称
                                         standard_type_code         string, -- 对应标准的小类型代码
                                         standard_type_c_name       string, -- 对应标准的小类型中文名称
                                         remark					  string, -- 备注
                                         primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_vessel_ais_type_rel',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '1'
      );


-- vt船国家数据匹配库（Source：doris）
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
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_vt_country_code_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 标准国家数据匹配库（Source：doris）
create table dim_country_code_name_info (
                                            id              string  comment 'id',
                                            source          string  comment '来源',
                                            c_name          string  comment '中文名称',
                                            country_code2   string  comment '国家2字代码',
                                            primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_code_name_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- mtf服务状态匹配库（Source：doris）
create table dim_mtf_service_status_info (
                                             id              string  comment 'id',
                                             e_name          string  comment '来源',
                                             c_name          string  comment '中文名称',
                                             primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_mtf_service_status_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 船舶属性表（Source：doris）
create table dws_vessel_et_info_rt_source (
                                              vessel_id	         bigint,
                                              vessel_name	         string,
                                              vessel_c_name        string,
                                              mmsi	             bigint,
                                              imo	                 bigint,
                                              callsign	         string,
                                              length	             double,
                                              width	             double,
                                              height	             double,
                                              gross_tonnage        double,
                                              deadweight	         double,
                                              build_year	         string,
                                              service_status_code	 string,
                                              service_status_name	 string,
                                              registry_port	     string,
                                              country_code	     string,
                                              country_name	     string,
                                              ais_type_code	     int,
                                              ais_type_name	     string,
                                              vessel_class_code	 int,
                                              vessel_class_name	 string,
                                              vessel_type_code	 int,
                                              vessel_type_name	 string,
                                              owner	             string,
                                              friend_foe	         string,
                                              check_cols	         string,
                                              primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_vessel_et_info_rt',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '100000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 实体详情入库（Sink：doris）
create table dws_vessel_et_info_rt (
                                       vessel_id           bigint,
                                       vessel_name         string,
                                       vessel_c_name       string,
                                       mmsi                bigint,
                                       imo                 bigint,
                                       callsign            string,
                                       length              double,
                                       width               double,
                                       height              double,
                                       gross_tonnage       double,
                                       deadweight          double,
                                       build_year          string,
                                       service_status_code string,
                                       service_status_name string,
                                       registry_port       string,
                                       country_code        string,
                                       country_name        string,
                                       ais_type_code       int,
                                       ais_type_name       string,
                                       vessel_class_code   int,
                                       vessel_class_name   string,
                                       vessel_type_code    int,
                                       vessel_type_name    string,
                                       owner               string,
                                       extend_info         string,
                                       search_content      string,
                                       friend_foe          string,
                                       update_time         string

) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.244:8030',
      'table.identifier' = 'sa.dws_vessel_et_info_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='30000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- -----------------------

-- 数据处理

-- -----------------------

create view tmp_kafka_source_info_01 as
select
    t1.*,
    t2.vessel_class_code as t5_vessel_class_code,
    t2.vessel_class_name as t5_vessel_class_name,
    t2.vessel_type_code as t5_vessel_type_code,
    t2.vessel_type_name as t5_vessel_type_name,
    t2.height                       as t5_height,
    t2.gross_tonnage                as t5_gross_tonnage,
    t2.deadweight                   as t5_deadweight,
    t2.owner                        as t5_owner,
    t2.callsign                     as t5_callsign,
    t2.imo                          as t5_imo,
    t2.build_year                   as t5_build_year
from kafka_source_info as t1
         left join dws_vessel_et_info_rt_source FOR SYSTEM_TIME AS OF t1.proctime as t2        -- vt最后一条数据
                   on cast(t1.newVesselId as bigint) = t2.vessel_id
where check_cols<>'["all-checked"]'
   or check_cols is null;


-- 关联维表处理vt、lb数据
create view tmp_01 as
select
    t1.newVesselId,
    t1.mmsi,
    t1.marinetrafficInfo,
    t5_vessel_class_code,
    t5_vessel_class_name,
    t5_vessel_type_code,
    t5_vessel_type_name,
    t5_height,
    t5_gross_tonnage,
    t5_deadweight,
    t5_owner,
    t5_callsign,
    t5_imo,
    t5_build_year,
    map[
        'e_name',if(t2.name = '',cast(null as varchar),t2.name),
    'imo',if(t2.imo = '0',cast(null as varchar),t2.imo),
    'mmsi',t2.mmsi,
    'length',cast(t2.length as varchar),
    'width',cast(t2.width as varchar),
    'callsign',t2.callsign,
    'class_code',t3.standard_class_code,
    'class_name',t3.standard_class_c_name,
    'type_code',t3.standard_type_code,
    'type_name',t3.standard_type_c_name,
    'ais_type_code',cast(t2.type as varchar),
    'ais_type_name',t3.ais_type_name,
    'country_code',t4.country_code2,
    'year_built',cast(null as varchar),
    'home_port',cast(null as varchar),
    'service_status',cast(null as varchar)
    ] as vt_map,

    map[
    'e_name',t5.ship_name,
    'imo',cast(t5.imo as varchar),
    'mmsi',cast(t5.mmsi as varchar),
    'length',cast(t5.length as varchar),
    'width',cast(t5.width as varchar),
    'callsign',t5.call_no,
    'class_code',t6.standard_class_code,
    'class_name',t6.standard_class_c_name,
    'type_code',t6.standard_type_code,
    'type_name',t6.standard_type_c_name,
    'ais_type_code',cast(t5.ship_and_carg_type as varchar),
    'ais_type_name',t6.ais_type_name,
    'country_code',cast(null as varchar),
    'year_built',cast(null as varchar),
    'home_port',cast(null as varchar),
    'service_status',cast(null as varchar)
  ] as lb_map,
  t1.proctime

from tmp_kafka_source_info_01 as t1
  left join dws_vt_vessel_status_info FOR SYSTEM_TIME AS OF t1.proctime as t2        -- vt最后一条数据
    on t1.mmsi = t2.mmsi
  left join dim_vessel_ais_type_rel FOR SYSTEM_TIME AS OF t1.proctime as t3          -- vt的类型表
    on cast(t2.type as varchar) = t3.ais_type_code
  left join dim_vt_country_code_info FOR SYSTEM_TIME AS OF t1.proctime as t4         -- vt的国家表
    on t2.country = t4.vt_c_name
  left join dws_ais_landbased_vessel_status FOR SYSTEM_TIME AS OF t1.proctime as t5  -- lb最后一条数据
    on cast(t1.mmsi as bigint) = t5.mmsi
  left join dim_vessel_ais_type_rel FOR SYSTEM_TIME AS OF t1.proctime as t6           -- lb的类型表
    on cast(t5.ship_and_carg_type as varchar) = t6.ais_type_code;



-- 调用函数，进行投票计算
create view tmp_02 as
select
    tmp_01.newVesselId,
    tmp_01.mmsi,
    tmp_01.proctime,
    t5_vessel_class_code,
    t5_vessel_class_name,
    t5_vessel_type_code,
    t5_vessel_type_name,
    t5_height,
    t5_gross_tonnage,
    t5_deadweight,
    t5_owner,
    t5_callsign,
    t5_imo,
    t5_build_year,
    b.vote_result
from tmp_01 ,lateral table(map_repeated_call_udtf(atr_vote_udf(marinetrafficInfo,vt_map,lb_map))) as b(vote_result);



-- 投票结果解析拆分
create view tmp_03 as
select
    newVesselId,
    mmsi,
    proctime,
    t5_vessel_class_code,
    t5_vessel_class_name,
    t5_vessel_type_code,
    t5_vessel_type_name,
    t5_height,
    t5_gross_tonnage,
    t5_deadweight,
    t5_owner,
    t5_callsign,
    t5_imo,
    t5_build_year,
    vote_result['e_name'] as e_name,
    vote_result['imo'] as imo,
    vote_result['mmsi'] as vote_mmsi,
    vote_result['length'] as length,
    vote_result['width'] as width,
    vote_result['callsign'] as callsign,
    vote_result['class_code'] as class_code,
    vote_result['class_name'] as class_name,
    vote_result['type_code'] as type_code,
    vote_result['type_name'] as type_name,
    vote_result['ais_type_code'] as ais_type_code,
    vote_result['ais_type_name'] as ais_type_name,
    vote_result['country_code'] as country_code,
    vote_result['year_built'] as year_built,
    vote_result['home_port'] as home_port,
    vote_result['service_status'] as service_status
from tmp_02;



-- 关联维表，整合入库字段
create view tmp_04 as
select
    cast(t1.newVesselId as bigint)  as vessel_id,
    t1.e_name                       as vessel_name,
    cast(null as varchar)           as vessel_c_name,
    cast(coalesce(t1.vote_mmsi,t1.mmsi) as bigint)    as mmsi,
    coalesce(cast(t1.imo as bigint),t5_imo)           as imo,
    coalesce(t1.callsign,t5_callsign)  as callsign,
    cast(t1.length as double)          as length,
    cast(t1.width as double)           as width,
    t5_height                       as height,
    t5_gross_tonnage                as gross_tonnage,
    t5_deadweight                   as deadweight,
    coalesce(t1.year_built,t5_build_year)   as build_year,
    t1.service_status               as service_status_code,
    t3.c_name                       as service_status_name,
    t1.home_port                    as registry_port,
    t1.country_code,
    t2.c_name                       as country_name,
    cast(t1.ais_type_code as int)   as ais_type_code,
    t1.ais_type_name,
    coalesce(t5_vessel_class_code,cast(t1.class_code as int)) as vessel_class_code,
    coalesce(t5_vessel_class_name,t1.class_name)             as vessel_class_name,
    coalesce(t5_vessel_type_code,cast(t1.type_code as int))  as vessel_type_code,
    coalesce(t5_vessel_type_name,t1.type_name)               as vessel_type_name,

    t5_owner                        as owner,
    cast(null as varchar)           as extend_info,
    concat(
            ifnull(t1.e_name,''),' ',
            ifnull(vote_mmsi,''),' ',
            ifnull(t1.imo,'')
        ) as search_content,
    case
        when t1.country_code in('IN','US','JP','AU','TW') and class_code = '5' then '1' -- 敌 - 美印日澳 军事
        when t1.country_code in ('CN','MO','HK') and class_code = '5' then '2'               -- 我 中国 军事
        when t1.country_code in ('CN','MO','HK') and  class_code <> '5' then '3'      -- 友 中国 非军事
        else '4'
        end as friend_foe,
    from_unixtime(unix_timestamp()) as update_time

from tmp_03 as t1
         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF t1.proctime as t2        -- 国家名称
                   on t1.country_code = t2.country_code2

         left join dim_mtf_service_status_info FOR SYSTEM_TIME AS OF t1.proctime as t3       -- 服务状态
                   on t1.service_status = t3.e_name;

-- left join dim_vessel_c_name_info FOR SYSTEM_TIME AS OF t1.proctime as t4       -- 船舶中文名称
--   on coalesce(t1.vote_mmsi,t1.mmsi) = t4.mmsi

-- left join dws_vessel_et_info_rt_source FOR SYSTEM_TIME AS OF t1.proctime as t5       -- 船舶实体表
--   on cast(t1.newVesselId as bigint) = t5.vessel_id;



--  -- -----------------------

--  -- 数据写入

--  -- -----------------------

begin statement set;

insert into dws_vessel_et_info_rt
select
    vessel_id           ,
    vessel_name         ,
    vessel_c_name       ,
    mmsi                ,
    imo                 ,
    callsign            ,
    length              ,
    width               ,
    height              ,
    gross_tonnage       ,
    deadweight          ,
    build_year          ,
    service_status_code ,
    service_status_name ,
    registry_port       ,
    country_code        ,
    country_name        ,
    ais_type_code       ,
    ais_type_name       ,
    vessel_class_code   ,
    vessel_class_name   ,
    vessel_type_code    ,
    vessel_type_name    ,
    owner               ,
    extend_info         ,
    search_content      ,
    friend_foe          ,
    update_time
from tmp_04;


end;


