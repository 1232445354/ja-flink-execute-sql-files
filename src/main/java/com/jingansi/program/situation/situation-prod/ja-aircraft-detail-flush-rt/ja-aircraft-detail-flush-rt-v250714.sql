--********************************************************************--
-- author:      write your name here
-- create time: 2025/7/10 16:39:17
-- description: write your description here
-- version: ja-aircraft-detail-flush-rt-v250714 飞机详情入库
--********************************************************************--
set 'pipeline.name' = 'ja-aircraft-detail-flush-rt';


SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '600000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- SET 'parallelism.default' = '4';
-- set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.interval' = '600000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/ja-aircraft-detail-flush-rt';



-- 设备检测数据上报 （Source：kafka）
create table aircraft_detail_kafka (
                                       flight_id                 string, -- 飞机id
                                       acquire_time              bigint, -- 最大时间戳 ms
                                       pk_type                   string, -- 源网站id类型
                                       src_pk                    string, -- 源网站id
                                       src_code                  string, -- 原实体表的来源
                                       icao_code                 string, -- icaoCode
                                       registration              string, -- 注册号
                                       flight_type               string, -- 机型
                                       is_military               int, -- 是否军用
                                       country_code              string, -- 国家代码
                                       airlines_icao             string, -- 航空公司代码
                                       adsE row<                         -- ads采集信息
                                           r                       string, -- 注册号
                                       t                       string, -- 机型
                                       icao                    string, -- icaoCode
                                       dbFlags                 bigint, -- 是否军用
                                       `desc`                  string, -- 飞机详细描述（制造商 + 型号）
    -- `timestamp`               bigint, -- 数据最后更新时间戳
                                       ownOp                   string, -- 飞机所有者/运营商（此处为信托公司托管）
                                       `year`                  string, -- 飞机出厂年份
                                       trackJsonObject row <
                                           flight             string, -- 航班号
                                       type               string, --  数据来源类型（ADS-B 信号，ICAO 地址解析）
                                       category           string  -- 飞机类别 A3
                                           >
                                           >


) WITH (
      'connector' = 'kafka',
      'topic' = 'aircraft_entity_detail',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.231:30090',
      'properties.group.id' = 'aircraft_entity_detail_group1',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1751014804000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 飞机实体表（Sink：doris）
create table dws_et_aircraft_info(
                                     flight_id	           string,
                                     acquire_time	       string,
                                     src_code	           string,
                                     icao_code	           string,
                                     pk_type	           string,
                                     src_pk	           string,
                                     registration	       string,
                                     flight_type	       string,
                                     model	               string,
                                     icao_short_type	   string,
                                     category_code	       string,
                                     category_name	       string,
                                     country_code	       string,
                                     country_name	       string,
                                     airlines_icao	       string,
                                     airlines_e_name	   string,
                                     airlines_c_name	   string,
                                     is_military	       int,
                                     test_reg	           string,
                                     registered	       string,
                                     reg_until	           string,
                                     first_flight_date	   string,
                                     engines	           string,
                                     m_year	           string,
                                     manufacturer_icao	   string,
                                     manufacturer_name	   string,
                                     line_number	       string,
                                     operator	           string,
                                     operator_c_name	   string,
                                     operator_callsign	   string,
                                     operator_icao	       string,
                                     operator_iata	       string,
                                     owner	               string,
                                     category_description string,
                                     faa_pia	           int,
                                     faa_ladd	           int,
                                     modes	               int,
                                     adsb	               int,
                                     acars	               int,
                                     is_icao	           int,
                                     notes	               string,
                                     extend_info	       string,
                                     friend_foe	       string,
                                     search_content	   string,
                                     update_time	       string
)WITH (
     'connector' = 'doris',
     -- 'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',   -- k8s部署
     -- 'fenodes' = '172.21.30.105:30030',
     'fenodes' = '172.21.30.245:8030',
     'table.identifier' = 'sa.dws_et_aircraft_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='3',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='20000',
     'sink.batch.interval'='5s',
     'sink.properties.escape_delimiters' = 'true',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 飞机实体表读取
create table dws_et_aircraft_info_source (
                                             flight_id	           string,
    -- acquire_time	       string,
    -- src_code	           string,
                                             icao_code	           string,
    -- pk_type	           string,
    -- src_pk	               string,
                                             registration	       string,
                                             flight_type	       string,
                                             model	               string,
                                             icao_short_type	   string,
                                             category_code	       string,
                                             category_name	       string,
                                             country_code	       string,
                                             country_name	       string,
                                             airlines_icao	       string,
                                             airlines_e_name	   string,
                                             airlines_c_name	   string,
                                             is_military	       int,
                                             test_reg	           string,
                                             registered	       string,
                                             reg_until	           string,
                                             first_flight_date	   string,
                                             engines	           string,
                                             m_year	           string,
                                             manufacturer_icao	   string,
                                             manufacturer_name	   string,
                                             line_number	       string,
                                             operator	           string,
                                             operator_c_name	   string,
                                             operator_callsign	   string,
                                             operator_icao	       string,
                                             operator_iata	       string,
                                             owner	               string,
                                             category_description string,
                                             faa_pia	           int,
                                             faa_ladd	           int,
                                             modes	               int,
                                             adsb	               int,
                                             acars	               int,
                                             is_icao	           int,
                                             notes	               string,
                                             extend_info	       string,
                                             check_cols	       string,
                                             PRIMARY KEY (flight_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://172.21.30.105:31030/sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_aircraft_info',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



-- 飞机类型
create table dim_aircraft_type_category (
                                            id 			        string comment '飞机机型',
                                            category_code       string comment '飞机类型代码',
                                            category_c_name 	string comment '飞机类型名称',
                                            primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://172.21.30.105:31030/sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_aircraft_type_category',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '20000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );


-- 国家数据匹配库（Source：doris）
create table dim_country_code_name_info (
                                            id                        string        comment '国家英文-id',
                                            c_name                    string        comment '国家的中文',
                                            country_code2             string        comment '国家的编码2',
                                            primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://172.21.30.105:31030/sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_country_code_name_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '20000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );



-- 航空公司匹配库国家表（Source：doris）
create table dim_airline_list_info (
                                       icao             string     comment '航空三字码',
                                       e_name           string     comment '航空公司英文名称',
                                       c_name           string     comment '航空公司中文名称',
                                       primary key (icao) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://172.21.30.105:31030/sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_airline_list_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '20000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );

-- 航空器国籍登记代码表
create table dim_aircraft_country_prefix_code (
                                                  prefix_code 	string  comment '代码前缀',
                                                  country_code 	string  comment '国家代码',
                                                  primary key (prefix_code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://172.21.30.105:31030/sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_aircraft_country_prefix_code',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '20000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );


-----------------------

-- 数据处理

-----------------------
-- 整合来源字段
create view tmp_01 as
select
    flight_id,
    acquire_time, -- 毫秒时间戳
    pk_type,
    -- src_pk,
    src_code,
    icao_code,
    registration,
    flight_type,
    is_military,
    country_code,
    airlines_icao,

    adsE.`year`                                  as ads_m_year,
    adsE.ownOp                                   as ads_owner,          -- 所有者
    adsE.r                                       as ads_registration,   -- 注册号
    adsE.t                                       as ads_flight_type,    -- 机型
    upper(adsE.icao)                             as ads_icao_code,      -- icaoCode
    if(left(adsE.icao,1)='~','non_icao','hex')   as ads_pk_type,        -- 原网站id类型
    adsE.dbFlags % 2                             as ads_is_military,    -- 是否军用飞机 0 非军用 1 军用
  adsE.`desc`                                  as ads_model,          -- 飞机详细描述（制造商 + 型号）
  adsE.trackJsonObject.category                as ads_icao_short_type -- 飞机类别
from aircraft_detail_kafka
  where flight_id is not null;


-- 切分注册号
create view tmp_02 as
select
    flight_id,
    acquire_time,
    src_code,      -- 实体表的来源
    country_code,
    airlines_icao,
    ads_icao_code,
    coalesce(ads_pk_type,pk_type)           as pk_type,         -- 原网站id类型
    -- coalesce(ads_icao_code,flight_id)       as src_pk,          -- 原网站id
    coalesce(ads_registration,registration) as registration,    -- 注册号
    coalesce(ads_icao_code,icao_code)       as icao_code,       -- icaoCode
    coalesce(ads_flight_type,flight_type)   as flight_type,     -- 机型
    coalesce(ads_is_military,is_military)   as is_military,     -- 是否军用
    ads_model                               as model,           -- 型号
    ads_icao_short_type                     as icao_short_type, -- 类别
    ads_m_year                              as m_year,          -- 年份
    ads_owner                               as owner,           -- 所有者
    PROCTIME() as proctime,
    if(instr(ads_registration,'-')>0,substring(ads_registration,1,2),concat(substring(ads_registration,1,1),'-')) as prefix_code2,
    if(instr(ads_registration,'-')>0,substring(ads_registration,1,3),concat(substring(ads_registration,1,2),'-')) as prefix_code3,
    if(instr(ads_registration,'-')>0,substring(ads_registration,1,4),concat(substring(ads_registration,1,3),'-')) as prefix_code4,
    if(instr(ads_registration,'-')>0,substring(ads_registration,1,5),concat(substring(ads_registration,1,4),'-')) as prefix_code5,
    CHAR_LENGTH(replace(coalesce(ads_registration,registration),'-','')) as registration_len -- 所有长度
from tmp_01;



-- 关联注册号前缀，取国家代码
create view tmp_03 as
select
    flight_id,
    acquire_time,
    src_code,
    airlines_icao,
    pk_type,
    -- src_pk,
    registration,
    icao_code,
    flight_type,
    is_military,
    ads_icao_code,
    model,
    m_year,
    owner,
    icao_short_type,
    proctime,
    if (coalesce(t7.country_code,t6.country_code,t5.country_code,t4.country_code,t1.country_code) = 'CN' and registration_len = 6,
        'TW',
        coalesce(t7.country_code,t6.country_code,t5.country_code,t4.country_code,t1.country_code)
        ) as country_code          -- 去除第一位B，剩余5位就是台湾

from tmp_02 as t1
         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.prefix_code2=t4.prefix_code

         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t5
                   on t1.prefix_code3=t5.prefix_code

         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t6
                   on t1.prefix_code4=t6.prefix_code

         left join dim_aircraft_country_prefix_code FOR SYSTEM_TIME AS OF t1.proctime as t7
                   on t1.prefix_code5=t7.prefix_code;



create view tmp_04 as
select
    t1.flight_id,
    from_unixtime(t1.acquire_time/1000,'yyyy-MM-dd HH:mm:ss')              as acquire_time,
    if(t1.src_code in('nl','origin') or t1.ads_icao_code is null,t1.src_code,'2')  as src_code,
    coalesce(t1.icao_code,t2.icao_code)                                    as icao_code,
    if(t1.src_code in('nl','origin'),'id',t1.pk_type)                      as pk_type,
    t1.flight_id                                                           as src_pk,
    if(t1.src_code in ('nl','origin'),coalesce(t2.registration,t1.registration),coalesce(t1.registration,t2.registration)) as registration,
    if(t1.src_code in('nl','origin'),coalesce(t2.flight_type,t1.flight_type),coalesce(t1.flight_type,t2.flight_type)) as flight_type,
    coalesce(t2.model,t1.model)                                            as model,
    coalesce(t2.icao_short_type,t1.icao_short_type)                        as icao_short_type,
    coalesce(t4.category_code,t2.category_code)                            as category_code,
    coalesce(t4.category_c_name,t2.category_name)                          as category_name,
    if(t1.src_code in('nl','origin'),coalesce(t2.country_code,t1.country_code),coalesce(t1.country_code,t2.country_code)) as country_code,
    if(t1.src_code in('nl','origin'),coalesce(t2.country_name,t3.c_name),coalesce(t3.c_name,t2.country_name))             as country_name,
    coalesce(t1.airlines_icao,t2.airlines_icao)                            as airlines_icao,
    coalesce(t5.e_name,t2.airlines_e_name)                                 as airlines_e_name,
    coalesce(t5.c_name,t2.airlines_c_name)                                 as airlines_c_name,
    if(t2.is_military is not null,t2.is_military,t1.is_military)           as is_military,

    t2.test_reg,
    t2.registered,
    t2.reg_until,
    t2.first_flight_date,
    t2.engines,
    coalesce(t2.m_year,t1.m_year)  as m_year,
    t2.manufacturer_icao,
    t2.manufacturer_name,
    t2.line_number,
    t2.operator,
    t2.operator_c_name,
    t2.operator_callsign,
    t2.operator_icao,
    t2.operator_iata,
    coalesce(t2.owner,t1.owner) as owner,
    t2.category_description,
    t2.faa_pia,
    t2.faa_ladd,
    t2.modes,
    t2.adsb,
    t2.acars,
    t2.is_icao,
    t2.notes,
    t2.extend_info

from tmp_03 as t1
         left join dws_et_aircraft_info_source FOR SYSTEM_TIME AS OF t1.proctime as t2
                   on t1.flight_id = t2.flight_id

         left join dim_country_code_name_info FOR SYSTEM_TIME AS OF t1.proctime as t3
                   on t1.country_code = t3.country_code2

         left join dim_aircraft_type_category FOR SYSTEM_TIME AS OF t1.proctime as t4
                   on t1.flight_type = t4.id

         left join dim_airline_list_info FOR SYSTEM_TIME AS OF t1.proctime as t5
                   on t1.airlines_icao = t5.icao;


create view tmp_05 as
select
    *,
    case
        when country_code in ('IN','US','JP','AU','TW') and is_military = 1 then '1' -- 敌(印度、美国、日本、澳大利亚、台湾 军事)
        when country_code in ('CN','HK','MO') and is_military = 1  then '2'       -- 我(中国	军事)
        when country_code in ('CN','HK','MO') and is_military = 0  then '3'  -- 友(中国	非军事)
        else '4' end friend_foe,         -- 敌我类型
    concat(
            ifnull(icao_code,''),' ',
            ifnull(registration,''),' ',
            ifnull(flight_type,'')
        ) as search_content
from tmp_04;


-----------------------

-- 数据插入

-----------------------


begin statement set;


insert into dws_et_aircraft_info
select
    flight_id	           ,
    acquire_time	       ,
    src_code	           ,
    icao_code	           ,
    pk_type	           ,
    src_pk	           ,
    registration	       ,
    flight_type	       ,
    model	               ,
    icao_short_type	   ,
    category_code	       ,
    category_name	       ,
    country_code	       ,
    country_name	       ,
    airlines_icao	       ,
    airlines_e_name	   ,
    airlines_c_name	   ,
    is_military	       ,
    test_reg	           ,
    registered	       ,
    reg_until	           ,
    first_flight_date	   ,
    engines	           ,
    m_year	           ,
    manufacturer_icao	   ,
    manufacturer_name	   ,
    line_number	       ,
    operator	           ,
    operator_c_name	   ,
    operator_callsign	   ,
    operator_icao	       ,
    operator_iata	       ,
    owner	               ,
    category_description ,
    faa_pia	           ,
    faa_ladd	           ,
    modes	               ,
    adsb	               ,
    acars	               ,
    is_icao	           ,
    notes	               ,
    extend_info	       ,
    friend_foe	       ,
    search_content	   ,
    from_unixtime(unix_timestamp()) as update_time
from tmp_05;

end;

