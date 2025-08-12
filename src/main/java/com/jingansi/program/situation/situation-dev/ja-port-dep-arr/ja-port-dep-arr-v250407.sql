--********************************************************************--
-- author:      write your name here
-- create time: 2024/12/23 19:57:39
-- description: 船舶港口的出发到达
--********************************************************************--
set 'pipeline.name' = 'ja-port-dep-arr';

set 'parallelism.default' = '4';
set 'sql-client.execution.result-mode' = 'TABLEAU';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';


-- checkpoint时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'execution.checkpointing.timeout' = '360000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-port-dep-arr';


 -- -----------------------

 -- source数据结构

 -- -----------------------

-- 预计到达港口信息
create table source_kafka(
                             acquireType             string, -- 采集数据类型，expected_arrival预计到达，arrival_departures 到达和离开，in_port 在港口
                             acquireTime             string, -- 采集时间
                             ID                      string, -- id
                             SHIP_ID                 string, -- 船舶id
                             MMSI                    string, -- mmsi
                             IMO                     string, -- imo
                             SHIPNAME                string, -- 船舶名称
                             CODE2                   string, -- 船舶国家2字代码

                             PORT_ID                 string, -- 主体港口id，也是到达港口
                             PORT_NAME               string, -- 主体港口名称，也是到达港口
                             PORT_UNLOCODE           string, -- 主体港口UNLOCODE，也是到达港口
                             PORT_COUNTRY_CODE       string, -- 港口国家2字代码
                             COUNTRY                 string, -- 国家英文

                             FROM_PORT_ID            string, -- 始发港口id
                             FROM_PORT_NAME          string, -- 始发港口名称
                             FROM_PORT_COUNTRY_CODE  string, -- 始发港口国家2字代码
                             FROM_PORT_UNLOCODE      string, -- 始发港口UNLOCODE
                             FROM_PORT_TIME          bigint, -- 始发港口ATD时间，北京时间戳
                             FROM_PORT_OFFSET        string, -- 始发时间偏移

                             CURRENT_PORT_ID         string, -- 当前港口id
                             CURRENT_PORT            string, -- 当前港口名称
                             CURRENT_PORT_UNLOCODE   string, -- 当前港口UNLOCODE

                             DESTINATION             string, -- 目的地
                             STATUS_NAME             string, -- 导航系统
                             WIND_TEMP               string, -- 风温，摄氏度
                             WIND_ANGLE              string, -- 风向角，度
                             WIND_SPEED              string, -- 风速，节
                             DISTANCE_TRAVELLED      string, -- 已经走过的航程距离，海里
                             DISTANCE_TO_GO          string, -- 距离目的地距离，海里
                             VOYAGE_IDLE_TIME_SECS   string, -- 航行中的空闲时间/天
                             LAST_UNDERWAY_TIMESTAMP bigint, -- 最后进行中的时间戳，北京时间戳
                             TIMEZONE                string, -- 船只时区
                             ETD                     bigint, -- 预测性ETD ，北京时间戳，预计离港时间
                             ETA                     bigint, -- 计算预计到达时间，北京时间戳
                             ETA_CALC_OFFSET         string, -- ETA时间偏移时区
                             ARR_TIME                bigint, -- 预计到达时间，北京时间戳
                             ETA_OFFSET              string, -- 预计到达时间的时区偏移

                             TYPE_COLOR              string, -- 船舶大类型数字
                             TYPE_SUMMARY            string, -- 船舶小类型英文
                             STATUS                  string, -- 未知
                             V_TIMEZONE              string, -- 未知时区
                             conditionJsonObj        string, -- 筛选条件
    -- ---------------------------------------------------------- 到达和离开

    -- acquireType             string, -- 采集数据类型，expected_arrival预计到达，arrival_departures 到达和离开，in_port 在港口
    -- acquireTime             string, -- 采集时间
    -- SHIP_ID                 string, -- 船舶id
    -- MMSI                    string, -- mmsi
    -- IMO                     string, -- imo
    -- SHIPNAME                string, -- 船舶名称
                             MOVE_TYPE_NAME          string, -- 港口停靠类型示例：到达-离开
                             PORT_TYPE_NAME          string, -- 港口类型名称

    -- PORT_ID                 string, -- 港口id ，停靠港
    -- PORT_NAME               string, -- 停靠港口名称
                             COUNTRY_CODE            string, -- 停靠港国家
                             CENTERX                 string, -- 港口经度
                             CENTERY                 string, -- 港口纬度

                             TO_PORT_ID            string, -- 目的港口id
                             TO_PORT_NAME          string, -- 目的港口名称
                             TO_PORT_COUNTRY       string, -- 目的港口国家
                             TO_PORT_UNLOCODE      string, -- 目的港口unlocode

    -- FROM_PORT_ID          string, -- 最后呼叫港口id
    -- FROM_PORT_NAME        string, -- 最后呼叫港口名称
                             FROM_PORT_TIMESTAMP   bigint, -- 最后呼叫ATD时间戳

                             TIMESTAMP_UTC         bigint, --ATA,ATD 北京时间戳
                             DISTANCE_LEG          string, -- 此次航程距离目的地距离，海里
                             LOAD_STATUS_NAME      string, -- 负载条件
    -- TIMEZONE              string, -- 港口时区
    -- DISTANCE_TRAVELLED    string, -- 已经走过的航行距离，海里
                             INTRANSIT_NAME        string, -- 过境途中停靠

                             ELAPSED_AT_PORT       string, -- 港口停靠时间
                             ELAPSED_UNDERWAY      string, -- 已经航行时间，
                             LEG_TIME_UNDERWAY     string, -- 当前航行段（leg）中船舶实际航行的时间 ，航段时间
                             VOYAGE_IDLE_TIME_MINS string, -- 航次闲置时间

                             FROM_NOANCH_ID         string, -- 航次始发港id
                             FROM_NOANCH_NAME       string, -- 航次始发港名称
                             FROM_NOANCH_COUNTRY    string, -- 航次始发港国家
                             FROM_NOANCH_UNLOCODE   string, -- 航次始发港unlocode
                             FROM_NOANCH_TIMESTAMP  bigint,  -- 航程始发港ATD
                             proctime as PROCTIME()

) with (
      'connector' = 'kafka',
      'topic' = 'marinetraffic_depart_arrive',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'marinetraffic_depart_arrive_group_id',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 港口实体表
drop table if exists dws_et_port_info;
create table dws_et_port_info (
                                  id            string, -- 港口id,
                                  c_name        string, -- 中文名称
                                  e_name        string, -- 英文名称
                                  country_code  string, -- 国家代码
                                  country_name  string, -- 国家名称
                                  mtf_port_id   bigint,-- 对应id
    -- longitude     double,
    -- latitude      double,
                                  primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.244:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_port_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '10000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );


-- 船舶实体表（Source：doris）
drop table if exists dws_vessel_et_info_rt;
create table dws_vessel_et_info_rt (
                                       vessel_id           bigint,   -- 船舶id
                                       vessel_name         string,   -- 船舶名称
                                       vessel_c_name       string,   -- 船舶中文名称
                                       country_code        string,   -- 国家代码
                                       country_name        string,   -- 国际名称
                                       vessel_class_code	  int,  	-- 船舶大类型代码
                                       vessel_class_name	  string,   -- 船舶大类型名称
                                       vessel_type_code	  int,  	-- 船舶小类型代码
                                       vessel_type_name	  string,   -- 船舶小类型名称
                                       primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_vessel_et_info_rt',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '500000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );


-- 对应关系表
drop table if exists dws_vessel_rl_src_id;
create table dws_vessel_rl_src_id (
                                      vessel_id           bigint,   -- 船舶id
                                      src_code            int,      -- 来源1:fleetmon 2:marinetraffic 3:vt 4:岸基
                                      src_pk              string,   -- 源网站主键

                                      primary key (vessel_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://172.21.30.245:9030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_vessel_rl_src_id',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '500000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );



-- ----------------------- sink数据结构 -----------------------

-- 港口最大采集时间表
drop table if exists dws_bhv_port_time;
create table dws_bhv_port_time (
                                   id                        string, -- 港口id
                                   src_code                  string, -- 类型，expected_arrival-预计到达,arrival_departures-到达和离开
                                   acquire_time              string, -- 采集时间string
                                   update_time               string -- 数据入库时间
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.244:8030',
      'table.identifier' = 'sa.dws_bhv_port_time',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 预计到达数据写入表（Sink：doris）
create table dws_bhv_port_expected_arrival (
                                               id                      string, --  港口id
                                               acquire_time            string, -- 采集时间
                                               vessel_id               string, --  船舶id
                                               mmsi                    string, --  船舶mmsi
                                               imo                     string, --  船舶imo
                                               ship_e_name             string, --  船舶英文名称
                                               ship_c_name             string, --  船舶中文名称
                                               ship_country_code       string, --  船舶国家代码
                                               ship_country_name       string, --  船舶国家名称
                                               vessel_class_code	      int,    --  船舶大类型代码
                                               vessel_class_name	      string, --  船舶大类型名称
                                               vessel_type_code	      int,    --  船舶小类型代码
                                               vessel_type_name	      string, --  船舶小类型名称

                                               port_id                 string, --  主体港口id，也是到达港口
                                               port_name               string, --  主体港口名称，也是到达港口
                                               from_port_id            string, --  始发港口id
                                               from_port_name          string, --  始发港口名称
                                               from_port_time          string, --  始发港口ATD时间
                                               from_port_offset        string, --  始发时间偏移

                                               current_port_id         string, --  当前港口id
                                               current_port            string, --  当前港口名称

                                               destination             string, --  目的地
                                               status_name             string, --  导航系统
                                               wind_temp               string, --  风温，摄氏度
                                               wind_angle              string, --  风向角，度
                                               wind_speed              string, --  风速，节
                                               distance_travelled      string, --  已经走过的航程距离，海里
                                               distance_to_go          string, --  距离目的地距离，海里
                                               voyage_idle_time_secs   string, --  航行中的空闲时间/天
                                               last_underway_timestamp string, --  最后进行中的时间
                                               timezone                string, --  船只时区
                                               etd                     string, --  预测性ETD，预计离港时间
                                               eta                     string, --  计算预计到达时间
                                               eta_calc_offset         string, --  ETA时间偏移时区
                                               arr_time                string, --  预计到达时间
                                               eta_offset              string, --  预计到达时间的时区偏移
                                               src_id 	    		  string, --  原始id
                                               src_ship_id 			  string, --  原始船舶id
                                               src_port_id			  string, --  原始港口id，也是到达港口id
                                               src_from_port_id		  string, --  原始始发港id
                                               src_current_id		  string, --  原始当前港口id
                                               condition_json_obj      string, -- 筛选条件
                                               update_time             string -- 数据入库时间
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_bhv_port_expected_arrival',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- 到达和离开数据写入表（Sink：doris）
create table dws_bhv_port_arrival_departure (
                                                id                    string,  -- 港口id
                                                acquire_time          string,  -- 采集时间
                                                vessel_id             string,  -- 船舶id
                                                mmsi                  string,  -- 船舶mmsi
                                                imo                   string,  -- 船舶imo
                                                ship_e_name           string,  -- 船舶英文名称
                                                ship_c_name           string,  -- 船舶中文名称
                                                ship_country_code     string,  -- 船舶国家代码
                                                ship_country_name     string,  -- 船舶国家名称
                                                vessel_class_code		int,  	 -- 船舶大类型代码
                                                vessel_class_name		string,  -- 船舶大类型名称
                                                vessel_type_code		int,  	 -- 船舶小类型代码
                                                vessel_type_name		string,  -- 船舶小类型名称

                                                move_type             string,  -- 港口停靠类型代码示例：到达-离开
                                                move_type_name        string,  -- 港口停靠类型示例：到达-离开
                                                port_type_name        string,  -- 港口类型名称
                                                port_id               string,  -- 港口id ，停靠港
                                                port_name             string,  -- 停靠港口名称
                                                centerx               string,  -- 港口经度
                                                centery               string,  -- 港口纬度

                                                to_port_id            string, -- 目的港口id
                                                to_port_name          string, -- 目的港口名称

                                                from_port_id          string, -- 最后呼叫港口id
                                                from_port_name        string, -- 最后呼叫港口名称
                                                from_port_timestamp   string, -- 最后呼叫atd时间戳

                                                timestamp_utc         string,   -- ataatd 北京时间戳
                                                distance_leg          string,  -- 此次航程距离目的地距离，海里
                                                load_status_name      string,  -- 负载条件
                                                timezone              string,  -- 港口时区
                                                distance_travelled    string,  -- 已经走过的航行距离，海里
                                                intransit_name        string,  -- 过境途中停靠

                                                elapsed_at_port       string,  -- 港口停靠时间
                                                elapsed_underway      string,  -- 已经航行时间
                                                leg_time_underway     string,  -- 当前航行段（leg）中船舶实际航行的时间 ，航段时间
                                                voyage_idle_time_mins string,  -- 航次闲置时间

                                                from_noanch_id         string,  -- 航次始发港id
                                                from_noanch_name       string,  -- 航次始发港名称
                                                from_noanch_timestamp  string,  -- 始发港atd

                                                src_ship_id 			string,  -- 原始-船舶id
                                                src_port_id			string,  -- 原始-港口id ，停靠港
                                                src_to_port_id		string,  -- 原始-目的港口id
                                                src_from_port_id      string,  -- 原始-最后呼叫港口id
                                                src_from_noanch_id  	string,  -- 原始-航次始发港id
                                                condition_json_obj    string,  -- 筛选条件
                                                update_time         	string   -- 数据入库时间
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dws_bhv_port_arrival_departure',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- -----------------------

-- 数据处理

-- -----------------------

-- 所有数据先关联通用数据
create view tmp_01 as
select
    t1.*,
    t1.conditionJsonObj as condition_json_obj,
    t1.acquireType as src_code,
    t1.acquireTime as acquire_time,
    cast(t3.vessel_id as varchar) as vessel_id,

    t3.vessel_name   as ship_e_name,
    t3.vessel_c_name as ship_c_name,
    t3.country_code  as ship_country_code,
    t3.country_name  as ship_country_name,
    t3.vessel_class_code,
    t3.vessel_class_name,
    t3.vessel_type_code,
    t3.vessel_type_name
from source_kafka as t1
         left join dws_vessel_rl_src_id FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 船舶对应关系表
                   on t1.SHIP_ID = t2.src_pk and 2 = t2.src_code

         left join dws_vessel_et_info_rt FOR SYSTEM_TIME AS OF t1.proctime as t3  -- 船舶实体表
                   on t2.vessel_id = t3.vessel_id
where t2.vessel_id is not null;



-- 预计到达数据处理
create view tmp_02 as
select
    PORT_ID       as id,
    acquire_time,
    vessel_id,
    MMSI as mmsi,
    IMO as imo,
    ship_e_name,
    ship_c_name,
    ship_country_code,
    ship_country_name,
    vessel_class_code,
    vessel_class_name,
    vessel_type_code,
    vessel_type_name,

    PORT_ID                          as port_id,
    PORT_NAME                        as port_name,

    FROM_PORT_ID                     as from_port_id,
    FROM_PORT_NAME                   as from_port_name,
    from_unixtime(FROM_PORT_TIME,'yyyy-MM-dd HH:mm:ss') as from_port_time,
    FROM_PORT_OFFSET                 as from_port_offset,

    CURRENT_PORT_ID                  as current_port_id,
    CURRENT_PORT                     as current_port,
    DESTINATION                      as destination,
    STATUS_NAME                      as status_name,
    WIND_TEMP                        as wind_temp,
    WIND_ANGLE                       as wind_angle,
    WIND_SPEED                       as wind_speed,
    DISTANCE_TRAVELLED               as distance_travelled,
    DISTANCE_TO_GO                   as distance_to_go,
    VOYAGE_IDLE_TIME_SECS            as voyage_idle_time_secs,
    from_unixtime(LAST_UNDERWAY_TIMESTAMP,'yyyy-MM-dd HH:mm:ss') as last_underway_timestamp,
    TIMEZONE                         as timezone,
    from_unixtime(ETD,'yyyy-MM-dd HH:mm:ss') as etd,
    from_unixtime(ETA,'yyyy-MM-dd HH:mm:ss') as eta,
    ETA_CALC_OFFSET                  as eta_calc_offset,
    from_unixtime(ARR_TIME,'yyyy-MM-dd HH:mm:ss')  as arr_time,
    ETA_OFFSET                       as eta_offset,
    ID                               as src_id,
    SHIP_ID                          as src_ship_id,
    PORT_ID                          as src_port_id,
    FROM_PORT_ID                     as src_from_port_id,
    CURRENT_PORT_ID                  as src_current_id,
    condition_json_obj,
    src_code
from tmp_01 where acquireType = 'expected_arrival';



-- (select * from tmp_01 where acquireType = 'expected_arrival') as t1;

-- left join dws_et_port_info FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 预计到达-港口
--   on cast(t1.PORT_ID as bigint) = t2.mtf_port_id;

-- left join dws_et_port_info FOR SYSTEM_TIME AS OF t1.proctime as t3   -- 预计到达-港口
--   on cast(t1.FROM_PORT_ID as bigint) = t3.mtf_port_id

-- left join dws_et_port_info FOR SYSTEM_TIME AS OF t1.proctime as t4   -- 预计到达-港口
--   on cast(t1.CURRENT_PORT_ID as bigint) = t4.mtf_port_id
--   where t2.id is not null;


-- 到达和离开数据处理
create view tmp_03 as
select
    PORT_ID    as id,
    acquire_time,
    vessel_id,
    MMSI as mmsi,
    IMO as imo,
    ship_e_name,
    ship_c_name,
    ship_country_code,
    ship_country_name,
    vessel_class_code,
    vessel_class_name,
    vessel_type_code,
    vessel_type_name,

    MOVE_TYPE_NAME       as  move_type,
    case when MOVE_TYPE_NAME = 'ARRIVAL' then '到达'
         when MOVE_TYPE_NAME = 'DEPARTURE' then '离开'
        end                  as move_type_name,
    PORT_TYPE_NAME       as port_type_name,
    PORT_ID              as port_id,
    PORT_NAME            as port_name,
    CENTERX              as centerx,
    CENTERY              as centery,
    TO_PORT_ID           as to_port_id,
    TO_PORT_NAME         as to_port_name,

    FROM_PORT_ID         as from_port_id,
    TO_PORT_NAME         as from_port_name,
    from_unixtime(FROM_PORT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss') as from_port_timestamp,
    from_unixtime(TIMESTAMP_UTC,'yyyy-MM-dd HH:mm:ss') as timestamp_utc,
    DISTANCE_LEG         as distance_leg,
    LOAD_STATUS_NAME     as load_status_name,
    TIMEZONE             as timezone,
    DISTANCE_TRAVELLED   as distance_travelled,
    INTRANSIT_NAME       as intransit_name,
    ELAPSED_AT_PORT      as elapsed_at_port,
    ELAPSED_UNDERWAY     as elapsed_underway,
    LEG_TIME_UNDERWAY    as leg_time_underway,
    VOYAGE_IDLE_TIME_MINS as voyage_idle_time_mins,
    FROM_NOANCH_ID        as from_noanch_id,
    FROM_NOANCH_NAME      as from_noanch_name,
    from_unixtime(FROM_NOANCH_TIMESTAMP,'yyyy-MM-dd HH:mm:ss') as from_noanch_timestamp,
    SHIP_ID              as src_ship_id,
    PORT_ID              as src_port_id,
    TO_PORT_ID           as src_to_port_id,
    FROM_PORT_ID         as src_from_port_id,
    FROM_NOANCH_ID       as src_from_noanch_id,
    condition_json_obj,
    src_code
from tmp_01 where acquireType = 'arrival_departures';



-- from (select * from tmp_01 where acquireType = 'arrival_departures') as t1
-- left join dws_et_port_info FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 预计到达-港口
--   on cast(t1.PORT_ID as bigint) = t2.mtf_port_id

-- left join dws_et_port_info FOR SYSTEM_TIME AS OF t1.proctime as t3   -- 预计到达-港口
--   on cast(t1.TO_PORT_ID as bigint) = t3.mtf_port_id

-- left join dws_et_port_info FOR SYSTEM_TIME AS OF t1.proctime as t4   -- 预计到达-港口
--   on cast(t1.FROM_PORT_ID as bigint) = t4.mtf_port_id

--   left join dws_et_port_info FOR SYSTEM_TIME AS OF t1.proctime as t5   -- 预计到达-港口
-- on cast(t1.FROM_NOANCH_ID as bigint) = t5.mtf_port_id
--   where t2.id is not null;



-- -----------------------

-- 数据写入

-- -----------------------


begin statement set;


-- 数据入库时间表
insert into dws_bhv_port_time
select
    PORT_ID as id,
    acquireType as src_code,
    acquireTime as acquire_time,
    from_unixtime(unix_timestamp()) as update_time
from source_kafka;


-- 到达和离开数据入库
insert into dws_bhv_port_arrival_departure
select
    id,
    acquire_time,
    vessel_id,
    mmsi,
    imo,
    ship_e_name,
    ship_c_name,
    ship_country_code,
    ship_country_name,
    vessel_class_code,
    vessel_class_name,
    vessel_type_code,
    vessel_type_name,
    move_type,
    move_type_name,
    port_type_name,
    port_id,
    port_name,
    centerx,
    centery,
    to_port_id,
    to_port_name,
    from_port_id,
    from_port_name,
    from_port_timestamp,
    timestamp_utc,
    distance_leg,
    load_status_name,
    timezone,
    distance_travelled,
    intransit_name,
    elapsed_at_port,
    elapsed_underway,
    leg_time_underway,
    voyage_idle_time_mins,
    from_noanch_id,
    from_noanch_name,
    from_noanch_timestamp,
    src_ship_id,
    src_port_id,
    src_to_port_id,
    src_from_port_id,
    src_from_noanch_id,
    condition_json_obj,
    from_unixtime(unix_timestamp()) as update_time
from tmp_03;



-- 预计到达数据入行为表
insert into dws_bhv_port_expected_arrival
select
    id,
    acquire_time,
    vessel_id,
    mmsi,
    imo,
    ship_e_name,
    ship_c_name,
    ship_country_code,
    ship_country_name,
    vessel_class_code,
    vessel_class_name,
    vessel_type_code,
    vessel_type_name,
    port_id,
    port_name,
    from_port_id,
    from_port_name,
    from_port_time,
    from_port_offset,
    current_port_id,
    current_port,
    destination,
    status_name,
    wind_temp,
    wind_angle,
    wind_speed,
    distance_travelled,
    distance_to_go,
    voyage_idle_time_secs,
    last_underway_timestamp,
    timezone,
    etd,
    eta,
    eta_calc_offset,
    arr_time,
    eta_offset,
    src_id,
    src_ship_id,
    src_port_id,
    src_from_port_id,
    src_current_id,
    condition_json_obj,
    from_unixtime(unix_timestamp()) as update_time
from tmp_02;


end;




