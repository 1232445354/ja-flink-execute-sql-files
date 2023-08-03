--********************************************************************--
-- author:      write your name here
-- create time: 2023/7/11 11:40:47
-- description: write your description here
--********************************************************************--

set 'pipeline.name' = 'ja-marinetraffic-vessel-list-rt';


set 'parallelism.default' = '4';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
-- set 'table.exec.state.ttl' = '600000';
-- set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '100000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-marinetraffic-vessel-list-rt';



 -----------------------

 -- 数据结构

 -----------------------

-- 创建kafka全量marineTraffic数据来源的表（Source：kafka）
drop table if exists marinetraffic_ship_list;
create table marinetraffic_ship_list(
                                        SHIP_ID              string,    -- 船舶的唯一标识符
                                        `timeStamp`          bigint,    -- 采集时间
                                        SHIPNAME             string,    -- 船舶的名称
                                        SPEED                string,    -- 船舶的当前速度，以节（knots）为单位 10倍
                                        DESTINATION          string,    -- 船舶的目的地
                                        FLAG                 string,    -- 船舶的国家或地区旗帜标识
                                        LON                  string,    -- 船舶当前位置的经度值
                                        LAT                  string,    -- 船舶当前位置的经度值
                                        ELAPSED              string,    -- 自上次位置报告以来经过的时间，以分钟为单位
                                        COURSE               string,    -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方
                                        GT_SHIPTYPE          string,    -- 船舶的全球船舶类型码
                                        SHIPTYPE             string,    -- 船舶的类型码，表示船舶所属的船舶类型
                                        HEADING              string,    -- 船舶的船首朝向
                                        LENGTH               string,    -- 船舶的长度，以米为单位
                                        WIDTH                string,    -- 船舶的宽度，以米为单位
                                        DWT                  string,    -- 船舶的载重吨位
                                        INVALID_DIMENSIONS   string,    -- 无效_维度
                                        ROT                  string,    -- 船舶的旋转率
                                        W_LEFT               string,    -- 舶的左舷吃水线宽度
                                        L_FORE               string    -- 船舶的前吃水线长度
) with (
      'connector' = 'kafka',
      'topic' = 'marinetraffic_ship_list',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-marineTraffic-ship-list-rt',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1690785128000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


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
                                        lng                  			string					, -- 船舶当前位置的经度值
                                        lat                  			string					, -- 船舶当前位置的经度值
                                        elapsed              			string					, -- 自上次位置报告以来经过的时间，以分钟为单位
                                        course               			string					, -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方
                                        big_type_num_code      			string					, -- 船舶类型代码--大类
                                        big_type_name					string					, -- 船舶类型名称--大类
                                        small_type_num_code				string					, -- 船舶类型代码--小类
                                        small_type_name					string					, -- 船舶类型名称--小类
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
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'table.identifier' = 'sa.dwd_vessel_list_all_rt',
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
                                           lng                  			string					, -- 船舶当前位置的经度值
                                           lat                  			string					, -- 船舶当前位置的经度值
                                           elapsed              			string					, -- 自上次位置报告以来经过的时间，以分钟为单位
                                           course               			string					, -- 船舶的当前航向，以度数（degrees）表示，0度表示北方，90度表示东方
                                           big_type_num_code      			string					, -- 船舶类型代码--大类
                                           big_type_name					string					, -- 船舶类型名称--大类
                                           small_type_num_code				string					, -- 船舶类型代码--小类
                                           small_type_name					string					, -- 船舶类型名称--小类
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
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'table.identifier' = 'sa.dws_vessel_list_status_rt',
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



-----------------------

-- 数据处理

-----------------------

-- 数据筛选处理
drop table if exists kafka_tmp_01;
create view kafka_tmp_01 as
select
    SHIP_ID              as vessel_id,
    from_unixtime(`timeStamp`,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    `timeStamp`          as acquire_timestamp,
    SHIPNAME             as vessel_name,
    SPEED                as speed,
    DESTINATION          as destination,
    FLAG                 as country_code,
    LON                  as lng,
    LAT                  as lat,
    ELAPSED              as elapsed,
    COURSE               as course,
    SHIPTYPE             as big_type_num_code,
    GT_SHIPTYPE          as gt_ship_type,
    HEADING              as heading,
    LENGTH               as length,
    WIDTH                as width,
    DWT                  as dwt,
    INVALID_DIMENSIONS   as invalid_dimensions,
    ROT                  as rot,
    W_LEFT               as w_left,
    L_FORE               as l_fore
from marinetraffic_ship_list
where  `timeStamp` is not null
  and CHAR_LENGTH(SHIP_ID) <= 30;



-----------------------

-- 数据插入

-----------------------

begin statement set;

insert into dwd_vessel_list_all_rt
select
    vessel_id,
    acquire_timestamp_format,
    acquire_timestamp,
    vessel_name,
    speed,
    destination,
    country_code,
    cast(null as varchar) as country_name,
    lng,
    lat,
    elapsed,
    course,
    big_type_num_code,
    cast(null as varchar) as big_type_name,
    gt_ship_type as small_type_num_code,
    cast(null as varchar) as small_type_name,
    gt_ship_type,
    heading,
    length,
    width,
    dwt,
    invalid_dimensions,
    rot,
    w_left,
    l_fore,
    from_unixtime(unix_timestamp()) as update_time
from kafka_tmp_01;



insert into dws_vessel_list_status_rt
select
    vessel_id,
    acquire_timestamp_format,
    acquire_timestamp,
    vessel_name,
    speed,
    destination,
    country_code,
    cast(null as varchar) as country_name,
    lng,
    lat,
    elapsed,
    course,
    big_type_num_code,
    cast(null as varchar) as big_type_name,
    gt_ship_type as small_type_num_code,
    cast(null as varchar) as small_type_name,
    gt_ship_type,
    heading,
    length,
    width,
    dwt,
    invalid_dimensions,
    rot,
    w_left,
    l_fore,
    from_unixtime(unix_timestamp()) as update_time
from kafka_tmp_01;



end;





