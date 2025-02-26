--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/3/19 10:26:05
-- description: 靖安vt船舶数据单独入库
--********************************************************************--

set 'pipeline.name' = 'ja-vtexplore-vessel-list';

set 'table.exec.state.ttl' = '144000';
set 'parallelism.default' = '2';


-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '300000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-vtexplore-vessel-list';


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
                                         course               double, -- 航向
                                         draught              double, -- 吃水
                                         ETA                  string, -- 预计到岗时间
                                         `timestamp`          bigint, -- 采集时间戳
                                         ais_time             string,
                                         proctime             as PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'ais_vtexplorer_ship_list2',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ais_vtexplorer_ship_list_2_idc',
      'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1716807514000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 创建映射doris的全量数据表(Sink:doris)
drop table if exists dwd_vt_vessel_all_info;
create table dwd_vt_vessel_all_info(
                                       mmsi                 			string  	, -- mmsi
                                       imo                  			string		, -- imo
                                       acquire_timestamp_format        string      , -- 采集时间戳格式化
                                       acquire_timestamp    			bigint      , -- 采集时间戳
                                       name                 			string		, -- 英文名称
                                       callsign             			string		, -- 呼号
                                       country              			string		, -- 国家中文
                                       lng            			 		double		, -- 经度
                                       lat             		 		double		, -- 纬度
                                       speed                			double		, -- 速度
                                       speed_km 						double		, -- 速度km/h
                                       course               			double      , -- 方向
                                       draught              			double		, -- 吃水
                                       type                 			bigint		, -- 船舶类型代码
                                       length 							double      , -- 长度
                                       width 							double      , -- 宽度
                                       is_satellite         			bigint      , -- 是否卫星数据，1:true，0:false
                                       eta                  			string		, -- 预计到岗时间
                                       dest_code     			 		string		, -- 目的地代码
                                       dest_name     			 		string		, -- 目的地名称英文
                                       ais_time             			string      , -- 自己计算的时间
                                       update_time                     string       -- 数据入库时间
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.21.30.244:8030,172.21.30.245:8030,172.21.30.246:8030',
     'table.identifier' = 'sa.dwd_vt_vessel_all_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='5',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='100000',
     'sink.batch.interval'='20s',
     'sink.properties.escape_delimiters' = 'flase',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );


-- 创建映射doris的全量数据表 - mmsi和imo为key(Sink:doris)
drop table if exists dws_vt_vessel_status_info;
create table dws_vt_vessel_status_info(
                                          mmsi                 			string  	, -- mmsi
                                          imo                  			string		, -- imo
                                          acquire_timestamp_format        string      , -- 采集时间戳格式化
                                          acquire_timestamp    			bigint      , -- 采集时间戳
                                          name                 			string		, -- 英文名称
                                          callsign             			string		, -- 呼号
                                          country              			string		, -- 国家中文
                                          lng            			 		double		, -- 经度
                                          lat             		 		double		, -- 纬度
                                          speed                			double		, -- 速度
                                          speed_km 						double		, -- 速度km/h
                                          course               			double      , -- 方向
                                          draught              			double		, -- 吃水
                                          type                 			bigint		, -- 船舶类型代码
                                          length 							double      , -- 长度
                                          width 							double      , -- 宽度
                                          is_satellite         			bigint      , -- 是否卫星数据，1:true，0:false
                                          eta                  			string		, -- 预计到岗时间
                                          dest_code     			 		string		, -- 目的地代码
                                          dest_name     			 		string		, -- 目的地名称英文
                                          ais_time             			string      , -- 自己计算的时间
                                          update_time                     string       -- 数据入库时间
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.21.30.244:8030,172.21.30.245:8030,172.21.30.246:8030',
     'table.identifier' = 'sa.dws_vt_vessel_status_info',
     'username' = 'admin',
     'password' = 'Jingansi@110',
     'doris.request.tablet.size'='5',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='100000',
     'sink.batch.interval'='20s',
     'sink.properties.escape_delimiters' = 'flase',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );



-----------------------

-- 数据处理

-----------------------


drop view if exists temo_ais_vtexplorer_ship_list_01;
create view temo_ais_vtexplorer_ship_list_01 as
select
    MMSI  as mmsi,
    IMO   as imo,
    if(callsign <> '',callsign,cast(null as string)) as callsign,
    `timestamp` as acquire_timestamp,
    from_unixtime(`timestamp`,'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    if(name <> '',name,cast(null as string)) as name,
    if(country <> '',country,cast(null as string)) as country,
    longitude as lng,
    latitude as lat,
    speed,
    speed * 1.852 as speed_km,
    course,
    draught,
    type,
    cast(trim(split_index(`size`,'*',0)) as double) as length,
    cast(trim(split_index(`size`,'*',1)) as double) as width,
    if(is_satellite,1,0) as is_satellite,
    if(ETA <> '',ETA,cast(null as string)) as eta,
    if(destination_code <> '',destination_code,cast(null as string))  as dest_code,
    if(destination_name <> '',destination_name,cast(null as string))  as dest_name,
    ais_time,
    from_unixtime(unix_timestamp()) as update_time
from (
         select
             *,
             count(*) over (partition by MMSI,IMO,`timestamp` order by proctime) as cnt
         from ais_vtexplorer_ship_list
         where `timestamp` is not null
     ) a
where cnt=1;




-----------------------

-- 数据插入

-----------------------

begin statement set;

insert into dwd_vt_vessel_all_info
select
    mmsi,
    imo,
    acquire_timestamp_format,
    acquire_timestamp,
    name,
    callsign,
    country,
    lng,
    lat,
    speed,
    speed_km,
    course,
    draught,
    type,
    length,
    width,
    is_satellite,
    eta,
    dest_code,
    dest_name,
    ais_time,
    update_time
from temo_ais_vtexplorer_ship_list_01;


insert into dws_vt_vessel_status_info
select
    mmsi,
    imo,
    acquire_timestamp_format,
    acquire_timestamp,
    name,
    callsign,
    country,
    lng,
    lat,
    speed,
    speed_km,
    course,
    draught,
    type,
    length,
    width,
    is_satellite,
    eta,
    dest_code,
    dest_name,
    ais_time,
    update_time
from temo_ais_vtexplorer_ship_list_01;


end;


