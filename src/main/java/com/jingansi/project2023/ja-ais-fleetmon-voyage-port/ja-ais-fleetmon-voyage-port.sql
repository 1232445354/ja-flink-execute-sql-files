--********************************************************************--
-- author:      write your name here
-- create time: 2023/5/1 21:33:48
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ja-ais-fleetmon-voyage-port';

-- set 'parallelism.default' = '2';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
-- set 'table.exec.state.ttl' = '600000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
-- set 'execution.checkpointing.interval' = '100000';
-- set 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/xxx-checkpoint';


 -----------------------

 -- 数据结构

 -----------------------


-- -- 创建kafka数据来源的表（Source：kafka）
drop table  if exists ais_fleetmon_vessel_port_info_kafka;
create table ais_fleetmon_vessel_port_info_kafka(
                                                    vesselId     bigint  , -- 船ID
                                                    `data` row (
                                                        vessels array<
                                                        row(
                                                        voyage row (
                                                        lastPort row(
                                                        port row(
                                                        portId        bigint   , -- 出发港口-ID
                                                        name          string   , -- 出发港口-名称
                                                        countryCode   string   , -- 出发港口-所属国家-简称
                                                        locode        string   , -- 出发港口-定位器
                                                        __typename    string
                                                        ),
                                                        atd         bigint , -- 实际出发时间
                                                        __typename  string
                                                        ),
                                                        nextPort row(
                                                        port row(
                                                        portId       bigint   , -- 目的港口-ID
                                                        name         string   , -- 目的港口-名称
                                                        countryCode  string   , -- 目的港口-所属国家-简称
                                                        locode       string   , -- 目的港口-定位器
                                                        __typename   string
                                                        ),
                                                        eta              bigint   , -- 由fleetmon提供，预计到达时间
                                                        distanceToPort   double   , -- 距离到达目的港口
                                                        __typename       string
                                                        ),
                                                        __typename           string
                                                        ),
                                                        __typename string
                                                        )
                                                        >
                                                        )
) with (
      'connector' = 'kafka',
      'topic' = 'ais_fleetmon_vessel_port_info',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.group.id' = 'ais_fleetmon_detail_single_item-rt',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1683320399000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


--船港口信息入库（Sink：Doris）
drop table if exists vessel_port_info;
create table vessel_port_info (
                                  vessel_id      			bigint 		comment '船ID字',
                                  last_port_id	        bigint	    comment '出发港口-ID',
                                  last_port_name	        string	    comment '出发港口-名称',
                                  last_port_country_code	string	    comment '出发港口-所属国家-简称',
                                  last_port_locode	    string	    comment '出发港口-定位器',
                                  last_port_atd	        bigint	    comment '实际出发时间',
                                  last_port_atd_format    string      comment '出发时间格式化',
                                  next_port_id	        bigint	    comment '目的港口-ID',
                                  next_port_name	        string	    comment '目的港口-名称',
                                  next_port_country_code	string	    comment '目的港口-所属国家-简称',
                                  next_port_locode	    string	    comment '目的港口-定位器',
                                  next_port_eta	        bigint	    comment '由fleetmon提供，预计到达时间',
                                  next_port_eta_format    string      comment '预计到达时间格式化',
                                  distance_to_port	    double	    comment '距离到达目的港口',
                                  update_time	            string	    comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.202:30030',
      'table.identifier' = 'kesadaran_situasional.vessel_port_info',
      'username' = 'admin',
      'password' = 'admin',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='100000',
      'sink.batch.interval'='10s'
      --  	'sink.properties.escape_delimiters' = 'flase',
      -- 'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      -- 'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      -- 'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-----------------------

-- 数据处理

-----------------------

-- 最外层的对象拆解
drop table if exists tmp_vessel_port_01;
create view tmp_vessel_port_01 as
select
    vesselId as vessel_id,
    `data`.vessels[1].voyage as voyage
from ais_fleetmon_vessel_port_info_kafka;


-- 取出出发港口和目的港口
drop table if exists tmp_vessel_port_02;
create view tmp_vessel_port_02 as
select
    vessel_id,
    voyage.lastPort as lastPort,
    voyage.nextPort as nextPort
from tmp_vessel_port_01;


-- 港口信息
drop table if exists tmp_vessel_port_03;
create view tmp_vessel_port_03 as
select
    vessel_id,
    lastPort.port.portId        as last_port_id,
    lastPort.port.name          as last_port_name,
    lastPort.port.countryCode   as last_port_country_code,
    lastPort.port.locode        as last_port_locode,
    lastPort.atd                as last_port_atd,
    nextPort.port.portId        as next_port_id,
    nextPort.port.name          as next_port_name,
    nextPort.port.countryCode   as next_port_country_code,
    nextPort.port.locode        as next_port_locode,
    nextPort.eta                as next_port_eta,
    nextPort.distanceToPort     as distance_to_port
from tmp_vessel_port_02;


-----------------------

-- 数据插入

-----------------------

insert into vessel_port_info
select
    vessel_id,
    last_port_id,
    last_port_name,
    last_port_country_code,
    last_port_locode,
    last_port_atd,
    from_unixtime(last_port_atd,'yyyy-MM-dd HH:mm:ss') as last_port_atd_format,
    next_port_id,
    next_port_name,
    next_port_country_code,
    next_port_locode,
    next_port_eta,
    from_unixtime(next_port_eta,'yyyy-MM-dd HH:mm:ss') as next_port_eta_format,
    distance_to_port,
    from_unixtime(unix_timestamp()) as update_time
from tmp_vessel_port_03;



