--********************************************************************--
-- author:      yibo@jingan-inc.com
-- create time: 2024/1/4 19:45:47
-- description: vesselfinder的船舶详情
-- version: 0.0.0.2401105
--********************************************************************--
set 'pipeline.name' = 'ja-vesselfinder-vesssel-detail';

set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '600000';
set 'sql-client.execution.result-mode' = 'TABLEAU';


 -----------------------

 -- 数据结构

 -----------------------

-- 数据来源kafka
drop table if exists vesselfinder_vesselDetail_info;
create table vesselfinder_vesselDetail_info(
                                               mmsi                 string,
                                               imo                  string,
                                               mmsi1                string,
                                               imo1                 string,
                                               flag                 string,
                                               source               string,
                                               `timeStamp`          bigint,
                                               lon                  double,
                                               vu_l                 double,
                                               a2                   string,
                                               dyn                  boolean,
                                               cog                  double,
                                               ga                   double,
                                               aw                   double,
                                               r                    double,
                                               draught              double,
                                               npa                  boolean,
                                               l                    double,
                                               `similar`            string,
                                               hm                   string,
                                               al                   double,
                                               vu_b                 boolean,
                                               pic                  string,
                                               lat                  double,
                                               owner_email          string,
                                               fail                 double,
                                               ex                   string,
                                               gr                   double,
                                               owner_web            string,
                                               ns                   double,
                                               mdb_name             string,
                                               manager_address      string,
                                               pl                   string,
                                               dr                   double,
                                               fl                   string,
                                               cr                   double,
                                               gt                   double,
                                               owner_address        string,
                                               ts                   bigint,
                                               cs                   string,
                                               sog                  double,
                                               callsign             string,
                                               dw                   double,
                                               d2                   double,
                                               pic_id               double,
                                               manager              string,
                                               yde                  double,
                                               aty                  double,
                                               ba                   double,
                                               y                    double,
                                               manager_email        string,
                                               type                 string,
                                               yd                   string,
                                               manager_web          string,
                                               teu                  double,
                                               ais_name             string,
                                               w                    double,
                                               bu                   string,
                                               owner                string,
                                               contact              string

) with (
      'connector' = 'kafka',
      'topic' = 'vesselfinder_vesselDetail_info',
      'properties.bootstrap.servers' = 'kafka.kafka.svc.cluster.local:9092',
      'properties.group.id' = 'aaa',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1704618046000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- 数据写入doris
drop table if exists dws_vesselfinder_vessel_detail_info;
create table dws_vesselfinder_vessel_detail_info (
                                                     mmsi1 						string    		comment '请求数据的mmsi',
                                                     imo1  						string  		comment '请求数据的imo',
                                                     mmsi                  		string  		comment 'vesselfinder返回的mmsi',
                                                     imo                  		string  		comment 'vesselfinder返回的imo',
                                                     acquire_timestamp_format 	string			comment '采集的时间戳格式化',
                                                     acquire_timestamp			bigint			comment '采集的时间戳s级别',
                                                     ais_name					string 			comment 'ais船舶名称,英文名称',
                                                     mdb_name					string 			comment '船舶名称,英文名称',
                                                     callsign					string 			comment '呼号',
                                                     type						string  		comment '类型英文',
                                                     homeport 					string 			comment '母港',
                                                     country_e_name				string 			comment '国家英文名称',
                                                     country_code				string 			comment '国家代码两位',
                                                     lng						double 			comment '经度，未知格式',
                                                     lat						double 			comment '维度，未知格式',
                                                     length						double 			comment '长度',
                                                     length_overall				double 			comment '长度',
                                                     width						double 			comment '宽度beam',
                                                     width_beam					double 			comment '宽度 beam',
                                                     course						double			comment '航向  原cog',
                                                     heading 					double			comment '航向 course/10 变成单位度',
                                                     sog 						double			comment '速度 需要换算',
                                                     speed 						double			comment '速度 sog / 10 单位变成节kn',
                                                     draught				    double			comment 'Current Draught 当前吃水 分米单位',
                                                     max_draught				double			comment '最大吃水max Draught (m)',
                                                     nav_status_code			double			comment '航行状态代码',
                                                     deadweight					double			comment 'Deadweight自重（t）',
                                                     gross_tonnage				double			comment '总吨位',
                                                     build_year					double			comment '建造年份',
                                                     yard 						string 			comment '码（等于3英尺或36英寸或0.9144米）; 场地; 帆桁',
                                                     pic						string          comment '船舶图片的请求id',
                                                     `similar`					string			comment '类似的船舶列表',
                                                     history_name				string 			comment '历史使用的名称等信息',
                                                     teu						double 			comment '标准箱（系集装箱运量统计单位，以长',
                                                     bale 						double 			comment 'Bale - 未知啥意思',
                                                     gas  						double 			comment '气体（立方米）- 未知啥意思',
                                                     grain						double 			comment 'Grain - 未知啥意思',
                                                     crude_oil 					double  		comment 'Crude Oil (bbl) (原油（桶）) - 未知啥意思',
                                                     builder 					string 			comment '建设者',
                                                     build_place				string 			comment '建设地点',
                                                     classification_society		string			comment '船级社',
                                                     registered_owner			string			comment '注册业主',
                                                     owner_address				string			comment '业主地址',
                                                     owner_website				string			comment '业主网站',
                                                     owner_email				string			comment '业主邮箱',
                                                     manager					string			comment '管理者',
                                                     manager_address			string			comment '管理者地址',
                                                     manager_website			string			comment '管理者网站',
                                                     manager_email				string			comment '管理者邮箱',
                                                     contact					string			comment '',
                                                     pic_id						double 			comment '',
                                                     r							double 			comment '',
                                                     aty						double 			comment '',
                                                     fail						double 			comment '',
                                                     vu_l						double 			comment '',
                                                     yde						double 			comment '',
                                                     d2							double 			comment '',
                                                     vu_b						boolean 		comment '',
                                                     npa						boolean 		comment '',
                                                     dyn						boolean 		comment '',
                                                     source                     string          comment 'mmsi1和imo1来源的网站',
                                                     remark 					string			comment '备注-该mmsi1和imo1是否获取到详情',
                                                     update_time				string			comment '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = 'doris-fe-service.bigdata-doris.svc.cluster.local:9999',
      'table.identifier' = 'sa.dws_vesselfinder_vessel_detail_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='100000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-----------------------

-- 数据处理

-----------------------
drop view if exists tmp_vesselfinder_vesselDetail_info_01;
create view tmp_vesselfinder_vesselDetail_info_01 as
select
    mmsi1,
    imo1,
    mmsi,
    imo,
    from_unixtime(if(ts is not null,ts,`timeStamp`),'yyyy-MM-dd HH:mm:ss') as acquire_timestamp_format,
    if(ts is not null,ts,`timeStamp`)                as acquire_timestamp,
    if(ais_name = '',cast(null as varchar),ais_name) as ais_name,
    if(mdb_name = '',cast(null as varchar),mdb_name) as mdb_name,
    if(callsign = '',cast(null as varchar),callsign) as callsign,
    type,
    if(hm = '',cast(null as varchar),hm)             as homeport,
    if(fl = '',cast(null as varchar),fl)             as country_e_name,
    a2                  as country_code,
    lon                 as lng,
    lat,
    l                   as length,
    al                  as length_overall,
    w                   as width,
    aw                  as width_beam,
    cog                 as course,
    cog / 10            as heading,
    sog,
    sog / 10            as speed,
    draught,
    dr                  as max_draught,
    ns                  as nav_status_code,
    dw                  as deadweight,
    gt                  as gross_tonnage,
    y                   as build_year,
    if(yd = '',cast(null as varchar),yd)   as yard,
    if(pic = '',cast(null as varchar),pic) as pic,
    `similar`,
    ex                 as history_name,
    teu,
    ba                 as bale,
    ga                 as gas,
    gr                 as grain,
    cr                 as crude_oil,
    if(bu = '',cast(null as varchar),bu)   as builder,
    if(pl = '',cast(null as varchar),pl)   as build_place,
    if(cs = '',cast(null as varchar),cs)       as classification_society,
    if(owner = '',cast(null as varchar),owner) as registered_owner,
    if(owner_address = '-',cast(null as varchar),owner_address) as owner_address,
    if(owner_web = '-',cast(null as varchar),owner_web) as owner_website,
    if(owner_email = '-',cast(null as varchar),owner_email) as owner_email,
    if(manager = '',cast(null as varchar),manager) as manager,
    if(manager_address = '-',cast(null as varchar),manager_address) as manager_address,
    if(manager_web = '-',cast(null as varchar),manager_web) as manager_website,
    if(manager_email = '-',cast(null as varchar),manager_email) as manager_email,
    if(contact = '-',cast(null as varchar),contact) as contact,
    pic_id,
    r,
    aty,
    fail,
    vu_l,
    yde,
    d2,
    vu_b,
    npa,
    dyn,
    source,
    flag               as remark
from vesselfinder_vesselDetail_info
where mmsi1 is not null;


-----------------------

-- 数据写入

-----------------------


insert into dws_vesselfinder_vessel_detail_info
select
    mmsi1,
    imo1,
    mmsi,
    imo,
    acquire_timestamp_format,
    acquire_timestamp,
    ais_name,
    mdb_name,
    callsign,
    type,
    homeport,
    country_e_name,
    country_code,
    lng,
    lat,
    length,
    length_overall,
    width,
    width_beam,
    course,
    heading,
    sog,
    speed,
    draught,
    max_draught,
    nav_status_code,
    deadweight,
    gross_tonnage,
    build_year,
    yard,
    pic,
    `similar`,
    history_name,
    teu,
    bale,
    gas,
    grain,
    crude_oil,
    builder,
    build_place,
    classification_society,
    registered_owner,
    owner_address,
    owner_website,
    owner_email,
    manager,
    manager_address,
    manager_website,
    manager_email,
    contact,
    pic_id,
    r,
    aty,
    fail,
    vu_l,
    yde,
    d2,
    vu_b,
    npa,
    dyn,
    source,
    remark,
    from_unixtime(unix_timestamp()) as update_time
from tmp_vesselfinder_vesselDetail_info_01;



