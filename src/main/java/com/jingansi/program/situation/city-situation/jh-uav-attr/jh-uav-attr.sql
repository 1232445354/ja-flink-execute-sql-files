--********************************************************************--
-- author:      write your name here
-- create time: 2025/5/11 17:38:47
-- description:
-- version: jh-uav-attr
--********************************************************************--
set 'pipeline.name' = 'jh-uav-attr';


SET 'execution.type' = 'streaming';
SET 'table.planner' = 'blink';
SET 'table.exec.state.ttl' = '60000';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SET 'parallelism.default' = '1';
-- set 'execution.checkpointing.tolerable-failed-checkpoints' = '10';

SET 'execution.checkpointing.interval' = '120000';
SET 'state.checkpoints.dir' = 's3://flink/flink-checkpoints/jh-uav-attr';

 -----------------------

 -- 数据结构来源

 -----------------------


create table jh_uav_info (

                             userInfo row<
                                 gmtRegister          string,
                             cardCode             string,
                             phone                string,
                             `delete`               boolean,
                             id                   bigint,
                             username             string,
                             userTypeName         string,
                             areaCode             string,
                             realName             boolean,
                             cardTypeName         string,
                             listStatus           int,
                             fullName             string,
                             gmtCreate            string,
                             residence            string
                                 >,
                             uavInfo row<
                                 username             string,
                             type                 int,
                             gmtRegister          string,
                             cardCode             string,
                             phone                string,
                             address              string,
                             typeName             string,
                             uavCompanyName       string,
                             `delete`              int,
                             id                   bigint,
                             uavModelName         string,
                             areaCode             string,
                             sn                   string,
                             fullName             string,
                             gmtCreate            string
                                 >,
                             whiteInfo row<
                                 phone                string,
                             gmtExpire            string,
                             listType             int,
                             `delete`               int,
                             id                   bigint,
                             uavModelName         string,
                             engineType           string,
                             areaCode             string,
                             maxHeight            int,
                             companyName          string,
                             listStatus           int,
                             sn                   string,
                             fullName             string,
                             gmtCreate            string
                                 >,
                             proctime as proctime()

) with (
      'connector' = 'kafka',
      'topic' = 'jh-uav-info',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'uav_merge_target2',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',  -- 1745564415000
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 无人机实体表来源
create table `dws_et_uav_info_source` (
                                          id                      string  comment '无人机id-sn号',
                                          sn                      string  comment '序列号',
                                          device_id               string  comment '设备编号',
                                          name                    string  comment '无人机名称',
                                          recvmac                 string  comment 'MAC地址',
                                          manufacturer            string  comment '厂商',
                                          model                   string  comment '型号',
                                          owner                   string  comment '所有者',
                                          type                    string  comment '类型',
                                          source                  string  comment '数据来源',
                                          category                string  comment '类别',
                                          phone                   string  comment '电话',
                                          empty_weight            string  comment '空机重量',
                                          maximum_takeoff_weight  string  comment '最大起飞重量',
                                          purpose                 string  comment '用途',
                                          PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://135.100.11.132:31030/sa?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'table-name' = 'dws_et_uav_info',
      'lookup.cache.max-rows' = '50000',
      'lookup.cache.ttl' = '86400s',
      'lookup.max-retries' = '10'
      );



create table `dws_et_uav_pilot_info` (
                                         id              varchar(100) NULL COMMENT '飞手id',
                                         a_method        varchar(100) NULL COMMENT '认证方式 个人认证；企业认证',
                                         name            varchar(200) NULL COMMENT '个人认证（名称）；企业认证（企业名称）',
                                         card_code       varchar(300) NULL COMMENT '个人认证（身份证号）；企业认证（统一社会信用代码）',
                                         phone           varchar(200) NULL COMMENT '联系方式"',
                                         email           varchar(300) NULL COMMENT '邮箱',
                                         address         varchar(300) NULL COMMENT '地址',
                                         register_uav    varchar(200) NULL COMMENT '登记无人机-序列号（产品型号）',
                                         username        varchar(20) NULL COMMENT '持有者姓名',
                                         user_type_name  varchar(20) NULL COMMENT '用户类型名称',
                                         card_type_name  varchar(20) NULL COMMENT '证件类型名称',
                                         area_code       varchar(20) NULL COMMENT '所属地区',
                                         real_name       boolean NULL COMMENT '是否实名（0 未实名，1已实名）',
                                         gmt_register    string NULL COMMENT '实名注册时间',
                                         list_status     int NULL COMMENT '名单状态（0 正常，1 白名单，2 灰名单，3 黑名单）',
                                         search_content  varchar(500) NULL COMMENT '搜索字段 将所有搜索值放在该字段，建立倒排索引',
                                         update_time     string NULL COMMENT '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      'table.identifier' = 'sa.dws_et_uav_pilot_info',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- 无人机实体表
create table `dws_et_uav_info` (
                                   id                      string  comment '无人机id-sn号',
                                   sn                      string  comment '序列号',
                                   name                    string  comment '无人机名称',
                                   device_id               string  comment '无人机的设备id-牍术介入的',
                                   recvmac                 string  comment 'MAC地址',
                                   manufacturer            string  comment '厂商',
                                   model                   string  comment '型号',
                                   owner                   string  comment '所有者',
                                   type                    string  comment '类型',
                                   source                  string  comment '数据来源',
                                   search_content          string  comment '倒排索引数据',
                                   update_time             string  comment '数据入库时间',
                                   category                string  COMMENT '类别',
                                   card_code               string  COMMENT '所有人身份证号/统一信用代码',
                                   card_type_name          string  COMMENT '证件类型名称',
                                   phone                   string  COMMENT '电话',
                                   email                   string  COMMENT '邮箱',
                                   company_name            string  COMMENT '持有单位',
                                   empty_weight            string  COMMENT '空机重量',
                                   maximum_takeoff_weight  string  COMMENT '最大起飞重量',
                                   identity_type           int     COMMENT '无人机身份类型（0 未知，1 无人机，2 低慢小）',
                                   identity_type_name      string  COMMENT '无人机身份类型名称',
                                   engine_type             string  COMMENT '动力类型',
                                   area_code               string  COMMENT '所属地区',
                                   address                 string  COMMENT '详细地址',
                                   username                string  COMMENT '持有者姓名',
                                   user_type_name          string  COMMENT '用户类型名称',
                                   list_status             int     COMMENT '名单状态（0 正常，1 白名单，2 灰名单）',
                                   list_type               int     COMMENT '名单类型(1 警用白名单，2 低空经济白名单，3 政务白名单， 4 多次黑飞黑名单， 5 重点人员黑名单)',
                                   gmt_expire              string  COMMENT '过期时间 ',
                                   real_name               boolean COMMENT '是否实名（0 未实名，1已实名） ',
                                   gmt_register            string  COMMENT '实名注册时间',
                                   is_white_list           boolean comment '是否白名单',
                                   residence               string  COMMENT '居住地',
                                   purpose                 string  COMMENT '用途'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      'table.identifier' = 'sa.dws_et_uav_info',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );




-- 飞手和无人机关系
create table `dws_rl_uav_pilot` (
                                    `uav_id` varchar(200) NULL COMMENT '无人机的id-sn号',
                                    `pilot_id` varchar(200) NULL COMMENT '飞机id',
                                    update_time     string NULL COMMENT '数据入库时间'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      'table.identifier' = 'sa.dws_rl_uav_pilot',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- 无人机实体信息表
create table `dwd_et_uav_jh_info` (
                                      id  bigint	comment '无人机id',
                                      type  int	comment '无人机类型（0 未知，1 无人机，2 低慢小）',
                                      type_name  varchar(20)	comment '无人机类型名称',
                                      full_name  varchar(20)	comment '无人机持有者姓名',
                                      sn  varchar(20)	comment '无人机序列号',
                                      uav_model_name  varchar(20)	comment '无人机型号名称',
                                      uav_company_name  varchar(20)	comment '无人机厂商名称',
                                      gmt_register  string	comment '注册时间',
                                      area_code  varchar(20)	comment '所属地区',
                                      address  varchar(200)	comment '详细地址',
                                      username  varchar(20)	comment '持有者姓名',
                                      card_code  varchar(20)	comment '持有者身份证号',
                                      phone  varchar(20)	comment '持有者手机号',
                                      deleted  int	comment '是否删除（0 正常，1 已删除）',
                                      gmt_create  string	comment '创建时间',
                                      update_time  string	comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      -- 'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dwd_et_uav_jh_info',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- 无人机白名单表
create table `dwd_et_uav_jh_white_info` (
                                            id  bigint  comment '无人机id',
                                            sn  varchar(20)  comment '无人机序列号',
                                            uav_model_name  varchar(20)  comment '无人机型号名称',
                                            max_height  int  comment '最大高度（单位米）',
                                            engine_type  varchar(20)  comment '动力类型',
                                            company_name  varchar(50)  comment '持有单位',
                                            list_status  int  comment '名单状态（0 正常，1 白名单，2 灰名单）',
                                            list_type  int  comment '名单类型(1 警用白名单，2 低空经济白名单，3 政务白名单， 4 多次黑飞黑名单， 5 重点人员黑名单)',
                                            gmt_expire  string  comment '过期时间',
                                            deleted  int  comment '是否删除（0 正常，1 已删除）',
                                            gmt_create  string  comment '创建时间',
                                            update_time  string  comment '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      -- 'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dwd_et_uav_jh_white_info',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



-- 无人机用户数据表
create table `dwd_et_uav_jh_user_info` (
                                           id  bigint comment  'id',
                                           username  varchar(20) comment  '持有者姓名',
                                           user_type_name  varchar(20) comment  '用户类型名称',
                                           card_type_name  varchar(20) comment  '证件类型名称',
                                           phone  varchar(20) comment  '持有者手机号',
                                           card_code  varchar(20) comment  '证件号码',
                                           area_code  varchar(20) comment  '所属地区',
                                           real_name  Boolean comment  '是否实名（0 未实名，1已实名）',
                                           gmt_register  string comment  '实名注册时间',
                                           full_name  varchar(20) comment  '姓名',
                                           residence  varchar(20) comment  '居住地',
                                           list_status  int comment  '名单状态（0 正常，1 白名单，2 灰名单，3 黑名单）',
                                           deleted  Boolean comment  '是否删除（0 正常，1 已删除）',
                                           gmt_create  string comment  '创建时间',
                                           update_time  string comment  '更新时间'
) with (
      'connector' = 'doris',
      'fenodes' = '135.100.11.132:30030',
      -- 'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dwd_et_uav_jh_user_info',
      'username' = 'root',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='3000',
      'sink.batch.interval'='2s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



begin statement set;

-- 警航数据写入实体表
insert into dws_et_uav_info
select
    coalesce(uavInfo.sn,whiteInfo.sn)                       as id,
    coalesce(uavInfo.sn,whiteInfo.sn)            as sn,
    coalesce(whiteInfo.uavModelName,uavInfo.uavModelName,whiteInfo.sn,uavInfo.sn,b.name)  as name,
    b.device_id                                              as device_id,
    b.recvmac as recvmac,
    coalesce(uavInfo.uavCompanyName,b.manufacturer)     as manufacturer,
    coalesce(whiteInfo.uavModelName,uavInfo.uavModelName,b.model)       as model,
    coalesce(whiteInfo.fullName,userInfo.fullName,uavInfo.fullName,b.owner)          as owner,
    b.type                     as type,
    'ZHENDI' as source,
    concat(
            ifnull(coalesce(whiteInfo.uavModelName,uavInfo.uavModelName,b.model),''),' ',
            ifnull(coalesce(uavInfo.sn,whiteInfo.sn),'')
        )         as search_content,
    from_unixtime(unix_timestamp()) as update_time,
    b.category as category                 , -- 类别
    coalesce(uavInfo.cardCode,userInfo.cardCode) as card_code                , -- 所有人身份证号/统一信用代码
    userInfo.cardTypeName as card_type_name           , -- 证件类型名称
    coalesce(whiteInfo.phone,uavInfo.phone,userInfo.phone) as phone                    , -- 电话
    cast(null as string  ) as email                    , -- 邮箱
    whiteInfo.companyName as company_name             , -- 持有单位
    b.empty_weight as empty_weight             , -- 空机重量
    b.maximum_takeoff_weight as maximum_takeoff_weight   , -- 最大起飞重量
    uavInfo.type as identity_type            , -- 无人机身份类型（0 未知，1 无人机，2 低慢小）
    uavInfo.typeName as identity_type_name       , -- 无人机身份类型名称
    whiteInfo.engineType as engine_type              , -- 动力类型
    coalesce(whiteInfo.areaCode,uavInfo.areaCode,userInfo.areaCode) as area_code                , -- 所属地区
    uavInfo.address as address                  , -- 详细地址
    coalesce(uavInfo.username,userInfo.username) as username                 , -- 持有者姓名
    userInfo.userTypeName as user_type_name           , -- 用户类型名称
    coalesce(whiteInfo.listStatus,userInfo.listStatus) as list_status              , -- 名单状态（0 正常，1 白名单，2 灰名单）
    whiteInfo.listType as list_type                , -- 名单类型(1 警用白名单，2 低空经济白名单，3 政务白名单， 4 多次黑飞黑名单， 5 重点人员黑名单)
    replace(replace(whiteInfo.gmtExpire,'T',' '),'.000+00:00','') as gmt_expire               , -- 过期时间
    if(whiteInfo.sn is not null,true,userInfo.realName) as real_name                , -- 是否实名（0 未实名，1已实名）
    replace(replace(coalesce(uavInfo.gmtRegister,userInfo.gmtRegister),'T',' '),'.000+00:00','') as gmt_register             , -- 实名注册时间
    whiteInfo.sn is not null as  is_white_list,
    userInfo.residence as residence                , -- 居住地
    b.purpose as purpose                   -- 用途
from jh_uav_info a
         left join dws_et_uav_info_source FOR SYSTEM_TIME AS OF a.proctime as b   -- 设备表 关联无人机
                   on a.uavInfo.sn = b.id;










-- -- 写入无人机数据表
insert into dwd_et_uav_jh_info
select
    uavInfo.id as id,
    uavInfo.type as type,
    uavInfo.typeName as type_name,
    uavInfo.fullName as full_name,
    uavInfo.sn as sn,
    uavInfo.uavModelName as uav_model_name,
    uavInfo.uavCompanyName as uav_company_name,
    uavInfo.gmtRegister as gmt_register,
    uavInfo.areaCode as area_code,
    uavInfo.address as address,
    uavInfo.username as username,
    uavInfo.cardCode as card_code,
    uavInfo.phone as phone,
    uavInfo.`delete` as deleted,
    uavInfo.gmtCreate as gmt_create,
    from_unixtime(unix_timestamp())  as update_time
from jh_uav_info
where uavInfo.id is not null;

-- -- 写入白名单数据表
insert into dwd_et_uav_jh_white_info
select
    whiteInfo.id as id,
    whiteInfo.sn as sn,
    whiteInfo.uavModelName as uav_model_name,
    whiteInfo.maxHeight as max_height,
    whiteInfo.engineType as engine_type,
    whiteInfo.companyName as company_name,
    whiteInfo.listStatus as list_status,
    whiteInfo.listType as list_type,
    whiteInfo.gmtExpire as gmt_expire,
    whiteInfo.`delete` as deleted,
    whiteInfo.gmtCreate as gmt_create,
    from_unixtime(unix_timestamp())  as update_time
from jh_uav_info
where whiteInfo.id is not null;

-- -- 写入用户数据表
insert into dwd_et_uav_jh_user_info
select
    userInfo.id as id,
    userInfo.username as username,
    userInfo.userTypeName as user_type_name,
    userInfo.cardTypeName as card_type_name,
    userInfo.phone as phone,
    userInfo.cardCode as card_code,
    userInfo.areaCode as area_code,
    userInfo.realName as real_name,
    userInfo.gmtRegister as gmt_register,
    userInfo.fullName as full_name,
    userInfo.residence as residence,
    userInfo.listStatus as list_status,
    userInfo.`delete` as deleted,
    userInfo.gmtCreate as gmt_create,
    from_unixtime(unix_timestamp())  as update_time
from jh_uav_info
where userInfo.id is not null;




insert into dws_et_uav_pilot_info
select
    cast(userInfo.id as string) as id,
    userInfo.userTypeName as a_method,
    coalesce(userInfo.fullName,uavInfo.fullName,whiteInfo.fullName,uavInfo.username,userInfo.username) as name,
    coalesce(uavInfo.cardCode,userInfo.cardCode) as card_code,
    coalesce(userInfo.phone,uavInfo.phone,whiteInfo.phone) as phone,
    cast(null as string  ) as email,
    uavInfo.address as address,
    coalesce(uavInfo.sn,whiteInfo.sn)  as register_uav,
    coalesce(userInfo.username,uavInfo.username) as username,
    userInfo.userTypeName as user_type_name,
    userInfo.cardTypeName as card_type_name,
    coalesce(whiteInfo.areaCode,uavInfo.areaCode,userInfo.areaCode)  as area_code,
    userInfo.realName as real_name,
    replace(replace(coalesce(uavInfo.gmtRegister,userInfo.gmtRegister),'T',' '),'.000+00:00','') as gmt_register,
    coalesce(userInfo.listStatus,whiteInfo.listStatus) as list_status,
    concat(
            nullif(coalesce(userInfo.fullName,uavInfo.fullName,whiteInfo.fullName,uavInfo.username,userInfo.username),''),' ',
            nullif(coalesce(uavInfo.cardCode,userInfo.cardCode),'')
        ) as search_content,
    from_unixtime(unix_timestamp()) as update_time
from jh_uav_info
where userInfo.id is not null;



insert into dws_rl_uav_pilot
select
    uavInfo.sn as uav_id , -- 无人机的id-sn号
    cast(userInfo.id as string) as pilot_id , -- 飞手
    from_unixtime(unix_timestamp())  as update_time  -- 更新时间
from jh_uav_info
where userInfo.id is not null;




end;



