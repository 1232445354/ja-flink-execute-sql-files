--********************************************************************--
-- author:      write your name here
-- create time: 2025/9/1 09:55:53
-- description: argo数据的详情数据入库
-- version: ja-argo-prof-info-v250901
--********************************************************************--
set 'pipeline.name' = 'ja-argo-prof-info';


set 'parallelism.default' = '1';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '600000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-argo-prof-info';


-- 数据去重复
create function get_distinct_udf as 'com.jingan.udf.argo.ArgoDistinctUdf';

-- 数据解压
create function get_decompress_udf as 'com.jingan.udf.argo.ArgoProDecompressDatafUdf';

-- 一次剖面的不同压力探测数据
create function get_prof_pres_udf as 'com.jingan.udf.argo.ArgoProfPressUdf';


-- 创建kafka来源的数据源表
create table argo_prof_kafka(
                                DATA_TYPE                     string, -- 数据类型
                                FORMAT_VERSION                string, -- 数据格式版本号
                                HANDBOOK_VERSION              string, -- 数据管理手册的版本号
                                REFERENCE_DATE_TIME           string, -- 时间参考基准点
                                DATE_CREATION                 string, -- 最初创建日期和时间
                                DATE_UPDATE                   string, -- 最后一次被更新的日期和时间
                                PLATFORM_NUMBER               string, -- 浮标的唯一标识符
                                PROJECT_NAME                  string, -- 负责部署和管理该浮标的科研项目或机构的名称
                                PI_NAME                       string, -- 首席研究员的姓名
                                STATION_PARAMETERS            string, -- 此浮标所测量的核心参数列表。
                                CYCLE_NUMBER                  string, -- 剖面循环号的列表
                                DIRECTION                     string, -- 剖面测量的方向
                                DATA_CENTRE                   string, -- 负责浮标数据处理的数据中心
                                DC_REFERENCE                  string, -- 数据中心的站点唯一标识符
                                DATA_STATE_INDICATOR          string, -- 数据状态标识符
                                DATA_MODE                     string, -- 数据模式
                                PLATFORM_TYPE                 string, -- 浮标的技术型号
                                FLOAT_SERIAL_NO               string, -- 浮标的序列号
                                FIRMWARE_VERSION              string, -- 浮标上核心传感器-通常是CTD-的固件版本号
                                WMO_INST_TYPE                 string, -- 世界气象组织-WMO-定义的仪器类型代码
                                JULD                          string, -- 核心时间变量
                                JULD_QC                       string, -- 时间数据的质量标识
                                JULD_LOCATION                 string, -- 定位时间
                                LATITUDE                      string, -- 剖面的纬度
                                LONGITUDE                     string, -- 剖面的经度
                                POSITION_QC                   string, -- 位置数据的质量标识
                                POSITIONING_SYSTEM            string, -- 用于定位的系统
                                VERTICAL_SAMPLING_SCHEME      string, -- 垂直采样方案的描述
                                CONFIG_MISSION_NUMBER         string, -- 任务配置编号
                                `PARAMETER`                   string, -- 哪些数据校准
                                SCIENTIFIC_CALIB_EQUATION     string, -- 应用于此参数的校准方程
                                SCIENTIFIC_CALIB_COEFFICIENT  string, -- 校准方程的系数
                                SCIENTIFIC_CALIB_COMMENT      string, -- 针对此参数校准的注释
                                SCIENTIFIC_CALIB_DATE         string, -- 校准执行的日期

                                HISTORY_INSTITUTION           string, -- 执行该处理操作的机构
                                HISTORY_STEP                  string, -- 数据处理步骤的名称
                                HISTORY_SOFTWARE              string, -- 执行此步骤的软件名称
                                HISTORY_SOFTWARE_RELEASE      string, -- 上述软件的版本号
                                HISTORY_REFERENCE             string, -- 处理所依据的参考数据库或文件。
                                HISTORY_DATE                  string, -- 此历史记录创建的日期和时间。
                                HISTORY_ACTION                string, -- 执行的操作
                                HISTORY_PARAMETER             string, -- 此操作所针对的参数
                                HISTORY_START_PRES            string, -- 操作所应用的起始压力（深度）。
                                HISTORY_STOP_PRES             string, -- 操作所应用的终止压力（深度）
                                HISTORY_PREVIOUS_VALUE        string, -- 操作执行前的参数或标志的原始值
                                HISTORY_QCTEST                string, -- 所执行的质量控制测试的文档

                                PROFILE_PRES_QC               string, -- 压力(深度)剖面的全局质量标志
                                PROFILE_TEMP_QC               string, -- 温度剖面的全局质量标志
                                PROFILE_PSAL_QC               string, -- 盐度剖面的全局质量标志
                                PRES                          string, -- 海水的实测压力
                                PRES_QC                       string, -- 实测压力值的质量标志
                                PRES_ADJUSTED                 string, -- 经过校正的压力值
                                PRES_ADJUSTED_QC              string, -- 校正后压力值的质量标志
                                PRES_ADJUSTED_ERROR           string, -- 校正后压力值的估计误差
                                TEMP                          string, -- 海水的实测温度
                                TEMP_QC                       string, -- 实测温度值的质量标志
                                TEMP_ADJUSTED                 string, -- 经过校正的温度值
                                TEMP_ADJUSTED_QC              string, -- 校正后温度值的质量标志
                                TEMP_ADJUSTED_ERROR           string, -- 校正后温度值的估计误差
                                PSAL                          string, -- 海水的实测实用盐度
                                PSAL_QC                       string, -- 实测盐度值的质量标志。
                                PSAL_ADJUSTED                 string, -- 经过校正的盐度值。
                                PSAL_ADJUSTED_QC              string, -- 校正后盐度值的质量标志
                                PSAL_ADJUSTED_ERROR           string, -- 校正后盐度值的估计误差

                                TEMP_STD                      string, -- 标准模式温度
                                TEMP_STD_QC                   string, -- 标准模式温度的质量标志
                                TEMP_MED                      string, -- 延时模式温度
                                TEMP_MED_QC                   string, -- 延时模式质量标志
                                PROFILE_TEMP_STD_QC           string, -- 标准模式剖面质量标志
                                PROFILE_TEMP_MED_QC           string, -- 延时模式剖面质量标志
                                PRES_MED                      string, -- 延时模式校正后的压力
                                PRES_MED_QC                   string, -- 延时模式压力的质量标志
                                PROFILE_PRES_MED_QC           string, -- 整个剖面的延时模式压力整体质量标志
                                PSAL_MED                      string, -- 延时模式校正后的盐度
                                PSAL_MED_QC                   string, -- 延时模式盐度的质量标志
                                PSAL_STD                      string, -- 标准模式（或实时模式）盐度
                                PSAL_STD_QC                   string, -- 标准模式盐度的质量标志
                                PROFILE_PSAL_STD_QC           string, -- 整个剖面的标准模式盐度整体质量标志
                                PROFILE_PSAL_MED_QC           string, -- 整个剖面的延时模式盐度整体质量标志
                                MTIME                         string, -- 测量时间
                                MTIME_QC                      string, -- 测量时间的质量标志
                                PROFILE_MTIME_QC              string  -- 整个剖面的测量时间整体质量标志
) with (
      'connector' = 'kafka',
      'topic' = 'argo-prof',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = 'kafka.idc.jing-an.com:30090',
      'properties.group.id' = 'argo-prof-group1',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- argo 剖面的基础信息表
create table dws_bhv_prof_base_info (
                                        id                              bigint, --     WMO编号
                                        cycle_number                    string, --     剖面循环号的列表
                                        acquire_juld                    string, --     剖面中心采样时刻
                                        acquire_juld_location           string, --     浮标浮出水面被卫星定位的时刻
                                        juld_qc                         string, --     时间数据的质量标识
                                        position_qc                     string, --     位置数据的质量标识
                                        latitude                        double, --     剖面的纬度
                                        longitude                       double, --     剖面的经度
                                        positioning_system              string, --     用于定位的系统
                                        direction                       string, --     剖面测量的方向
                                        data_type                       string, --     数据类型
                                        format_version                  string, --     数据格式版本号
                                        handbook_version                string, --     数据管理手册的版本号
                                        reference_date_time             string, --     时间参考基准点
                                        acquire_date_creation           string, --     最初创建日期和时间-北京时间
                                        acquire_date_update             string, --     最后一次被更新的日期和时间
                                        project_name                    string, --     负责部署和管理该浮标的科研项目或机构的名称
                                        pi_name                         string, --     首席研究员的姓名
                                        station_parameters              string, --     此浮标所测量的核心参数列表
                                        data_centre                     string, --     负责此浮标数据处理和分发的数据中心代码
                                        dc_reference                    string, --     负责此浮标数据处理和分发的数据中心代码
                                        data_state_indicator            string, --     数据状态标识符
                                        data_mode                       string, --     数据模式
                                        platform_type                   string, --     浮标的技术型号
                                        float_serial_no                 string, --     浮标的序列号
                                        firmware_version                string, --     浮标上核心传感器-通常是CTD的固件版本号
                                        wmo_inst_type                   string, --     世界气象组织-WMO-定义的仪器类型代码

                                        rofile_pres_qc                  string, -- 压力(深度)剖面的全局质量标志
                                        profile_temp_qc                 string, -- 温度剖面的全局质量标志
                                        profile_psal_qc                 string, -- 盐度剖面的全局质量标志
                                        profile_temp_std_qc             string, -- xxx的全局质量标志
                                        profile_temp_med_qc             string, -- xxx的全局质量标志
                                        profile_pres_med_qc             string, -- xxx的全局质量标志
                                        profile_psal_std_qc             string, -- xxx的全局质量标志
                                        profile_psal_med_qc             string, -- xxx的全局质量标志
                                        profile_mtime_qc                string, -- xxx的全局质量标志
                                        update_time                     string  --     数据入库时间
) with (
      'connector' = 'doris',
      -- 'fenodes' = '172.21.30.202:30030',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'ja_argo.dws_bhv_prof_base_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='2000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',  	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'  	 -- 行分隔符
      );



-- argo 剖面的基础信息表 - 无用字段存储
create table dws_bhv_prof_base_no_use_info (
                                               id                              bigint, --     WMO编号
                                               cycle_number                    string, --     剖面循环号的列表
                                               acquire_juld                    string, --     剖面中心采样时刻
                                               acquire_juld_location           string, --     浮标浮出水面被卫星定位的时刻

                                               vertical_sampling_scheme        string, --     垂直采样方案的描述
                                               config_mission_number           string, --     任务配置编号
                                               `parameter_name`                string, --     数据校准
                                               scientific_calib_equation       string, --     应用于此参数的校准方程
                                               scientific_calib_coefficient    string, --     校准方程的系数
                                               scientific_calib_               string, --     针对此参数校准的注释
                                               scientific_calib_date           string, --     校准执行的日期
                                               history_institution             string, --     执行此处理步骤的机构
                                               history_step                    string, --     数据处理步骤的名称
                                               history_software                string, --     执行此步骤的软件名称
                                               history_software_release        string, --     上述软件的版本号
                                               history_reference               string, --     处理所依据的参考数据库或文件
                                               history_date                    string, --     此历史记录创建的日期和时间
                                               history_action                  string, --     执行的操作
                                               history_parameter               string, --     此操作所针对的参数
                                               history_start_pres              string, --     操作所应用的起始压力（深度）
                                               history_stop_pres               string, --     操作所应用的终止压力（深度）
                                               history_previous_value          string, --     操作执行前的参数或标志的原始值
                                               history_qctest                  string, --     所执行的质量控制测试的文档
                                               update_time                     string  --     数据入库时间
) with (
      'connector' = 'doris',
      -- 'fenodes' = '172.21.30.202:30030',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'ja_argo.dws_bhv_prof_base_no_use_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='2000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',  	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'  	 -- 行分隔符
      );





-- 剖面综合监测指标表
create table dws_bhv_prof_pres_info(
                                       id                          bigint         comment 'WMO编号',
                                       cycle_number                string         comment '剖面循环号的列表',
                                       pres                        string         comment '海水的实测压力',
                                       index_no                    bigint         comment '索引顺序',
                                       pres_qc                     string         comment '实测压力值的质量标志',
                                       pres_adjusted               string         comment '经过校正的压力值',
                                       pres_adjusted_qc            string         comment '校正后压力值的质量标志',
                                       pres_adjusted_error         string         comment '校正后压力值的估计误差',
                                       temp                        string         comment '实测温度',
                                       temp_qc                     string         comment '实时质量标志',
                                       temp_adjusted               string         comment '经过校正的温度值',
                                       temp_adjusted_qc            string         comment '校正后温度值的质量标志',
                                       temp_adjusted_error         string         comment '校正后温度值的估计误差',
                                       psal                        string         comment '海水的实测实用盐度',
                                       psal_qc                     string         comment '实测盐度值的质量标志。',
                                       psal_adjusted               string         comment '经过校正的盐度值',
                                       psal_adjusted_qc            string         comment '校正后盐度值的质量标志',
                                       psal_adjusted_error         string         comment '校正后盐度值的估计误差',
                                       temp_std                    string         comment '',
                                       temp_std_qc                 string         comment '',
                                       temp_med                    string         comment '',
                                       temp_med_qc                 string         comment '',
                                       pres_med                    string         comment '',
                                       pres_med_qc                 string         comment '',
                                       psal_med                    string         comment '',
                                       psal_med_qc                 string         comment '',
                                       psal_std                    string         comment '',
                                       psal_std_qc                 string         comment '',
                                       mtime                       double         comment '测量时间',
                                       mtime_qc                    string         comment '该时间的质量QC',
                                       acquire_mtime               string         comment '测量时间 - 格式化北京时间',
                                       update_time                 string         comment '数据入库时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = '172.21.30.202:30030',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'ja_argo.dws_bhv_prof_pres_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='2000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',  	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'  	 -- 行分隔符
      );



--  *************************************************************** 维表



-- ************************************** 传感器、参数、配置 数据写入中转kafka ***************************************************************

-- create table argo_prof_transfer(
--   id                  string,
--   cycle_number        string,
--   prof_pres_array     ARRAY<
--     Row<
--          pres_name                    string,
--          pres_qc_name                 string,
--          pres_adjusted_name           string,
--          pres_adjusted_qc_name        string,
--          pres_adjusted_error_name     string,
--          temp_name                    string,
--          temp_qc_name                 string,
--          temp_adjusted_name           string,
--          temp_adjusted_qc_name        string,
--          temp_adjusted_error_name     string,
--          psal_name                    string,
--          psal_qc_name                 string,
--          psal_adjusted_name           string,
--          psal_adjusted_qc_name        string,
--          psal_adjusted_error_name     string,
--          mtime_name                   double,
--          mtime_qc_name                string,
--          index_no                     string
--       >
--    >
-- ) with (
--   'connector' = 'kafka',
--   'topic' = 'argo_prof_transfer',
--   'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
--   'properties.group.id' = 'argo_prof_transfer_group1',
--   'format' = 'json',
--     -- 'scan.startup.mode' = 'group-offsets',
--   'scan.startup.mode' = 'latest-offset',
--   -- 'scan.startup.mode' = 'timestamp',
--   -- 'scan.startup.timestamp-millis' = '0',
--   'json.fail-on-missing-field' = 'false',
--   'json.ignore-parse-errors' = 'true'
--   -- 'key.format' = 'json',
--   -- 'key.fields' = 'id'
-- );


-----------------------

-- 数据处理

-----------------------

-- 去除两侧空格字段
create view tmp_01 as
select
    trim(split_index(trim(PLATFORM_NUMBER),',',0))  as id,
    trim(DATA_TYPE)                                 as data_type,
    trim(FORMAT_VERSION)                            as format_version,
    trim(HANDBOOK_VERSION)                          as handbook_version,
    trim(REFERENCE_DATE_TIME)                       as reference_date_time,
    trim(DATE_CREATION)                             as date_creation,
    trim(DATE_UPDATE)                               as date_update,

    trim(split_index(PROJECT_NAME,',',0))           as project_name,
    trim(split_index(PI_NAME,',',0))                as pi_name,
    trim(STATION_PARAMETERS)                        as station_parameters,
    trim(split_index(CYCLE_NUMBER,' ',0))           as cycle_number,  -- 99999
    substring(trim(DIRECTION),1,1)                  as direction,
    trim(split_index(DATA_CENTRE,',',0))            as data_centre,
    trim(split_index(DC_REFERENCE,',',0))           as dc_reference,
    trim(split_index(DATA_STATE_INDICATOR,',',0))   as data_state_indicator,
    substring(trim(DATA_MODE),1,1)                   as data_mode,
    trim(split_index(PLATFORM_TYPE,',',0))          as platform_type,
    trim(split_index(FLOAT_SERIAL_NO,',',0))        as float_serial_no,
    trim(split_index(FIRMWARE_VERSION,',',0))       as firmware_version,
    trim(split_index(WMO_INST_TYPE,',',0))          as wmo_inst_type,
    trim(JULD_QC)                                   as juld_qc,

    trim(split_index(trim(JULD),' ',0))             as juld,           -- 999999.0
    trim(split_index(trim(JULD_LOCATION),' ',0))    as juld_location,  -- 999999.0
    trim(split_index(trim(LATITUDE),' ',0))         as latitude,       -- 99999.0
    trim(split_index(trim(LONGITUDE),' ',0))        as longitude,      -- 99999.0

    trim(POSITION_QC)                               as position_qc,
    trim(split_index(POSITIONING_SYSTEM,',',0))     as positioning_system,
    trim(VERTICAL_SAMPLING_SCHEME)                  as vertical_sampling_scheme,
    trim(CONFIG_MISSION_NUMBER)                     as config_mission_number,      -- 99999
    trim(`PARAMETER`)                               as parameter_name,
    trim(SCIENTIFIC_CALIB_EQUATION)                 as scientific_calib_equation,
    trim(SCIENTIFIC_CALIB_COEFFICIENT)              as scientific_calib_coefficient,
    trim(SCIENTIFIC_CALIB_COMMENT)                  as scientific_calib_comment,
    trim(SCIENTIFIC_CALIB_DATE)                     as scientific_calib_date,
    -- 直接入库-加密数据
    HISTORY_INSTITUTION                            as history_institution,
    HISTORY_STEP                                   as history_step,
    HISTORY_SOFTWARE                               as history_software,
    HISTORY_SOFTWARE_RELEASE                       as history_software_release,
    HISTORY_REFERENCE                              as history_reference,
    HISTORY_DATE                                   as history_date,
    HISTORY_ACTION                                 as history_action,
    HISTORY_PARAMETER                              as history_parameter,
    HISTORY_START_PRES                             as history_start_pres,
    HISTORY_STOP_PRES                              as history_stop_pres,
    HISTORY_PREVIOUS_VALUE                         as history_previous_value,
    HISTORY_QCTEST                                 as history_qctest,
    -- 加密数据，处理的
    PROFILE_PRES_QC               as profile_pres_qc,
    PROFILE_TEMP_QC               as profile_temp_qc,
    PROFILE_PSAL_QC               as profile_psal_qc,
    PRES                          as pres,
    PRES_QC                       as pres_qc,
    PRES_ADJUSTED                 as pres_adjusted,
    PRES_ADJUSTED_QC              as pres_adjusted_qc,
    PRES_ADJUSTED_ERROR           as pres_adjusted_error,
    TEMP                          as temp,
    TEMP_QC                       as temp_qc,
    TEMP_ADJUSTED                 as temp_adjusted,
    TEMP_ADJUSTED_QC              as temp_adjusted_qc,
    TEMP_ADJUSTED_ERROR           as temp_adjusted_error,
    PSAL                          as psal,
    PSAL_QC                       as psal_qc,
    PSAL_ADJUSTED                 as psal_adjusted,
    PSAL_ADJUSTED_QC              as psal_adjusted_qc,
    PSAL_ADJUSTED_ERROR           as psal_adjusted_error,
    TEMP_STD                      as temp_std,
    TEMP_STD_QC                   as temp_std_qc,
    TEMP_MED                      as temp_med,
    TEMP_MED_QC                   as temp_med_qc,
    PROFILE_TEMP_STD_QC           as profile_temp_std_qc,
    PROFILE_TEMP_MED_QC           as profile_temp_med_qc,
    PRES_MED                      as pres_med,
    PRES_MED_QC                   as pres_med_qc,
    PROFILE_PRES_MED_QC           as profile_pres_med_qc,
    PSAL_MED                      as psal_med,
    PSAL_MED_QC                   as psal_med_qc,
    PSAL_STD                      as psal_std,
    PSAL_STD_QC                   as psal_std_qc,
    PROFILE_PSAL_STD_QC           as profile_psal_std_qc,
    PROFILE_PSAL_MED_QC           as profile_psal_med_qc,
    MTIME                         as mtime,
    MTIME_QC                      as mtime_qc,
    PROFILE_MTIME_QC              as profile_mtime_qc
from argo_prof_kafka;



-- 转换字段，判断空值null
create view tmp_02 as
select
    if(id = '',cast(null as varchar),id)            as id,
    if(data_type = '',cast(null as varchar),data_type)                        as data_type,
    if(format_version = '',cast(null as varchar),format_version)              as format_version,
    if(handbook_version = '',cast(null as varchar),handbook_version)          as handbook_version,
    if(reference_date_time = '',cast(null as varchar),reference_date_time)    as reference_date_time,
    if(date_creation = '',cast(null as varchar),date_creation)                as date_creation,
    if(date_update = '',cast(null as varchar),date_update)                    as date_update,
    if(project_name = '',cast(null as varchar),project_name)                  as project_name,
    if(pi_name = '',cast(null as varchar),pi_name)                            as pi_name,
    if(station_parameters = '',cast(null as varchar),station_parameters)      as station_parameters,
    if(cycle_number in ('','99999'),cast(null as varchar),cycle_number)       as cycle_number,
    if(direction = '',cast(null as varchar),direction)                        as direction,
    if(data_centre = '',cast(null as varchar),data_centre)                    as data_centre,
    if(dc_reference = '',cast(null as varchar),dc_reference)                  as dc_reference,
    if(data_state_indicator = '',cast(null as varchar),data_state_indicator)  as data_state_indicator,
    if(data_mode = '',cast(null as varchar),data_mode)                        as data_mode,
    if(platform_type = '',cast(null as varchar),platform_type)                as platform_type,
    if(float_serial_no = '',cast(null as varchar),float_serial_no)            as float_serial_no,
    if(firmware_version = '',cast(null as varchar),firmware_version)          as firmware_version,
    if(wmo_inst_type = '',cast(null as varchar),wmo_inst_type)                as wmo_inst_type,
    if(juld_qc = '',cast(null as varchar),juld_qc)                            as juld_qc,

    if(juld in('','9¥'),cast(null as double),cast(juld as double))                    as juld,
    if(juld_location in('','9¥'),cast(null as double),cast(juld_location as double)) as juld_location,
    if(latitude in('','¥'),cast(null as double),cast(latitude as double))             as latitude,
    if(longitude in('','¥'),cast(null as double),cast(longitude as double))           as longitude,

    if(position_qc = '',cast(null as varchar),position_qc)                                    as position_qc,
    if(positioning_system = '',cast(null as varchar),positioning_system)                      as positioning_system,
    if(vertical_sampling_scheme = '',cast(null as varchar),vertical_sampling_scheme)          as vertical_sampling_scheme,
    if(config_mission_number in ('','99999'),cast(null as varchar),config_mission_number)     as config_mission_number,
    if(parameter_name = '',cast(null as varchar),parameter_name)                              as parameter_name,
    if(scientific_calib_equation = '',cast(null as varchar),scientific_calib_equation)        as scientific_calib_equation,
    if(scientific_calib_coefficient = '',cast(null as varchar),scientific_calib_coefficient)  as scientific_calib_coefficient,
    if(scientific_calib_comment = '',cast(null as varchar),scientific_calib_comment)          as scientific_calib_comment,
    if(scientific_calib_date = '',cast(null as varchar),scientific_calib_date)                as scientific_calib_date,

    -- 加密的数据-直接入库
    history_institution           ,
    history_step                  ,
    history_software              ,
    history_software_release      ,
    history_reference             ,
    history_date                  ,
    history_action                ,
    history_parameter             ,
    history_start_pres            ,
    history_stop_pres             ,
    history_previous_value        ,
    history_qctest                ,

    profile_pres_qc     ,
    profile_temp_qc     ,
    profile_psal_qc     ,

    pres                ,
    pres_qc             ,
    pres_adjusted       ,
    pres_adjusted_qc    ,
    pres_adjusted_error ,
    temp                ,
    temp_qc             ,
    temp_adjusted       ,
    temp_adjusted_qc    ,
    temp_adjusted_error ,
    psal                ,
    psal_qc             ,
    psal_adjusted       ,
    psal_adjusted_qc    ,
    psal_adjusted_error ,

    temp_std,
    temp_std_qc,
    temp_med,
    temp_med_qc,
    profile_temp_std_qc,
    profile_temp_med_qc,
    pres_med,
    pres_med_qc,
    profile_pres_med_qc,
    psal_med,
    psal_med_qc,
    psal_std,
    psal_std_qc,
    profile_psal_std_qc,
    profile_psal_med_qc,
    mtime,
    mtime_qc,
    profile_mtime_qc
from tmp_01;


-- 处理时间字段,过滤id不为空数据,udf函数处理传感器数据
create view tmp_03 as
select
    id,
    data_type,
    format_version,
    handbook_version,
    reference_date_time,
    project_name,
    pi_name,
    get_distinct_udf(station_parameters,',')                  as station_parameters,
    cycle_number,
    direction,
    data_centre,
    dc_reference,
    data_state_indicator,
    data_mode,
    platform_type,
    float_serial_no,
    firmware_version,
    wmo_inst_type,
    juld_qc,

    juld,
    juld_location,
    latitude,
    longitude,

    position_qc,
    positioning_system,
    vertical_sampling_scheme,
    config_mission_number,
    get_distinct_udf(parameter_name,',')  as parameter_name,
    scientific_calib_equation     ,
    scientific_calib_coefficient  ,
    scientific_calib_comment      ,
    scientific_calib_date         ,
    history_institution           ,
    history_step                  ,
    history_software              ,
    history_software_release      ,
    history_reference             ,
    history_date                  ,
    history_action                ,
    history_parameter             ,
    history_start_pres            ,
    history_stop_pres             ,
    history_previous_value        ,
    history_qctest                ,
    -- 需要函数处理
    get_decompress_udf(profile_pres_qc)     as profile_pres_qc,
    get_decompress_udf(profile_temp_qc)     as profile_temp_qc,
    get_decompress_udf(profile_psal_qc)     as profile_psal_qc,
    get_decompress_udf(profile_temp_std_qc) as profile_temp_std_qc,
    get_decompress_udf(profile_temp_med_qc) as profile_temp_med_qc,
    get_decompress_udf(profile_pres_med_qc) as profile_pres_med_qc,
    get_decompress_udf(profile_psal_std_qc) as profile_psal_std_qc,
    get_decompress_udf(profile_psal_med_qc) as profile_psal_med_qc,
    get_decompress_udf(profile_mtime_qc)    as profile_mtime_qc,
    pres                          ,
    pres_qc                       ,
    pres_adjusted                 ,
    pres_adjusted_qc              ,
    pres_adjusted_error           ,
    temp                          ,
    temp_qc                       ,
    temp_adjusted                 ,
    temp_adjusted_qc              ,
    temp_adjusted_error           ,
    psal                          ,
    psal_qc                       ,
    psal_adjusted                 ,
    psal_adjusted_qc              ,
    psal_adjusted_error           ,
    temp_std,
    temp_std_qc,
    temp_med,
    temp_med_qc,
    pres_med,
    pres_med_qc,
    psal_med,
    psal_med_qc,
    psal_std,
    psal_std_qc,
    mtime,
    mtime_qc,
    CONVERT_TZ(FROM_UNIXTIME(UNIX_TIMESTAMP(date_creation, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss') ,'UTC', 'Asia/Shanghai')AS acquire_date_creation,
    CONVERT_TZ(FROM_UNIXTIME(UNIX_TIMESTAMP(date_update, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss') ,'UTC', 'Asia/Shanghai')  AS acquire_date_update,
    -- juld
    CONVERT_TZ(
            cast(timestampadd(
                second,
                    cast((juld - floor(juld)) * 86400 as int),
                    timestampadd(day,cast(floor(juld) as int),TO_TIMESTAMP('1950-01-01 00:00:00','yyyy-MM-dd HH:mm:ss'))
                ) as string) ,
            'UTC',
            'Asia/Shanghai'
        ) as acquire_juld,

    -- juld_location
    CONVERT_TZ(
            cast(timestampadd(
                second,
                    cast((juld_location - floor(juld_location)) * 86400 as int),
                    timestampadd(day,cast(floor(juld_location) as int),TO_TIMESTAMP('1950-01-01 00:00:00','yyyy-MM-dd HH:mm:ss'))
                ) as string) ,
            'UTC',
            'Asia/Shanghai'
        ) as acquire_juld_location
from tmp_02
where id is not null;





-- 数据处理 温度、压力、盐度
create view tmp_pres_01 as
select
    id,
    cycle_number,
    get_prof_pres_udf(
            cast(juld as varchar)  ,
            pres                          ,
            pres_qc                       ,
            pres_adjusted                 ,
            pres_adjusted_qc              ,
            pres_adjusted_error           ,
            temp                          ,
            temp_qc                       ,
            temp_adjusted                 ,
            temp_adjusted_qc              ,
            temp_adjusted_error           ,
            psal                          ,
            psal_qc                       ,
            psal_adjusted                 ,
            psal_adjusted_qc              ,
            psal_adjusted_error           ,
            mtime,
            mtime_qc
        ) as prof_pres_array

    -- temp_std,
    -- temp_std_qc,
    -- temp_med,
    -- temp_med_qc,
    -- pres_med,
    -- pres_med_qc,
    -- psal_med,
    -- psal_med_qc,
    -- psal_std,
    -- psal_std_qc,
from tmp_02
where id is not null;




-- 传感器数据处理
create view tmp_prof_pres_01 as
select
    cast(t1.id as bigint) as id,
    t1.cycle_number,
    t2.pres_name                  as pres,
    t2.pres_qc_name               as pres_qc,
    t2.pres_adjusted_name         as pres_adjusted,
    t2.pres_adjusted_qc_name      as pres_adjusted_qc,
    t2.pres_adjusted_error_name   as pres_adjusted_error,
    t2.temp_name                  as temp,
    t2.temp_qc_name               as temp_qc,
    t2.temp_adjusted_name         as temp_adjusted,
    t2.temp_adjusted_qc_name      as temp_adjusted_qc,
    t2.temp_adjusted_error_name   as temp_adjusted_error,
    t2.psal_name                  as psal,
    t2.psal_qc_name               as psal_qc,
    t2.psal_adjusted_name         as psal_adjusted,
    t2.psal_adjusted_qc_name      as psal_adjusted_qc,
    t2.psal_adjusted_error_name   as psal_adjusted_error,
    if(t2.mtime_name = 0,cast(null as double),t2.mtime_name) as mtime,
    if(
                t2.mtime_name = 0,
                cast(null as varchar),
                CONVERT_TZ(
                        cast(timestampadd(
                            second,
                                cast((mtime_name - floor(mtime_name)) * 86400 as int),
                                timestampadd(day,cast(floor(mtime_name) as int),TO_TIMESTAMP('1950-01-01 00:00:00','yyyy-MM-dd HH:mm:ss'))
                            ) as string) ,
                        'UTC',
                        'Asia/Shanghai')
        )  as acquire_mtime,
    t2.mtime_qc_name            as mtime_qc,
    cast(t2.index_no as bigint) as     index_no
from tmp_pres_01 as t1
         cross join unnest (prof_pres_array) as t2 (
                                                    pres_name               ,
                                                    pres_qc_name            ,
                                                    pres_adjusted_name      ,
                                                    pres_adjusted_qc_name   ,
                                                    pres_adjusted_error_name,
                                                    temp_name               ,
                                                    temp_qc_name            ,
                                                    temp_adjusted_name      ,
                                                    temp_adjusted_qc_name   ,
                                                    temp_adjusted_error_name,
                                                    psal_name               ,
                                                    psal_qc_name            ,
                                                    psal_adjusted_name      ,
                                                    psal_adjusted_qc_name   ,
                                                    psal_adjusted_error_name,
                                                    mtime_name              ,
                                                    mtime_qc_name           ,
                                                    index_no
    );




begin statement set;


-- 剖面数据入库基础表
insert into dws_bhv_prof_base_info
select
    cast(id as bigint) as id        ,
    cycle_number                    ,
    acquire_juld                    ,
    acquire_juld_location           ,
    juld_qc                         ,
    position_qc                     ,
    latitude                        ,
    longitude                       ,
    positioning_system              ,
    direction                       ,
    data_type                       ,
    format_version                  ,
    handbook_version                ,
    reference_date_time             ,
    acquire_date_creation           ,
    acquire_date_update             ,
    project_name                    ,
    pi_name                         ,
    station_parameters              ,
    data_centre                     ,
    dc_reference                    ,
    data_state_indicator            ,
    data_mode                       ,
    platform_type                   ,
    float_serial_no                 ,
    firmware_version                ,
    wmo_inst_type                   ,

    profile_pres_qc,
    profile_temp_qc,
    profile_psal_qc,
    profile_temp_std_qc,
    profile_temp_med_qc,
    profile_pres_med_qc,
    profile_psal_std_qc,
    profile_psal_med_qc,
    profile_mtime_qc,
    from_unixtime(unix_timestamp()) as update_time   -- 更新时间
from tmp_03;




-- 剖面数据入库 - 无用的字段数据表
insert into dws_bhv_prof_base_no_use_info
select
    cast(id as bigint) as id        ,
    cycle_number                    ,
    acquire_juld                    ,
    acquire_juld_location           ,

    vertical_sampling_scheme,
    config_mission_number,
    `parameter_name`,
    scientific_calib_equation,
    scientific_calib_coefficient,
    scientific_calib_comment,
    scientific_calib_date,
    history_institution,
    history_step,
    history_software,
    history_software_release,
    history_reference,
    history_date,
    history_action,
    history_parameter,
    history_start_pres,
    history_stop_pres,
    history_previous_value,
    history_qctest,
    from_unixtime(unix_timestamp()) as update_time   -- 更新时间
from tmp_03;



-- 剖面数据综合监测数据 入库doris
insert into dws_bhv_prof_pres_info
select
    id                          ,
    cycle_number                ,
    pres                        ,
    index_no,
    pres_qc                     ,
    pres_adjusted               ,
    pres_adjusted_qc            ,
    pres_adjusted_error         ,
    temp                        ,
    temp_qc                     ,
    temp_adjusted               ,
    temp_adjusted_qc            ,
    temp_adjusted_error         ,
    psal                        ,
    psal_qc                     ,
    psal_adjusted               ,
    psal_adjusted_qc            ,
    psal_adjusted_error         ,
    cast(null as varchar) as temp_std                    ,
    cast(null as varchar) as temp_std_qc                 ,
    cast(null as varchar) as temp_med                    ,
    cast(null as varchar) as temp_med_qc                 ,
    cast(null as varchar) as pres_med                    ,
    cast(null as varchar) as pres_med_qc                 ,
    cast(null as varchar) as psal_med                    ,
    cast(null as varchar) as psal_med_qc                 ,
    cast(null as varchar) as psal_std                    ,
    cast(null as varchar) as psal_std_qc                 ,
    mtime                       ,
    mtime_qc                    ,
    acquire_mtime               ,
    from_unixtime(unix_timestamp()) as update_time   -- 更新时间
from  tmp_prof_pres_01;

end;



