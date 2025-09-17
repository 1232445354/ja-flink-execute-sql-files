--********************************************************************--
-- author:      write your name here
-- create time: 2025/9/1 09:55:53
-- description: argo数据的详情数据入库
-- version: ja-argo-entity-info-v250901
--********************************************************************--
set 'pipeline.name' = 'ja-argo-entity-info';


set 'parallelism.default' = '1';
set 'execution.type' = 'streaming';
set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '600000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '120000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-argo-entity-info';


-- 计算sensor传感器函数
create function get_sensor_udf as 'com.jingan.udf.argo.ArgoSensorInfoUdf';

-- 计算物理参数信息参数函数
create function get_parameter_udf as 'com.jingan.udf.argo.ArgoParameterInfoUdf';

-- 计算浮标配置参数信息函数
create function get_config_udf as 'com.jingan.udf.argo.ArgoConfigInfoUdf';


-- 创建kafka来源的数据源表
create table argo_meta_kafka(
                                DATA_TYPE                             string, -- 数据类型
                                FORMAT_VERSION                        string, -- 格式版本
                                HANDBOOK_VERSION                      string, -- 手册版本
                                DATE_CREATION                         string, -- 创建日期
                                DATE_UPDATE                           string, -- 更新日期
                                PLATFORM_NUMBER                       string, -- 浮标号
                                PTT                                   string, -- 发射器标识
                                TRANS_SYSTEM                          string, -- 通信方式 - 卫星-铱
                                TRANS_SYSTEM_ID                       string, -- 传输系统使用的程序标识符
                                TRANS_FREQUENCY                       string, -- 浮标的发射频率
                                POSITIONING_SYSTEM                    string, -- 定位系统
                                PLATFORM_FAMILY                       string, -- 平台系列
                                PLATFORM_TYPE                         string, -- 浮标型号-平台类型
                                PLATFORM_MAKER                        string, -- 平台制造商名称
                                FIRMWARE_VERSION                      string, -- 浮标的固件版本
                                MANUAL_VERSION                        string, -- 浮标的手册版本
                                FLOAT_SERIAL_NO                       string, -- 浮标序列号
                                STANDARD_FORMAT_ID                    string, -- 标准格式编号ID
                                DAC_FORMAT_ID                         string, -- DAC使用的格式编号
                                WMO_INST_TYPE                         string, -- WMO编码的仪器类型。
                                PROJECT_NAME                          string, -- 部署浮标的项目名称
                                DATA_CENTRE                           string, -- 负责浮标实时数据处理的数据中心
                                PI_NAME                               string, -- 首席研究员姓名
                                ANOMALY                               string, -- 异常情况
                                BATTERY_TYPE                          string, -- 电池类型
                                BATTERY_PACKS                         string, -- 电池组配置
                                CONTROLLER_BOARD_TYPE_PRIMARY         string, -- 主控制器板主类型
                                CONTROLLER_BOARD_TYPE_SECONDARY       string, -- 副控制器板类型
                                CONTROLLER_BOARD_SERIAL_NO_PRIMARY    string, -- 主控制器板序列号
                                CONTROLLER_BOARD_SERIAL_NO_SECONDARY  string, -- 副控制器板序列号
                                SPECIAL_FEATURES                      string, -- 浮标的特殊功能
                                FLOAT_OWNER                           string, -- 浮标所者
                                OPERATING_INSTITUTION                 string, -- 浮标的运营机构
                                CUSTOMISATION                         string, -- 浮标定制信息
                                LAUNCH_DATE                           string, -- 部署日期和时间（UTC）。
                                LAUNCH_LATITUDE                       string, -- 部署点的纬度/经度
                                LAUNCH_LONGITUDE                      string, -- 部署点的纬度/经度
                                LAUNCH_QC                             string, -- 发射日期、时间和位置的质量标志
                                START_DATE                            string, -- 首次下潜日期和时间（UTC）。
                                START_DATE_QC                         string, -- 开始日期的质量标志
                                STARTUP_DATE                          string, -- 浮标激活日期和时间（UTC）。
                                STARTUP_DATE_QC                       string, -- 启动日期的质量标志。
                                DEPLOYMENT_PLATFORM                   string, -- 部署平台-投放船
                                DEPLOYMENT_CRUISE_ID                  string, -- 部署航次ID-航次编号
                                DEPLOYMENT_REFERENCE_STATION_ID       string, -- 部署可用的配置文件ID
                                END_MISSION_DATE                      string, -- 任务结束日期 / 任务结束状态
                                END_MISSION_STATUS                    string, -- 任务结束日期 / 任务结束状态
                                LAUNCH_CONFIG_PARAMETER_NAME          string, -- 发射时的配置参数名 / 发射时的配置参数值
                                LAUNCH_CONFIG_PARAMETER_VALUE         string, -- 发射时的配置参数名 / 发射时的配置参数值
                                CONFIG_PARAMETER_NAME                 string, -- （当前）配置参数名 / （当前）配置参数值
                                CONFIG_PARAMETER_VALUE                string, -- （当前）配置参数名 / （当前）配置参数值
                                CONFIG_MISSION_NUMBER                 string, -- 任务配置编号
                                CONFIG_MISSION_COMMENT                string, -- 配置说明。
                                SENSOR                                string, -- 浮标上安装的传感器名称
                                SENSOR_MAKER                          string, -- 传感器制造商名称
                                SENSOR_MODEL                          string, -- 传感器型号
                                SENSOR_SERIAL_NO                      string, -- 传感器序列号
                                `PARAMETER`                           string, -- 参数名称
                                PARAMETER_SENSOR                      string, -- 测量该参数的传感器名称
                                PARAMETER_UNITS                       string, -- 参数的单位
                                PARAMETER_ACCURACY                    string, -- 参数的准确度
                                PARAMETER_RESOLUTION                  string, -- 参数的分辨率
                                PREDEPLOYMENT_CALIB_EQUATION          string, -- 参数预部署校准方程
                                PREDEPLOYMENT_CALIB_COEFFICIENT       string, -- 部署前校准系数
                                PREDEPLOYMENT_CALIB_COMMENT           string -- 部署前校准的注释
) with (
      'connector' = 'kafka',
      'topic' = 'argo-meta',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.group.id' = 'argo-meta-group1',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- 创建kafka来源的数据源表 - 综合分析chart图
create table argo_chart_kafka(
                                 id        string,
                                 `key`     string,
                                 url       string,
                                 flag      string
) with (
      'connector' = 'kafka',
      'topic' = 'argo-sec-ove-chart',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '172.21.30.105:30090',
      'properties.group.id' = 'argo-sec-ove-chart-group1',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



-- argo实体表
create table dws_et_argo_info (
                                  id                                    bigint  comment 'argo-WMO编号',
                                  data_type                             string  comment '数据类型',
                                  format_version                        string  comment '格式版本',
                                  handbook_version                      string  comment '手册版本',
                                  acquire_date_creation                 string  comment '创建日期',
                                  acquire_date_update                   string  comment '更新日期',
                                  ptt                                   string  comment '平台信息-发射器标识',
                                  trans_system                          string  comment '平台信息-通信方式 - 卫星-铱',
                                  trans_system_e_name                   string  comment '平台信息-通信方式 - 英文描述',
                                  trans_system_c_name                   string  comment '平台信息-通信方式 - 中文描述',
                                  trans_system_id                       string  comment '平台信息-传输系统使用的程序标识符',
                                  trans_frequency                       string  comment '任务参数-浮标的发射频率',
                                  positioning_system                    string  comment '任务参数-定位系统',
                                  positioning_system_name               string  comment '定位系统描述',
                                  platform_family                       string  comment '平台信息-平台系列',
                                  platform_type                         string  comment '平台信息-浮标型号 - 平台类型',
                                  platform_maker                        string  comment '平台信息-平台制造商名称',
                                  platform_maker_e_name                 string  comment '平台制造商名称-英文描述',
                                  platform_maker_c_name                 string  comment '平台制造商名称-中文描述',
                                  firmware_version                      string  comment '平台信息-浮标的固件版本',
                                  manual_version                        string  comment '平台信息-浮标的手册版本',
                                  float_serial_no                       string  comment '平台信息-浮标序列号',
                                  standard_format_id                    string  comment '平台信息-标准格式编号ID',
                                  dac_format_id                         string  comment '其他-DAC使用的格式编号',
                                  wmo_inst_type                         string  comment '平台信息-WMO编码的仪器类型',
                                  wmo_inst_type_e_name                  string  comment 'WMO编码的仪器类型-对应英文',
                                  wmo_inst_type_c_name                  string  comment 'WMO编码的仪器类型-对应中文',
                                  project_name                          string  comment '项目信息属性-部署浮标的项目名称',
                                  data_centre                           string  comment '项目信息属性-负责浮标实时数据处理的数据中心',
                                  data_centre_e_name                    string  comment '处理的数据中心-英文名称',
                                  data_centre_c_name                    string  comment '处理的数据中心-中文名称',
                                  data_centre_country_name              string  comment '处理的数据中心-国家名称',
                                  data_centre_country_code              string  comment '处理的数据中心-国家代码',
                                  pi_name                               string  comment '项目信息属性-首席研究员姓名',
                                  anomaly                               string  comment '其他-异常情况',
                                  battery_type                          string  comment '平台信息-电池类型',
                                  battery_packs                         string  comment '平台信息-电池组配置',
                                  controller_board_type_primary         string  comment '平台信息-主控制器板主类型',
                                  controller_board_type_secondary       string  comment '平台信息-副控制器板类型',
                                  controller_board_serial_no_primary    string  comment '平台信息-主控制器板序列号',
                                  controller_board_serial_no_secondary  string  comment '平台信息-副控制器板序列号',
                                  special_features                      string  comment '平台信息-浮标的特殊功能',
                                  float_owner                           string  comment '项目信息属性-浮标所者',
                                  operating_institution                 string  comment '项目信息属性-浮标的运营机构',
                                  customisation                         string  comment '平台信息-浮标定制信息',
                                  acquire_launch_date                   string        comment '部署信息-部署日期和时间',
                                  launch_latitude                       double        comment '部署信息-部署点的纬度/经度',
                                  launch_longitude                      double        comment '部署信息-部署点的纬度/经度',
                                  launch_qc                             string  comment '其他-发射日期、时间和位置的质量标志',
                                  acquire_start_date                    string        comment '其他-首次下潜日期和时间',
                                  start_date_qc                         string  comment '其他-开始日期的质量标志',
                                  acquire_startup_date                  string  comment '部署信息-浮标激活日期和时间',
                                  startup_date_qc                       string  comment '启动日期的质量标志',
                                  deployment_platform                   string  comment '部署信息-部署平台-投放船',
                                  deployment_cruise_id                  string  comment '部署航次ID-航次编号',
                                  deployment_reference_station_id       string  comment '部署信息-部署可用的配置文件ID',
                                  acquire_end_mission_date              string  comment '其他-任务结束日期 / 任务结束状态',
                                  end_mission_status                    string  comment '其他-任务结束日期 / 任务结束状态',
                                  country_code                          string  comment '国家代码',
                                  country_name                          string  comment '国家名称',
                                  extend_info                           string  comment '扩展字段',
                                  friend_foe                            string  comment '敌我识别',
                                  search_content                        string  comment '搜索字段 将所有搜索值放在该字段，建立倒排索引',
                                  update_time                           string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = '172.21.30.202:30030',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'ja_argo.dws_et_argo_info',
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


-- 1. argo的传感器数据表
create table dws_atr_argo_sensor_info (
                                          id                     bigint        comment 'WMO编号',
                                          type                   string        comment '浮标上安装的传感器名称',
                                          sensor_name            string        comment '浮标上安装的传感器名称-中文',
                                          index_no               bigint        comment '索引顺序',
                                          sensor_maker           string        comment '传感器制造商名称',
                                          sensor_maker_name      string        comment '传感器制造商名称-中文',
                                          sensor_model           string        comment '传感器型号',
                                          sensor_serial_no       string        comment '传感器序列号',
                                          update_time            string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = '172.21.30.202:30030',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'ja_argo.dws_atr_argo_sensor_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='2000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',  	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'  	     -- 行分隔符

      );



-- 2. argo的物理参数信息表
create table dws_atr_argo_pp_info (
                                      id                     		        bigint        comment 'WMO编号',
                                      type                                  string        comment '参数名称',
                                      index_no                              bigint        comment '索引顺序',
                                      parameter_sensor                      string        comment '测量该参数的传感器名称',
                                      parameter_units                       string        comment '参数的单位',
                                      parameter_accuracy                    string        comment '参数的准确度',
                                      parameter_resolution                  string        comment '参数的分辨率',
                                      predeployment_calib_equation          string        comment '参数预部署校准方程',
                                      predeployment_calib_coefficient       string        comment '部署前校准系数',
                                      predeployment_calib_comment           string        comment '部署前校准的注释',
                                      update_time                           string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = '172.21.30.202:30030',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'ja_argo.dws_atr_argo_pp_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='2000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',  	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'  	     -- 行分隔符
      );



-- 3. argo的发射时浮标配置参数信息表
create table dws_atr_argo_config_info (
                                          id               bigint  comment 'WMO编号',
                                          type             string  comment '配置的参数名称',
                                          index_no         bigint  comment '索引顺序',
                                          config_value     string  comment '配置的参数值',
                                          update_time      string  comment '数据入库时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = '172.21.30.202:30030',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'ja_argo.dws_atr_argo_config_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='2000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',  	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'  	     -- 行分隔符
      );




-- 4. argo的当前浮标任务配置参数信息表
create table dws_atr_argo_mission_config_info (
                                                  id                     bigint        comment 'WMO编号',
                                                  config_parameter_name  string        comment '（当前）配置参数名 / （当前）配置参数值',
                                                  config_parameter_value string        comment '（当前）配置参数名 / （当前）配置参数值',
                                                  update_time            string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = '172.21.30.202:30030',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'ja_argo.dws_atr_argo_mission_config_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='2000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',  	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'  	     -- 行分隔符
      );


-- 5. aogo的综合分析chart图
create table dws_atr_argo_sec_ove_chart (
                                            id            bigint        comment 'WMO编号',
                                            type          string,
                                            image_url     string,
                                            update_time   string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = '172.21.30.202:30030',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'ja_argo.dws_atr_argo_sec_ove_chart',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='2000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',  	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'  	     -- 行分隔符
      );


-- 6. dws_atr_argo_sec_ove_chart_error
create table dws_atr_argo_sec_ove_chart_error (
                                                  id            bigint        comment 'WMO编号',
                                                  flag          string,
                                                  update_time   string        comment '数据入库时间'
) with (
      'connector' = 'doris',
      -- 'fenodes' = '172.21.30.202:30030',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'ja_argo.dws_atr_argo_sec_ove_chart_error',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='2000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',  	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'  	     -- 行分隔符
      );



--  *************************************************************** 维表
-- 浮标类型的枚举类型
create table dim_float_type_info (
                                     id  	 		   varchar(200)  	comment '浮标编号',
                                     e_name 		   varchar(200) 	comment '浮标类型-英文名称',
                                     c_name 		   varchar(200) 	comment '浮标类型-中文名称',
                                     primary key (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://172.21.30.202:31030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.245:9030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_float_type_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '1000000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );



-- 卫星定位/通讯系统类型的枚举类型
create table dim_satellite_system_info (
                                           name  	 	varchar(200)  			comment '名称',
                                           description 	varchar(200) 			comment '描述',
                                           primary key (name) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://172.21.30.202:31030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.245:9030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_satellite_system_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '1000000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );


-- 卫星传输系统类型
create table dim_transmission_system_info (
                                              code  	 		varchar(200)  			comment '名称',
                                              e_name 			varchar(200) 			comment '英文描述',
                                              c_name            varchar(200)			comment '中文描述',
                                              primary key (code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      --'url' = 'jdbc:mysql://172.21.30.202:31030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.245:9030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_transmission_system_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '1000000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );



-- 浮标生产商枚举
create table dim_platform_maker_info (
                                         code  	 		varchar(200)  			comment '码-名称',
                                         e_name 			varchar(200) 			comment '英文描述',
                                         c_name            varchar(200)			comment '中文描述',
                                         primary key (code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://172.21.30.202:31030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.245:9030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_platform_maker_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '1000000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );


-- Argo资料中心和研究所代码
create table dim_data_center_info (
                                      code  	 		varchar(200)  			comment '代码-名称',
                                      e_name 			varchar(200) 			comment '英文描述',
                                      c_name            varchar(200)			comment '中文描述',
                                      country_name 		varchar(200) 			comment '国家名称',
                                      country_code      varchar(200)			comment '国家代码',
                                      primary key (code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://172.21.30.202:31030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.245:9030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_data_center_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '1000000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );


-- 传感器类型
create table dim_sensor_type_info (
                                      code  	 		varchar(200)  			comment '代码-名称',
                                      c_name            varchar(200)			comment '中文描述',
                                      primary key (code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://172.21.30.202:31030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.245:9030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_sensor_type_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '1000000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );


-- 传感器生产商
create table dim_sensor_maker_info (
                                       code  	 		varchar(200)  			comment '代码-名称',
                                       c_name            varchar(200)			comment '中文描述',
                                       primary key (code) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://172.21.30.202:31030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'url' = 'jdbc:mysql://172.21.30.245:9030/ja_argo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC&autoReconnect=true',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'table-name' = 'dim_sensor_maker_info',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'lookup.cache.max-rows' = '1000000',
      'lookup.cache.ttl' = '84000s',
      'lookup.max-retries' = '10'
      );




-- ************************************** 传感器、参数、配置 数据写入中转kafka ***************************************************************

create table argo_meta_transfer(
                                   id                  bigint,
                                   sensor_array        ARRAY<String>,  -- 传感器
                                   parameter_array     ARRAY<
                                       Row<
                                       parameter_name                   string,
                                   parameter_sensor                 string,
                                   parameter_units                  string,
                                   parameter_accuracy               string,
                                   parameter_resolution             string,
                                   predeployment_calib_equation     string,
                                   predeployment_calib_coefficient  string,
                                   predeployment_calib_comment      string,
                                   index_no                         string
                                       >
                                       >,   -- 物理参数
                                   config_array      ARRAY<String>  -- 配置参数
) with (
      'connector' = 'kafka',
      'topic' = 'argo_meta_transfer',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'argo_meta_transfer_group1',
      'format' = 'json',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      -- 'key.format' = 'json',
      -- 'key.fields' = 'id'
      );


-----------------------

-- 数据处理

-----------------------

-- 去除两侧空格字段
create view tmp_01 as
select
    trim(PLATFORM_NUMBER)                       as id,
    trim(DATA_TYPE)                             as data_type,
    trim(FORMAT_VERSION)                        as format_version,
    trim(HANDBOOK_VERSION)                      as handbook_version,
    trim(DATE_CREATION)                         as date_creation,
    trim(DATE_UPDATE)                           as date_update,
    trim(PTT)                                   as ptt,
    trim(TRANS_SYSTEM)                          as trans_system,
    trim(TRANS_SYSTEM_ID)                       as trans_system_id,
    trim(TRANS_FREQUENCY)                       as trans_frequency,
    trim(POSITIONING_SYSTEM)                    as positioning_system,
    trim(PLATFORM_FAMILY)                       as platform_family,
    trim(PLATFORM_TYPE)                         as platform_type,
    trim(PLATFORM_MAKER)                        as platform_maker,
    trim(FIRMWARE_VERSION)                      as firmware_version,
    trim(MANUAL_VERSION)                        as manual_version,
    trim(FLOAT_SERIAL_NO)                       as float_serial_no,
    trim(STANDARD_FORMAT_ID)                    as standard_format_id,
    trim(DAC_FORMAT_ID)                         as dac_format_id,
    trim(WMO_INST_TYPE)                         as wmo_inst_type,
    trim(PROJECT_NAME)                          as project_name,
    trim(DATA_CENTRE)                           as data_centre,
    trim(PI_NAME)                               as pi_name,
    trim(ANOMALY)                               as anomaly,
    trim(BATTERY_TYPE)                          as battery_type,
    trim(BATTERY_PACKS)                         as battery_packs,
    trim(CONTROLLER_BOARD_TYPE_PRIMARY)         as controller_board_type_primary,
    trim(CONTROLLER_BOARD_TYPE_SECONDARY)       as controller_board_type_secondary,
    trim(CONTROLLER_BOARD_SERIAL_NO_PRIMARY)    as controller_board_serial_no_primary,
    trim(CONTROLLER_BOARD_SERIAL_NO_SECONDARY)  as controller_board_serial_no_secondary,
    trim(SPECIAL_FEATURES)                      as special_features,
    trim(FLOAT_OWNER)                           as float_owner,
    trim(OPERATING_INSTITUTION)                 as operating_institution,
    trim(CUSTOMISATION)                         as customisation,
    trim(LAUNCH_DATE)                           as launch_date,
    trim(LAUNCH_LATITUDE)                       as launch_latitude,
    trim(LAUNCH_LONGITUDE)                      as launch_longitude,
    trim(LAUNCH_QC)                             as launch_qc,
    trim(`START_DATE`)                          as `start_date`,
    trim(START_DATE_QC)                         as start_date_qc,
    trim(STARTUP_DATE)                          as startup_date,
    trim(STARTUP_DATE_QC)                       as startup_date_qc,
    trim(DEPLOYMENT_PLATFORM)                   as deployment_platform,
    trim(DEPLOYMENT_CRUISE_ID)                  as deployment_cruise_id,
    trim(DEPLOYMENT_REFERENCE_STATION_ID)       as deployment_reference_station_id,
    trim(END_MISSION_DATE)                      as end_mission_date,
    trim(END_MISSION_STATUS)                    as end_mission_status,
    trim(LAUNCH_CONFIG_PARAMETER_NAME)          as launch_config_parameter_name,
    trim(LAUNCH_CONFIG_PARAMETER_VALUE)         as launch_config_parameter_value,
    trim(CONFIG_PARAMETER_NAME)                 as config_parameter_name,
    trim(CONFIG_PARAMETER_VALUE)                as config_parameter_value,
    trim(CONFIG_MISSION_NUMBER)                 as config_mission_number,
    trim(CONFIG_MISSION_COMMENT)                as config_mission_comment,
    trim(SENSOR)                                as sensor,
    trim(SENSOR_MAKER)                          as sensor_maker,
    trim(SENSOR_MODEL)                          as sensor_model,
    trim(SENSOR_SERIAL_NO)                      as sensor_serial_no,
    `PARAMETER`                                 as parameter_name,
    PARAMETER_SENSOR                            as parameter_sensor,
    PARAMETER_UNITS                             as parameter_units,
    PARAMETER_ACCURACY                          as parameter_accuracy,
    PARAMETER_RESOLUTION                        as parameter_resolution,
    PREDEPLOYMENT_CALIB_EQUATION                as predeployment_calib_equation,
    PREDEPLOYMENT_CALIB_COEFFICIENT             as predeployment_calib_coefficient,
    PREDEPLOYMENT_CALIB_COMMENT                 as predeployment_calib_comment

    -- trim之后就没有tab键了
    -- trim(`PARAMETER`)                           as parameter_name,
    -- trim(PARAMETER_SENSOR)                      as parameter_sensor,
    -- trim(PARAMETER_UNITS)                       as parameter_units,
    -- trim(PARAMETER_ACCURACY)                    as parameter_accuracy,
    -- trim(PARAMETER_RESOLUTION)                  as parameter_resolution,
    -- trim(PREDEPLOYMENT_CALIB_EQUATION)          as predeployment_calib_equation,
    -- trim(PREDEPLOYMENT_CALIB_COEFFICIENT)       as predeployment_calib_coefficient,
    -- trim(PREDEPLOYMENT_CALIB_COMMENT)           as predeployment_calib_comment
from argo_meta_kafka;


-- 转换字段，判断空值null
create view tmp_02 as
select
    if(id in('n/a',''),cast(null as varchar),id)                                    as id,
    if(data_type in('n/a',''),cast(null as varchar),data_type)                      as data_type,
    if(format_version in('n/a',''),cast(null as varchar),format_version)            as format_version,
    if(handbook_version in('n/a',''),cast(null as varchar),handbook_version)        as handbook_version,
    if(date_creation in('n/a',''),cast(null as varchar),date_creation)              as date_creation,
    if(date_update in('n/a',''),cast(null as varchar),date_update)                  as date_update,
    if(ptt in('n/a',''),cast(null as varchar),ptt)                                  as ptt,
    if(trans_system in('n/a',''),cast(null as varchar),trans_system)                as trans_system,
    if(trans_system_id in('n/a',''),cast(null as varchar),trans_system_id)          as trans_system_id,
    if(trans_frequency in('n/a',''),cast(null as varchar),trans_frequency)          as trans_frequency,
    if(positioning_system in('n/a',''),cast(null as varchar),positioning_system)    as positioning_system,
    if(platform_family in('n/a',''),cast(null as varchar),platform_family)          as platform_family,
    if(platform_type in('n/a',''),cast(null as varchar),platform_type)              as platform_type,
    if(platform_maker in('n/a',''),cast(null as varchar),platform_maker)            as platform_maker,
    if(firmware_version in('n/a',''),cast(null as varchar),firmware_version)        as firmware_version,
    if(manual_version in('n/a',''),cast(null as varchar),manual_version)            as manual_version,
    if(float_serial_no in('n/a',''),cast(null as varchar),float_serial_no)          as float_serial_no,
    if(standard_format_id in('n/a',''),cast(null as varchar),standard_format_id)    as standard_format_id,
    if(dac_format_id in('n/a',''),cast(null as varchar),dac_format_id)              as dac_format_id,
    if(wmo_inst_type in('n/a',''),cast(null as varchar),wmo_inst_type)              as wmo_inst_type,
    if(project_name in('n/a',''),cast(null as varchar),project_name)                as project_name,
    if(data_centre in('n/a',''),cast(null as varchar),data_centre)                  as data_centre,
    if(pi_name in('n/a',''),cast(null as varchar),pi_name)                          as pi_name,
    if(anomaly in('n/a',''),cast(null as varchar),anomaly)                          as anomaly,
    if(battery_type in('n/a',''),cast(null as varchar),battery_type)                as battery_type,
    if(battery_packs in('n/a',''),cast(null as varchar),battery_packs)              as battery_packs,
    if(controller_board_type_primary in('n/a',''),cast(null as varchar),controller_board_type_primary)              as controller_board_type_primary,
    if(controller_board_type_secondary in('n/a',''),cast(null as varchar),controller_board_type_secondary)           as controller_board_type_secondary,
    if(controller_board_serial_no_primary in('n/a',''),cast(null as varchar),controller_board_serial_no_primary)     as controller_board_serial_no_primary,
    if(controller_board_serial_no_secondary in('n/a',''),cast(null as varchar),controller_board_serial_no_secondary) as controller_board_serial_no_secondary,
    if(special_features in('n/a',''),cast(null as varchar),special_features)            as special_features,
    if(float_owner in('n/a',''),cast(null as varchar),float_owner)                      as float_owner,
    if(operating_institution in('n/a',''),cast(null as varchar),operating_institution)  as operating_institution,
    if(customisation in('n/a',''),cast(null as varchar),customisation)                  as customisation,
    if(launch_date in('n/a',''),cast(null as varchar),launch_date)                      as launch_date,
    if(launch_latitude in('n/a',''),cast(null as varchar),launch_latitude)              as launch_latitude,
    if(launch_longitude in('n/a',''),cast(null as varchar),launch_longitude)            as launch_longitude,
    if(launch_qc in('n/a',''),cast(null as varchar),launch_qc)                          as launch_qc,
    if(`start_date` in('n/a',''),cast(null as varchar),`start_date`)                    as `start_date`,
    if(start_date_qc in('n/a',''),cast(null as varchar),start_date_qc)                  as start_date_qc,
    if(startup_date in('n/a',''),cast(null as varchar),startup_date)                    as startup_date,
    if(startup_date_qc in('n/a',''),cast(null as varchar),startup_date_qc)              as startup_date_qc,
    if(deployment_platform in('n/a',''),cast(null as varchar),deployment_platform)      as deployment_platform,
    if(deployment_cruise_id in('n/a',''),cast(null as varchar),deployment_cruise_id)    as deployment_cruise_id,
    if(deployment_reference_station_id in('n/a',''),cast(null as varchar),deployment_reference_station_id)  as deployment_reference_station_id,
    if(end_mission_date in('n/a',''),cast(null as varchar),end_mission_date)                                as end_mission_date,
    if(end_mission_status in('n/a',''),cast(null as varchar),end_mission_status)                            as end_mission_status,
    if(launch_config_parameter_name in('n/a',''),cast(null as varchar),launch_config_parameter_name)        as launch_config_parameter_name,
    if(launch_config_parameter_value in('n/a',''),cast(null as varchar),launch_config_parameter_value)      as launch_config_parameter_value,
    if(config_parameter_name in('n/a',''),cast(null as varchar),config_parameter_name)                      as config_parameter_name,
    if(config_parameter_value in('n/a',''),cast(null as varchar),config_parameter_value)                    as config_parameter_value,
    if(config_mission_number in('n/a',''),cast(null as varchar),config_mission_number)                      as config_mission_number,
    if(config_mission_comment in('n/a',''),cast(null as varchar),config_mission_comment)                    as config_mission_comment,
    if(sensor in('n/a',''),cast(null as varchar),sensor)                                                    as sensor,
    if(sensor_maker in('n/a',''),cast(null as varchar),sensor_maker)                                        as sensor_maker,
    if(sensor_model in('n/a',''),cast(null as varchar),sensor_model)                                        as sensor_model,
    if(sensor_serial_no in('n/a',''),cast(null as varchar),sensor_serial_no)                                as sensor_serial_no,
    parameter_name,
    parameter_sensor,
    parameter_units,
    parameter_accuracy,
    parameter_resolution,
    predeployment_calib_equation,
    predeployment_calib_coefficient,
    predeployment_calib_comment
    -- if(parameter_name in('n/a',''),cast(null as varchar),parameter_name)                                    as parameter_name,
    -- if(parameter_sensor in('n/a',''),cast(null as varchar),parameter_sensor)                                as parameter_sensor,
    -- if(parameter_units in('n/a',''),cast(null as varchar),parameter_units)                                  as parameter_units,
    -- if(parameter_accuracy in('n/a',''),cast(null as varchar),parameter_accuracy)                            as parameter_accuracy,
    -- if(parameter_resolution in('n/a',''),cast(null as varchar),parameter_resolution)                        as parameter_resolution,
    -- if(predeployment_calib_equation in('n/a',''),cast(null as varchar),predeployment_calib_equation)        as predeployment_calib_equation,
    -- if(predeployment_calib_coefficient in('n/a',''),cast(null as varchar),predeployment_calib_coefficient)  as predeployment_calib_coefficient,
    -- if(predeployment_calib_comment          in('n/a',''),cast(null as varchar),predeployment_calib_comment) as predeployment_calib_comment
from tmp_01;


-- 处理时间字段,过滤id不为空数据,udf函数处理传感器数据
create view tmp_03 as
select
    id,
    data_type,
    format_version,
    handbook_version,
    date_creation,
    date_update,
    ptt,
    trans_system,
    trans_system_id,
    trans_frequency,
    positioning_system,
    platform_family,
    platform_type,
    platform_maker,
    firmware_version,
    manual_version,
    float_serial_no,
    standard_format_id,
    dac_format_id,
    wmo_inst_type,
    project_name,
    data_centre,
    pi_name,
    anomaly,
    battery_type,
    battery_packs,
    controller_board_type_primary,
    controller_board_type_secondary,
    controller_board_serial_no_primary,
    controller_board_serial_no_secondary,
    special_features,
    float_owner,
    operating_institution,
    customisation,
    launch_date,
    cast(launch_latitude as double) as launch_latitude,
    cast(launch_longitude as double) as launch_longitude,
    launch_qc,
    start_date,
    start_date_qc,
    startup_date,
    startup_date_qc,
    deployment_platform,
    deployment_cruise_id,
    deployment_reference_station_id,
    end_mission_date,
    end_mission_status,
    launch_config_parameter_name,
    launch_config_parameter_value,
    config_parameter_name,
    config_parameter_value,
    config_mission_number,
    config_mission_comment,
    sensor,
    sensor_maker,
    sensor_model,
    sensor_serial_no,
    parameter_name,
    parameter_sensor,
    parameter_units,
    parameter_accuracy,
    parameter_resolution,
    predeployment_calib_equation,
    predeployment_calib_coefficient,
    predeployment_calib_comment,

    -- 传感器参数数据
    get_sensor_udf(sensor,sensor_maker,sensor_model,sensor_serial_no) as sensor_array,

    -- 物理参数信息
    get_parameter_udf(parameter_name,parameter_sensor,parameter_units,parameter_accuracy,parameter_resolution,predeployment_calib_equation,
                      predeployment_calib_coefficient,predeployment_calib_comment) as parameter_array,

    -- 浮标配置参数信息
    get_config_udf(launch_config_parameter_name,launch_config_parameter_value) as config_array,

    PROCTIME()  as proctime,
    CONVERT_TZ(FROM_UNIXTIME(UNIX_TIMESTAMP(date_creation, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss') ,'UTC', 'Asia/Shanghai')AS acquire_date_creation,
    CONVERT_TZ(FROM_UNIXTIME(UNIX_TIMESTAMP(date_update, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss') ,'UTC', 'Asia/Shanghai')AS   acquire_date_update,
    CONVERT_TZ(FROM_UNIXTIME(UNIX_TIMESTAMP(launch_date, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss') ,'UTC', 'Asia/Shanghai')AS   acquire_launch_date,
    CONVERT_TZ(FROM_UNIXTIME(UNIX_TIMESTAMP(start_date, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss') ,'UTC', 'Asia/Shanghai')AS    acquire_start_date,
    CONVERT_TZ(FROM_UNIXTIME(UNIX_TIMESTAMP(startup_date, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss') ,'UTC', 'Asia/Shanghai')AS  acquire_startup_date,
    CONVERT_TZ(FROM_UNIXTIME(UNIX_TIMESTAMP(end_mission_date, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss') ,'UTC', 'Asia/Shanghai')AS acquire_end_mission_date
from tmp_02
where id is not null;



-- meta 数据 关联字段 入库实体表
create view tmp_04 as
select
    t1.*,
    t2.e_name as wmo_inst_type_e_name,
    t2.c_name as wmo_inst_type_c_name,
    t3.description as positioning_system_name,
    t4.e_name as trans_system_e_name,
    t4.c_name as trans_system_c_name,
    t5.e_name as platform_maker_e_name,
    t5.c_name as platform_maker_c_name,
    t6.e_name as data_centre_e_name,
    t6.c_name as data_centre_c_name,
    t6.country_name as data_centre_country_name,
    t6.country_code as data_centre_country_code
from tmp_03 as t1
         left join dim_float_type_info FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 浮标类型枚举
                   on t1.wmo_inst_type = t2.id

         left join dim_satellite_system_info FOR SYSTEM_TIME AS OF t1.proctime as t3   -- 卫星定位/通讯系统类型的枚举类型
                   on t1.positioning_system = t3.name

         left join dim_transmission_system_info FOR SYSTEM_TIME AS OF t1.proctime as t4   -- 卫星传输系统类型
                   on t1.trans_system = t4.code

         left join dim_platform_maker_info FOR SYSTEM_TIME AS OF t1.proctime as t5   -- 浮标生产商枚举
                   on t1.platform_maker = t5.code

         left join dim_data_center_info FOR SYSTEM_TIME AS OF t1.proctime as t6   -- Argo资料中心和研究所代码
                   on t1.data_centre = t6.code;



-- 传感器数据处理
create view tmp_sensor_01 as
select
    t1.id,
    PROCTIME()  as proctime,
    split_index(t2.line,'=',0)                 as type,
    split_index(t2.line,'=',1)                 as sensor_maker,
    split_index(t2.line,'=',2)                 as sensor_model,
    split_index(t2.line,'=',3)                 as sensor_serial_no,
    cast(split_index(t2.line,'=',4) as bigint) as index_no
from argo_meta_transfer as t1
         cross join unnest (sensor_array) as t2 (
                                                 line
    );

-- 传感器数据 关联
create view tmp_sensor_02 as
select
    t1.id,
    t1.index_no,
    if(t1.type = 'null',cast(null as varchar),type)                         as type,
    if(t1.sensor_maker = 'null',cast(null as varchar),sensor_maker)         as sensor_maker,
    if(t1.sensor_model = 'null',cast(null as varchar),sensor_model)         as sensor_model,
    if(t1.sensor_serial_no = 'null',cast(null as varchar),sensor_serial_no) as sensor_serial_no,
    t2.c_name as sensor_name,
    t3.c_name as sensor_maker_name

from tmp_sensor_01 as t1
         left join dim_sensor_type_info FOR SYSTEM_TIME AS OF t1.proctime as t2   -- 浮标类型枚举
                   on t1.type = t2.code

         left join dim_sensor_maker_info FOR SYSTEM_TIME AS OF t1.proctime as t3   -- 卫星定位/通讯系统类型的枚举类型
                   on t1.sensor_maker = t3.code;


-- argo的物理参数处理
create view tmp_parameter_01 as
select
    t1.id,
    parameter_name as type,
    parameter_sensor,
    parameter_units,
    parameter_accuracy,
    parameter_resolution,
    predeployment_calib_equation,
    predeployment_calib_coefficient,
    predeployment_calib_comment,
    cast(index_no as bigint) as index_no
from argo_meta_transfer as t1
         cross join unnest (parameter_array) as t2 (
                                                    parameter_name,
                                                    parameter_sensor,
                                                    parameter_units,
                                                    parameter_accuracy,
                                                    parameter_resolution,
                                                    predeployment_calib_equation,
                                                    predeployment_calib_coefficient,
                                                    predeployment_calib_comment,
                                                    index_no
    );


-- argo的发射配置参数信息表
create view tmp_config_01 as
select
    t1.id,
    if(split_index(t2.line,'=',0) = 'null',cast(null as varchar),split_index(t2.line,'=',0))                 as type,
    if(split_index(t2.line,'=',1) = 'null',cast(null as varchar),split_index(t2.line,'=',1))                 as config_value,
    cast(split_index(t2.line,'=',2) as bigint) as index_no
from argo_meta_transfer as t1
         cross join unnest (config_array) as t2 (
                                                 line
    );



begin statement set;

insert into dws_et_argo_info
select
    cast(id as bigint) as id,
    data_type,
    format_version,
    handbook_version,
    acquire_date_creation,
    acquire_date_update,
    ptt,
    trans_system,
    trans_system_e_name,
    trans_system_c_name,
    trans_system_id,
    trans_frequency,
    positioning_system,
    positioning_system_name,
    platform_family,
    platform_type,
    platform_maker,
    platform_maker_e_name,
    platform_maker_c_name,
    firmware_version,
    manual_version,
    float_serial_no,
    standard_format_id,
    dac_format_id,
    wmo_inst_type,
    wmo_inst_type_e_name,
    wmo_inst_type_c_name,
    project_name,
    data_centre,
    data_centre_e_name,
    data_centre_c_name,
    data_centre_country_name,
    data_centre_country_code,
    pi_name,
    anomaly,
    battery_type,
    battery_packs,
    controller_board_type_primary,
    controller_board_type_secondary,
    controller_board_serial_no_primary,
    controller_board_serial_no_secondary,
    special_features,
    float_owner,
    operating_institution,
    customisation,
    acquire_launch_date as launch_date,
    launch_latitude,
    launch_longitude,
    launch_qc,
    acquire_start_date as acquire_start_date,
    start_date_qc,
    acquire_startup_date    as startup_date,
    startup_date_qc,
    deployment_platform,
    deployment_cruise_id,
    deployment_reference_station_id,
    acquire_end_mission_date,
    end_mission_status,
    cast(null as varchar) as country_code,
    cast(null as varchar) as country_name,
    cast(null as varchar) as extend_info,
    cast(null as varchar) as friend_foe,
    id as search_content,
    from_unixtime(unix_timestamp()) as update_time   -- 更新时间
from tmp_04;



-- 传感器数据表
insert into dws_atr_argo_sensor_info
select
    id,
    type,
    sensor_name,
    index_no,
    sensor_maker,
    sensor_maker_name,
    sensor_model,
    sensor_serial_no,
    from_unixtime(unix_timestamp()) as update_time
from tmp_sensor_02;


-- argo的物理参数信息表 入库
insert into dws_atr_argo_pp_info
select
    id,
    type,
    index_no,
    parameter_sensor,
    parameter_units,
    parameter_accuracy,
    parameter_resolution,
    predeployment_calib_equation,
    predeployment_calib_coefficient,
    predeployment_calib_comment,
    from_unixtime(unix_timestamp()) as update_time
from tmp_parameter_01;


-- argo的发射时浮标配置参数信息表 入库
insert into dws_atr_argo_config_info
select
    id,
    type,
    index_no,
    config_value,
    from_unixtime(unix_timestamp()) as update_time
from tmp_config_01;


-- 当前任务配置参数
insert into dws_atr_argo_mission_config_info
select
    cast(id as bigint) as id,
    config_parameter_name,
    config_parameter_value,
    from_unixtime(unix_timestamp()) as update_time
from tmp_03;



-- 传感器、参数、配置 数据写入中转kafka
insert into argo_meta_transfer
select
    cast(id as bigint) as id,
    sensor_array,
    parameter_array,
    config_array
from tmp_03;


insert into dws_atr_argo_sec_ove_chart
select
    cast(id as bigint) as id,
    `key` as type,
    url as image_url,
    from_unixtime(unix_timestamp()) as update_time
from argo_chart_kafka
where flag is null or flag <> 'FAIL';


insert into dws_atr_argo_sec_ove_chart_error
select
    cast(id as bigint) as id,
    flag,
    from_unixtime(unix_timestamp()) as update_time
from argo_chart_kafka
where flag is not null and  flag = 'FAIL';

end;


