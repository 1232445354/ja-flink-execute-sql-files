-- 飞机注册号前缀对应国家
insert into dim_aircraft_country_prefix_code
select * from doris_ecs.sa.dim_aircraft_country_prefix_code;

-- 飞机机型固定的32个
insert into dim_aircraft_model_info
select * from doris_ecs.sa.dim_aircraft_model_info;

-- 机型和类型代码对应关系
insert into dim_aircraft_model_type_relation_info
select * from doris_ecs.sa.dim_aircraft_model_type_relation_info;

-- 更改之后的机型和类型对应关系，去掉军用
insert into dim_aircraft_type_category
select * from doris_ecs.sa.dim_aircraft_type_category;

-- 飞机类型代码和类型中文对应(直升机、大型机...)
insert into dim_aircraft_type_info
select * from doris_ecs.sa.dim_aircraft_type_info;

-- 航空公司2、3字代码、英文、中文
insert into dim_airline_3_2_code_info
select * from doris_ecs.sa.dim_airline_3_2_code_info;

-- radarbox的航空公司列表
insert into dim_airline_list_info
select * from doris_ecs.sa.dim_airline_list_info;

-- 机场类型枚举
insert into dim_airport_apttype_info
select * from doris_ecs.sa.dim_airport_apttype_info;

-- 机场所允许航班类别枚举
insert into dim_airport_fltcat_info
select * from doris_ecs.sa.dim_airport_fltcat_info;

-- 机场三字、四字代码对应关系
insert into dim_airport_iata_icao_relation_info
select * from doris_ecs.sa.dim_airport_iata_icao_relation_info;

-- 基地、机场、港口关系、llm需要的
insert into dim_base_port_airport_reletion_info
select * from doris_ecs.sa.dim_base_port_airport_reletion_info;

-- 机场天气枚举
insert into dim_airport_iconweather_info
select * from doris_ecs.sa.dim_airport_iconweather_info;

-- 机场中文名称翻译
insert into dim_airport_name_info
select * from doris_ecs.sa.dim_airport_name_info;

-- 国家列表
insert into dim_country_code_name_info
select * from doris_ecs.sa.dim_country_code_name_info;

-- 国家二字、三字代码对应关系
insert into dim_country_info
select * from doris_ecs.sa.dim_country_info;

-- fleetmon船舶的采集块
insert into dim_fm_block_range_list
select * from doris_ecs.sa.dim_fm_block_range_list;

-- fleetmon、marinetraffic网站船舶id对应关系
insert into dim_mt_fm_id_relation
select * from doris_ecs.sa.dim_mt_fm_id_relation;

-- marinetraffic的船舶id、mmsi、类型列表
insert into dim_mt_id_mmsi
select * from doris_ecs.sa.dim_mt_id_mmsi;

-- marinetraffic的船舶大类型
insert into dim_mtf_fm_class
select * from doris_ecs.sa.dim_mtf_fm_class;

-- marinetraffic的船舶小类型
insert into dim_mtf_fm_type
select * from doris_ecs.sa.dim_mtf_fm_type;

-- marinetraffic的船舶服务状态
insert into sa.dim_mtf_service_status_info
select * from doris_ecs.sa.dim_mtf_service_status_info

-- marinetraffic、VT的船舶对应关系
insert into sa.dim_mtf_vt_reletion_info
select * from doris_ecs.sa.dim_mtf_vt_reletion_info

-- 港口名称
insert into dim_port_name_info
select * from doris_ecs.sa.dim_port_name_info;

-- 卫星静态属性表
insert into dim_satellite_static_info
select * from doris_ecs.sa.dim_satellite_static_info;

-- 海域
insert into dim_sea_area
select * from doris_ecs.sa.dim_sea_area;

-- 船舶中文名称
insert into dim_vessel_c_name_info
select * from doris_ecs.sa.dim_vessel_c_name_info;

-- fleetmon的船舶大类型枚举列表
insert into dim_vessel_class_list
select * from doris_ecs.sa.dim_vessel_class_list;

-- fleetmon的船舶国家列表
insert into dim_vessel_country_code_list
select * from doris_ecs.sa.dim_vessel_country_code_list;

-- fleetmon的船舶航行状态
insert into dim_vessel_nav_status_list
select * from doris_ecs.sa.dim_vessel_nav_status_list;

-- fleetmon的船舶风险等级
insert into dim_vessel_risk_rating_list
select * from doris_ecs.sa.dim_vessel_risk_rating_list;

-- fleetmon的船舶服务状态枚举
insert into dim_vessel_service_status_list
select * from doris_ecs.sa.dim_vessel_service_status_list;

-- fleetmon的船舶id和类型列表
insert into dim_vessel_type
select * from doris_ecs.sa.dim_vessel_type;

-- fleetmon的船舶小类型枚举列表
insert into dim_vessel_type_categorie_list
select * from doris_ecs.sa.dim_vessel_type_categorie_list;

-- fleetmon的船舶小类型枚举列表
insert into dim_vt_country_code_info
select * from doris_ecs.sa.dim_vt_country_code_info;

-- 机场列表
insert into dwd_airport_list_info
select * from doris_ecs.sa.dwd_airport_list_info;

-- 发射场实体表
insert into dwd_launch_site_info
select * from doris_ecs.sa.dwd_launch_site_info;

-- marinetraffic上的港口实体表
insert into dwd_maric_port_all_info
select * from doris_ecs.sa.dwd_maric_port_all_info;

-- 军事基地实体表
insert into sa.dwd_military_base_info
select * from doris_ecs.sa.dwd_military_base_info;

-- poi表
-- insert into sa.dwd_poi_all_info
-- select * from doris_ecs.sa.dwd_poi_all_info;

-- 港口实体表
insert into sa.dwd_port_all_list
select * from doris_ecs.sa.dwd_port_all_list;

-- 飞机实体表
insert into sa.dws_aircraft_info
select * from doris_ecs.sa.dws_aircraft_info;

-- 机场详情表
insert into sa.dws_airport_detail_info
select * from doris_ecs.sa.dws_airport_detail_info;

-- 地面站图片
insert into sa.dws_ground_basestation_image_info
select * from doris_ecs.sa.dws_ground_basestation_image_info;

-- 地面站实体表
insert into sa.dws_ground_basestation_info
select * from doris_ecs.sa.dws_ground_basestation_info;

-- 飞机s模式、注册号、机型表
insert into sa.dws_sa_aircraft_icao_info
select * from doris_ecs.sa.dws_sa_aircraft_icao_info;

-- 卫星实体表
insert into sa.dws_satellite_info
select * from doris_ecs.sa.dws_satellite_info;

-- vesselfinder的船舶详情
insert into dws_vesselfinder_vessel_detail_info
select * from doris_ecs.sa.dws_vesselfinder_vessel_detail_info;

-- 军事舰船的名单
insert into dws_vessle_nato_malitary
select * from doris_ecs.sa.dws_vessle_nato_malitary;


