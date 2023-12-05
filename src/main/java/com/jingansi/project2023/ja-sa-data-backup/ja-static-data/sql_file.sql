-- 表：dim_aircraft_country_prefix_code -- 飞机注册号前缀
insert into dim_aircraft_country_prefix_code
select * from doris_ecs.sa.dim_aircraft_country_prefix_code;

-- 表：dim_aircraft_model_info -- 飞机型号
insert into dim_aircraft_model_info
select * from doris_ecs.sa.dim_aircraft_model_info;

-- 表：dim_aircraft_model_type_relation_info -- 飞机型号类型对应关系
insert into dim_aircraft_model_type_relation_info
select * from doris_ecs.sa.dim_aircraft_model_type_relation_info;

-- 表：dim_aircraft_type_info -- 飞机类型
insert into dim_aircraft_type_info
select * from doris_ecs.sa.dim_aircraft_type_info;

-- 表：dim_airline_3_2_code_info -- 航空公司二三段码对应关系
insert into dim_airline_3_2_code_info
select * from doris_ecs.sa.dim_airline_3_2_code_info;

-- 表：dim_airline_list_info -- 航空公司列表
insert into dim_airline_list_info
select * from doris_ecs.sa.dim_airline_list_info;

-- 表：dim_vessel_c_name_info -- 船舶中文名称
insert into dim_vessel_c_name_info
select * from doris_ecs.sa.dim_vessel_c_name_info;

-- 表：dim_vessel_class_list -- 船类型（大类）
insert into dim_vessel_class_list
select * from doris_ecs.sa.dim_vessel_class_list;

-- 表：dim_vessel_country_code_list -- 船舶国家表
insert into dim_vessel_country_code_list
select * from doris_ecs.sa.dim_vessel_country_code_list;

-- 表：dim_vessel_nav_status_list -- 船舶航行状态字段
insert into dim_vessel_nav_status_list
select * from doris_ecs.sa.dim_vessel_nav_status_list;

-- 表：dim_vessel_risk_rating_list -- 船舶风险等级字段
insert into dim_vessel_risk_rating_list
select * from doris_ecs.sa.dim_vessel_risk_rating_list;

-- 表：dim_vessel_service_status_list -- 船舶服务状态字段
insert into dim_vessel_service_status_list
select * from doris_ecs.sa.dim_vessel_service_status_list;

-- 表：dim_mt_fm_id_relation
insert into dim_mt_fm_id_relation
select * from doris_ecs.sa.dim_mt_fm_id_relation;

-- 表：dim_mt_id_mmsi
insert into dim_mt_id_mmsi
select * from doris_ecs.sa.dim_mt_id_mmsi;

-- 表：dim_vessel_type
insert into dim_vessel_type
select * from doris_ecs.sa.dim_vessel_type;

-- 表：dim_vessel_type_categorie_list -- 船舶类别字段（小类）
insert into dim_vessel_type_categorie_list
select * from doris_ecs.sa.dim_vessel_type_categorie_list;

-- 表：dim_area_range -- 区域范围
insert into dim_area_range
select * from doris_ecs.sa.dim_area_range;

-- 表：dim_country_code_name_info -- 国家代码名称
insert into dim_country_code_name_info
select * from doris_ecs.sa.dim_country_code_name_info;

-- 表：dim_country_info -- 国家表
insert into dim_country_info
select * from doris_ecs.sa.dim_country_info;

-- 表：dim_satellite_static_info -- 卫星静态属性
insert into dim_satellite_static_info
select * from doris_ecs.sa.dim_satellite_static_info;

-- 表：dim_port_name_info -- 港口中英文名称对应
insert into dim_port_name_info
select * from doris_ecs.sa.dim_port_name_info;

-- 表：dim_sea_area -- 海域
insert into dim_sea_area
select * from doris_ecs.sa.dim_sea_area;

-- 表：dwd_airport_list_info -- 机场列表
insert into dwd_airport_list_info
select * from doris_ecs.sa.dwd_airport_list_info;

-- 表：dim_airport_apttype_info -- 机场类型
insert into dim_airport_apttype_info
select * from doris_ecs.sa.dim_airport_apttype_info;

-- 表：dws_airport_detail_info -- 机场详情
insert into sa.dws_airport_detail_info
select * from doris_ecs.sa.dws_airport_detail_info;

-- 表：dim_airport_fltcat_info -- 机场所允许航班类型
insert into dim_airport_fltcat_info
select * from doris_ecs.sa.dim_airport_fltcat_info;

-- 表：dim_airport_iata_icao_relation_info -- 机场icao、iata对应
insert into dim_airport_iata_icao_relation_info
select * from doris_ecs.sa.dim_airport_iata_icao_relation_info;

-- 表：dim_airport_iconweather_info -- 机场天气字段
insert into dim_airport_iconweather_info
select * from doris_ecs.sa.dim_airport_iconweather_info;

-- 表：dim_airport_name_info -- 机场中英文名称
insert into dim_airport_name_info
select * from doris_ecs.sa.dim_airport_name_info;

-- 表：dwd_military_base_info -- 军事基地
insert into sa.dwd_military_base_info
select * from doris_ecs.sa.dwd_military_base_info;

-- 表：dwd_port_all_list -- 港口
insert into sa.dwd_port_all_list
select * from doris_ecs.sa.dwd_port_all_list;


-- 表：dwd_poi_all_info -- poi
-- insert into sa.dwd_poi_all_info
-- select * from doris_ecs.sa.dwd_poi_all_info;