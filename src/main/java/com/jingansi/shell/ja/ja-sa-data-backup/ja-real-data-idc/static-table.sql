-- 整个时间跨度1月

SET @start_time = "2024-11-11 00:00:00";
SET @end_time = "2024-12-15 23:59:59";

-- marinetraffic的详情表
insert into dwd_mtf_ship_info
select * from doris_idc.sa.dwd_mtf_ship_info
where update_time between @start_time and @end_time;



-- marinetraffic船舶状态数据
insert into dws_vessel_list_status_rt
select * from doris_idc.sa.dws_vessel_list_status_rt
where acquire_timestamp_format between @start_time and @end_time;



-- vt的船舶状态数据单独入库
insert into dws_vt_vessel_status_info
select * from doris_idc.sa.dws_vt_vessel_status_info
where acquire_timestamp_format between @start_time and @end_time;



-- 岸基数据状态表
insert into dws_ais_landbased_vessel_status
select * from doris_idc.sa.dws_ais_landbased_vessel_status
where acquire_time between @start_time and @end_time;



-- 卫星全量表
insert into dwd_bhv_satellite_rt
select * from doris_idc.sa.dwd_bhv_satellite_rt
where acquire_time between @start_time and @end_time;


-- 卫星每日聚合表
insert into dws_bhv_satellite_list_fd
select * from doris_idc.sa.dws_bhv_satellite_list_fd
where today_time between @start_time and @end_time;


-- 卫星图片表
insert into dws_atr_satellite_image_info
select * from doris_idc.sa.dws_atr_satellite_image_info
where acquire_time between @start_time and @end_time;



-- 卫星实体详情
insert into dws_et_satellite_info
select * from doris_idc.sa.dws_et_satellite_info
where update_time between @start_time and @end_time;



-- 飞机实体表
insert into dws_et_aircraft_info
select * from doris_idc.sa.dws_et_aircraft_info
where acquire_time between @start_time and @end_time;



-- 飞机最后位置-新表、数据融合版
insert into dws_bhv_aircraft_last_location_rt
select * from doris_idc.sa.dws_bhv_aircraft_last_location_rt
where acquire_time between @start_time and @end_time;



-- 飞机起飞航班表
insert into dws_airport_flight_info
select * from doris_idc.sa.dws_airport_flight_info
where update_time between @start_time and @end_time;



-- 船舶实体表
insert into dws_vessel_et_info_rt
select * from doris_idc.sa.dws_vessel_et_info_rt
where update_time between @start_time and @end_time;



-- 船舶最后位置状态表
insert into dws_vessel_bhv_status_rt
select * from doris_idc.sa.dws_vessel_bhv_status_rt
where acquire_time between @start_time and @end_time;

