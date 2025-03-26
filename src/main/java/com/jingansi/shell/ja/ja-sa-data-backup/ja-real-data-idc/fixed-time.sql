SET @start_time = "2024-11-09 00:00:00";
SET @end_time = "2024-12-15 23:59:59";

-- gps干扰
insert into dwd_gps_url_rt
select * from doris_idc.sa.dwd_gps_url_rt
where update_time between @start_time and @end_time;

-- 天气
insert into dwd_weather_save_url_rt
select * from doris_idc.sa.dwd_weather_save_url_rt
where update_time between @start_time and @end_time;

-- 飞机图片原始采集表
insert into dws_aircraft_image_info
select * from doris_idc.sa.dws_aircraft_image_info
where update_time between @start_time and @end_time;

-- 飞机图片id表
insert into dws_atr_aircraft_image_id_info
select * from doris_idc.sa.dws_atr_aircraft_image_id_info
where update_time between @start_time and @end_time;

-- 船舶id与源网站对应关系
insert into dws_vessel_rl_src_id
select * from doris_idc.sa.dws_vessel_rl_src_id
where update_time between @start_time and @end_time;

-- 船舶id与源网站对应关系
insert into dws_vessel_rl_src_ids
select * from doris_idc.sa.dws_vessel_rl_src_ids
where update_time between @start_time and @end_time;

