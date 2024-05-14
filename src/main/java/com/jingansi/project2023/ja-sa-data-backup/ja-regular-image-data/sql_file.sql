-- 原船舶图片表
insert into sa.dws_ais_vessel_image_info
select * from doris_ecs.sa.dws_ais_vessel_image_info;

-- 船舶实体图片表
insert into sa.ads_vessel_image_info
select * from doris_ecs.sa.ads_vessel_image_info;

-- 飞机实体图片
insert into sa.dws_aircraft_image_info
select * from doris_ecs.sa.dws_aircraft_image_info;

-- 天气
insert into sa.dwd_weather_save_url_rt
select * from doris_ecs.sa.dwd_weather_save_url_rt;

-- gps干扰
insert into sa.dwd_gps_url_rt
select * from doris_ecs.sa.dwd_gps_url_rt;
