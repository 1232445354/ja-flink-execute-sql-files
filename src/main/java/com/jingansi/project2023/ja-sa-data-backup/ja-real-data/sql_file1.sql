-- 表：dwd_aircraft_list_all_info -- 飞机全量数据
insert into sa.dwd_aircraft_list_all_info
select
    flight_trace_id
     ,acquire_timestamp_format
     ,if(flight_no = '',null,flight_no) as flight_no
     ,acquire_timestamp
     ,latitude
     ,longitude
     ,altitude
     ,altitude_m
     ,if(flight_type = '',null,flight_type) as flight_type
     ,if(speed = '',null,speed) as speed
     ,speed_km
     ,if(heading = '',null,heading) as heading
     ,data_source
     ,if(registration='',null,registration) as registration
     ,if(origin_airport3_code = '',null,origin_airport3_code) as origin_airport3_code
     ,if(destination_airport3_code='',null,destination_airport3_code) as destination_airport3_code
     ,if(airlines_icao = '',null,airlines_icao) as airlines_icao
     ,airlines_name
     ,country_code
     ,country_name
     ,if(num = '',null,num) as num
     ,if(station = '',null,station) as station
     ,source
     ,null as flight_special_flag
     ,source_longitude
     ,source_latitude
     ,destination_longitude
     ,destination_latitude
     ,flight_status
     ,if(num2 = '',null,num2) as num2
     ,expected_landing_time
     ,expected_landing_time_format
     ,if(flight_photo='',null,flight_photo) as flight_photo
     ,flight_departure_time
     ,flight_departure_time_format
     ,un_konwn
     ,to_destination_distance
     ,estimated_landing_duration
     ,if(s_mode='',null,s_mode) as s_mode
     ,position_country_code2
     ,friend_foe
     ,sea_id
     ,sea_name
     ,update_time
from doris_ecs.sa.dwd_aircraft_list_all_info
where acquire_timestamp_format
between concat(to_date(days_sub(now(),1)),' 00:00:00') and concat(to_date(days_sub(now(),1)),' 08:00:00')
;


select sleep(20) as sleep1;


-- 表：dwd_aircraft_list_all_info -- 飞机全量数据
insert into sa.dwd_aircraft_list_all_info
select
    flight_trace_id
     ,acquire_timestamp_format
     ,if(flight_no = '',null,flight_no) as flight_no
     ,acquire_timestamp
     ,latitude
     ,longitude
     ,altitude
     ,altitude_m
     ,if(flight_type = '',null,flight_type) as flight_type
     ,if(speed = '',null,speed) as speed
     ,speed_km
     ,if(heading = '',null,heading) as heading
     ,data_source
     ,if(registration='',null,registration) as registration
     ,if(origin_airport3_code = '',null,origin_airport3_code) as origin_airport3_code
     ,if(destination_airport3_code='',null,destination_airport3_code) as destination_airport3_code
     ,if(airlines_icao = '',null,airlines_icao) as airlines_icao
     ,airlines_name
     ,country_code
     ,country_name
     ,if(num = '',null,num) as num
     ,if(station = '',null,station) as station
     ,source
     ,null as flight_special_flag
     ,source_longitude
     ,source_latitude
     ,destination_longitude
     ,destination_latitude
     ,flight_status
     ,if(num2 = '',null,num2) as num2
     ,expected_landing_time
     ,expected_landing_time_format
     ,if(flight_photo='',null,flight_photo) as flight_photo
     ,flight_departure_time
     ,flight_departure_time_format
     ,un_konwn
     ,to_destination_distance
     ,estimated_landing_duration
     ,if(s_mode='',null,s_mode) as s_mode
     ,position_country_code2
     ,friend_foe
     ,sea_id
     ,sea_name
     ,update_time
from doris_ecs.sa.dwd_aircraft_list_all_info
where acquire_timestamp_format
between concat(to_date(days_sub(now(),1)),' 08:00:00') and concat(to_date(days_sub(now(),1)),' 16:00:00')
;


select sleep(20) as sleep2;


-- 表：dwd_aircraft_list_all_info -- 飞机全量数据
insert into sa.dwd_aircraft_list_all_info
select
    flight_trace_id
     ,acquire_timestamp_format
     ,if(flight_no = '',null,flight_no) as flight_no
     ,acquire_timestamp
     ,latitude
     ,longitude
     ,altitude
     ,altitude_m
     ,if(flight_type = '',null,flight_type) as flight_type
     ,if(speed = '',null,speed) as speed
     ,speed_km
     ,if(heading = '',null,heading) as heading
     ,data_source
     ,if(registration='',null,registration) as registration
     ,if(origin_airport3_code = '',null,origin_airport3_code) as origin_airport3_code
     ,if(destination_airport3_code='',null,destination_airport3_code) as destination_airport3_code
     ,if(airlines_icao = '',null,airlines_icao) as airlines_icao
     ,airlines_name
     ,country_code
     ,country_name
     ,if(num = '',null,num) as num
     ,if(station = '',null,station) as station
     ,source
     ,null as flight_special_flag
     ,source_longitude
     ,source_latitude
     ,destination_longitude
     ,destination_latitude
     ,flight_status
     ,if(num2 = '',null,num2) as num2
     ,expected_landing_time
     ,expected_landing_time_format
     ,if(flight_photo='',null,flight_photo) as flight_photo
     ,flight_departure_time
     ,flight_departure_time_format
     ,un_konwn
     ,to_destination_distance
     ,estimated_landing_duration
     ,if(s_mode='',null,s_mode) as s_mode
     ,position_country_code2
     ,friend_foe
     ,sea_id
     ,sea_name
     ,update_time
from doris_ecs.sa.dwd_aircraft_list_all_info
where acquire_timestamp_format
between concat(to_date(days_sub(now(),1)),' 16:00:00') and concat(to_date(days_sub(now(),1)),' 23:59:59')
;


select sleep(20) as sleep3;



-- 表：dws_aircraft_list_status_info -- 飞机状态数据
insert into sa.dws_aircraft_list_status_info
select
    flight_trace_id
     ,acquire_timestamp_format
     ,if(flight_no = '',null,flight_no) as flight_no
     ,acquire_timestamp
     ,latitude
     ,longitude
     ,altitude
     ,altitude_m
     ,if(flight_type = '',null,flight_type) as flight_type
     ,if(speed = '',null,speed) as speed
     ,speed_km
     ,if(heading = '',null,heading) as heading
     ,data_source
     ,if(registration='',null,registration) as registration
     ,if(origin_airport3_code = '',null,origin_airport3_code) as origin_airport3_code
     ,if(destination_airport3_code='',null,destination_airport3_code) as destination_airport3_code
     ,if(airlines_icao = '',null,airlines_icao) as airlines_icao
     ,airlines_name
     ,country_code
     ,country_name
     ,if(num = '',null,num) as num
     ,if(station = '',null,station) as station
     ,source
     ,null as flight_special_flag
     ,source_longitude
     ,source_latitude
     ,destination_longitude
     ,destination_latitude
     ,flight_status
     ,if(num2 = '',null,num2) as num2
     ,expected_landing_time
     ,expected_landing_time_format
     ,if(flight_photo='',null,flight_photo) as flight_photo
     ,flight_departure_time
     ,flight_departure_time_format
     ,un_konwn
     ,to_destination_distance
     ,estimated_landing_duration
     ,if(s_mode='',null,s_mode) as s_mode
     ,position_country_code2
     ,friend_foe
     ,sea_id
     ,sea_name
     ,update_time
from doris_ecs.sa.dws_aircraft_list_status_info
where acquire_timestamp_format
between to_date(days_sub(now(),1)) and to_date(now())
;


select sleep(10) as sleep4;



-- 表：dws_aircraft_list_all_info_day -- 飞机按天表
insert into sa.dws_aircraft_list_all_info_day
select
    flight_trace_id
     ,acquire_timestamp_format
     ,if(flight_no = '',null,flight_no) as flight_no
     ,acquire_timestamp
     ,latitude
     ,longitude
     ,altitude
     ,altitude_m
     ,if(flight_type = '',null,flight_type) as flight_type
     ,if(speed = '',null,speed) as speed
     ,speed_km
     ,if(heading = '',null,heading) as heading
     ,data_source
     ,if(registration='',null,registration) as registration
     ,if(origin_airport3_code = '',null,origin_airport3_code) as origin_airport3_code
     ,if(destination_airport3_code='',null,destination_airport3_code) as destination_airport3_code
     ,if(airlines_icao = '',null,airlines_icao) as airlines_icao
     ,airlines_name
     ,country_code
     ,country_name
     ,if(num = '',null,num) as num
     ,if(station = '',null,station) as station
     ,source
     ,null as flight_special_flag
     ,source_longitude
     ,source_latitude
     ,destination_longitude
     ,destination_latitude
     ,flight_status
     ,if(num2 = '',null,num2) as num2
     ,expected_landing_time
     ,expected_landing_time_format
     ,if(flight_photo='',null,flight_photo) as flight_photo
     ,flight_departure_time
     ,flight_departure_time_format
     ,un_konwn
     ,to_destination_distance
     ,estimated_landing_duration
     ,if(s_mode='',null,s_mode) as s_mode
     ,position_country_code2
     ,friend_foe
     ,update_time
from doris_ecs.sa.dws_aircraft_list_all_info_day
where acquire_timestamp_format
between to_date(days_sub(now(),1)) and to_date(now())
;


select sleep(10) as sleep5;


-- 表：dws_aircraft_list_base_info_rt -- 飞机航班、注册号、类型对应关系
-- insert into sa.dws_aircraft_list_base_info_rt
-- select
--     flight_trace_id
--      ,acquire_timestamp_format
--      ,acquire_timestamp
--      ,flight_no
--      ,flight_type
--      ,registration
--      ,remark
--      ,create_by
--      ,update_time
-- from doris_ecs.sa.dws_aircraft_list_base_info_rt
-- where update_time between to_date(days_sub(now(),1)) and to_date(now())
-- ;


-- select sleep(10) as sleep6;


-- 表：dws_aircraft_search_rt -- 飞机搜索表
-- insert into sa.dws_aircraft_search_rt
-- select
--     flight_id_no
--      ,flight_id
--      ,flight_no
--      ,flight_trace_id
--      ,registration
--      ,acquire_timestamp_format
--      ,acquire_timestamp
--      ,latitude
--      ,longitude
--      ,altitude
--      ,altitude_m
--      ,flight_type
--      ,speed
--      ,speed_km
--      ,heading
--      ,data_source
--      ,origin_airport3_code
--      ,origin_airport_e_name
--      ,origin_airport_c_name
--      ,destination_airport3_code
--      ,destination_airport_e_name
--      ,destination_airport_c_name
--      ,airlines_icao
--      ,airlines_name
--      ,country_code
--      ,country_name
--      ,num
--      ,station
--      ,source
--      ,null as flight_special_flag
--      ,source_longitude
--      ,source_latitude
--      ,destination_longitude
--      ,destination_latitude
--      ,flight_status
--      ,squawk_code
--      ,expected_landing_time
--      ,expected_landing_time_format
--      ,flight_photo
--      ,flight_departure_time
--      ,flight_departure_time_format
--      ,un_konwn
--      ,to_destination_distance
--      ,estimated_landing_duration
--      ,icao_code
--      ,position_country_code2
--      ,friend_foe
--      ,sea_id
--      ,sea_name
--      ,update_time
-- from doris_ecs.sa.dws_aircraft_search_rt
-- where acquire_timestamp_format between to_date(days_sub(now(),1)) and to_date(now())
-- ;


-- select sleep(10) as sleep7;


-- 表：dwd_ais_vessel_all_info -- 船舶全量表
insert into sa.dwd_ais_vessel_all_info
select
    vessel_id
     ,acquire_timestamp_format
     ,acquire_timestamp
     ,vessel_name
     ,c_name
     ,imo
     ,mmsi
     ,callsign
     ,rate_of_turn
     ,orientation
     ,master_image_id
     ,lng
     ,lat
     ,source
     ,speed
     ,speed_km
     ,vessel_class
     ,vessel_class_name
     ,vessel_type
     ,vessel_type_name
     ,draught
     ,cn_iso2
     ,country_name
     ,nav_status
     ,nav_status_name
     ,dimensions_01
     ,dimensions_02
     ,dimensions_03
     ,dimensions_04
     ,block_map_index
     ,block_range_x
     ,block_range_y
     ,position_country_code2
     ,friend_foe
     ,sea_id
     ,sea_name
     ,update_time
from doris_ecs.sa.dwd_ais_vessel_all_info
where acquire_timestamp_format between to_date(days_sub(now(),1)) and to_date(now())
;


select sleep(20) as sleep6;


-- 表：dws_ais_vessel_status_info -- 船舶状态表
insert into sa.dws_ais_vessel_status_info
select
    vessel_id
     ,acquire_timestamp_format
     ,acquire_timestamp
     ,vessel_name
     ,c_name
     ,imo
     ,mmsi
     ,callsign
     ,rate_of_turn
     ,orientation
     ,master_image_id
     ,lng
     ,lat
     ,source
     ,speed
     ,speed_km
     ,vessel_class
     ,vessel_class_name
     ,vessel_type
     ,vessel_type_name
     ,draught
     ,cn_iso2
     ,country_name
     ,nation_flag_minio_url_jpg
     ,nav_status
     ,nav_status_name
     ,dimensions_01
     ,dimensions_02
     ,dimensions_03
     ,dimensions_04
     ,block_map_index
     ,block_range_x
     ,block_range_y
     ,position_country_code2
     ,friend_foe
     ,sea_id
     ,sea_name
     ,update_time
from doris_ecs.sa.dws_ais_vessel_status_info
where acquire_timestamp_format between to_date(days_sub(now(),1)) and to_date(now())
;


select sleep(10) as sleep7;


-- 表：dws_ais_vessel_all_info_day -- 船舶按天
insert into sa.dws_ais_vessel_all_info_day
select
    *
from doris_ecs.sa.dws_ais_vessel_all_info_day
where acquire_timestamp_format between to_date(days_sub(now(),1)) and to_date(now())
;


select sleep(10) as sleep8;


-- 表：dwd_ais_vessel_port_all_info -- 船舶出发到达港口全量
insert into sa.dwd_ais_vessel_port_all_info
select
    *
from doris_ecs.sa.dwd_ais_vessel_port_all_info
where acquire_timestamp_format between to_date(days_sub(now(),1)) and to_date(now())
;


select sleep(10) as sleep9;


-- 表：dws_ais_vessel_port_status_info -- 船舶出发到达港口状态
insert into sa.dws_ais_vessel_port_status_info
select
    *
from doris_ecs.sa.dws_ais_vessel_port_status_info
where acquire_timestamp_format between to_date(days_sub(now(),1)) and to_date(now())
;


select sleep(10) as sleep10;


-- 表：dwd_satellite_all_info -- 卫星
insert into sa.dwd_satellite_all_info
select
    *
from doris_ecs.sa.dwd_satellite_all_info
where current_date >= to_date(days_sub(now(),1))
;


select sleep(10) as sleep11;


-- 表：dwd_vessel_list_all_rt -- marinetraffic船舶全量数据
insert into sa.dwd_vessel_list_all_rt
select
    *
from doris_ecs.sa.dwd_vessel_list_all_rt
where acquire_timestamp_format between to_date(days_sub(now(),1)) and to_date(now())
;


select sleep(20) as sleep12;


-- 表：dws_vessel_list_status_rt -- marinetraffic船舶状态数据
insert into sa.dws_vessel_list_status_rt
select
    *
from doris_ecs.sa.dws_vessel_list_status_rt
where acquire_timestamp_format
between to_date(days_sub(now(),1)) and to_date(now())
;


select sleep(10) as sleep13;


-- 表：dwd_adsbexchange_aircraft_list_rt -- adsbexchange飞机全量数据
insert into sa.dwd_adsbexchange_aircraft_list_rt
select
    *
from doris_ecs.sa.dwd_adsbexchange_aircraft_list_rt
where acquire_timestamp_format
between concat(to_date(days_sub(now(),1)),' 00:00:00') and concat(to_date(days_sub(now(),1)),' 08:00:00')
;


select sleep(20) as sleep14;


-- 表：dwd_adsbexchange_aircraft_list_rt -- adsbexchange飞机全量数据
insert into sa.dwd_adsbexchange_aircraft_list_rt
select
    *
from doris_ecs.sa.dwd_adsbexchange_aircraft_list_rt
where acquire_timestamp_format
between concat(to_date(days_sub(now(),1)),' 08:00:00') and concat(to_date(days_sub(now(),1)),' 16:00:00')
;


select sleep(20) as sleep15;


-- 表：dwd_adsbexchange_aircraft_list_rt -- adsbexchange飞机全量数据
insert into sa.dwd_adsbexchange_aircraft_list_rt
select
    *
from doris_ecs.sa.dwd_adsbexchange_aircraft_list_rt
where acquire_timestamp_format
between concat(to_date(days_sub(now(),1)),' 16:00:00') and concat(to_date(days_sub(now(),1)),' 23:59:59')
;


select sleep(20) as sleep16;


-- 表：dws_ais_vessel_detail_static_attribute -- 全量静态属性表
insert into sa.dws_ais_vessel_detail_static_attribute
select
    vessel_id
     ,imo
     ,mmsi
     ,callsign
     ,year_built
     ,vessel_type
     ,vessel_type_name
     ,vessel_class
     ,vessel_class_name
     ,name
     ,c_name
     ,draught_average
     ,speed_average
     ,speed_max
     ,length
     ,width
     ,height
     ,owner
     ,risk_rating
     ,risk_rating_name
     ,service_status
     ,service_status_name
     ,flag_country_code
     ,country_name
     ,source
     ,gross_tonnage
     ,deadweight
     ,rate_of_turn
     ,null as is_on_my_fleet
     ,null as is_on_shared_fleet
     ,null as is_on_own_fleet
     ,timestamp
     ,update_time
from doris_ecs.sa.dws_ais_vessel_detail_static_attribute
where update_time between to_date(days_sub(now(),2)) and to_date(now())
;


select sleep(20) as sleep17;


-- 表：dws_aircraft_combine_status_rt -- 飞机融合状态数据表
insert into sa.dws_aircraft_combine_status_rt
select
    *
from doris_ecs.sa.dws_aircraft_combine_status_rt
where acquire_time
between to_date(days_sub(now(),1)) and to_date(now())
;


select sleep(20) as sleep18;


-- 表：dws_aircraft_combine_list_rt -- 飞机融合航班表
insert into sa.dws_flight_segment_rt
select
  *
from doris_ecs.sa.dws_flight_segment_rt
where update_time
between to_date(days_sub(now(),1)) and to_date(now())
;