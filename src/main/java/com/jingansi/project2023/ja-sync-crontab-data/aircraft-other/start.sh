#!/bin/bash
mysql -h172.21.30.202 -P31030 -uadmin -pJingansi@110  -e "

insert into sa.dws_aircraft_list_base_info_rt
select
  flight_trace_id
  ,acquire_timestamp_format
  ,acquire_timestamp
  ,flight_no
  ,flight_type
  ,registration
  ,remark
  ,create_by
  ,update_time
from doris_idc.sa.dws_aircraft_list_base_info_rt
where acquire_timestamp_format > DATE_SUB(now(),interval 1 day);


insert into sa.dws_aircraft_search_rt
select
  flight_id_no
  ,flight_id
  ,flight_no
  ,flight_trace_id
  ,registration
  ,acquire_timestamp_format
  ,acquire_timestamp
  ,latitude
  ,longitude
  ,altitude
  ,altitude_m
  ,flight_type
  ,speed
  ,speed_km
  ,heading
  ,data_source
  ,origin_airport3_code
  ,origin_airport_e_name
  ,origin_airport_c_name
  ,destination_airport3_code
  ,destination_airport_e_name
  ,destination_airport_c_name
  ,airlines_icao
  ,airlines_name
  ,country_code
  ,country_name
  ,num
  ,station
  ,source
  ,null as flight_special_flag
  ,source_longitude
  ,source_latitude
  ,destination_longitude
  ,destination_latitude
  ,flight_status
  ,squawk_code
  ,expected_landing_time
  ,expected_landing_time_format
  ,flight_photo
  ,flight_departure_time
  ,flight_departure_time_format
  ,un_konwn
  ,to_destination_distance
  ,estimated_landing_duration
  ,icao_code
  ,position_country_code2
  ,friend_foe
  ,sea_id
  ,sea_name
  ,update_time
from doris_idc.sa.dws_aircraft_search_rt
where acquire_timestamp_format > DATE_SUB(now(),interval 1 day);


insert into sa.dws_aircraft_list_all_info_day
select
	flight_trace_id
	,acquire_timestamp_format
	,flight_no
	,acquire_timestamp
	,latitude
	,longitude
	,altitude
	,altitude_m
	,flight_type
	,speed
	,speed_km
	,heading
	,data_source
	,registration
	,origin_airport3_code
	,destination_airport3_code
	,airlines_icao
	,airlines_name
	,country_code
	,country_name
	,num
	,station
	,source
	,null as flight_special_flag
	,source_longitude
	,source_latitude
	,destination_longitude
	,destination_latitude
	,flight_status
	,num2
	,expected_landing_time
	,expected_landing_time_format
	,flight_photo
	,flight_departure_time
	,flight_departure_time_format
	,un_konwn
	,to_destination_distance
	,estimated_landing_duration
	,s_mode
	,position_country_code2
	,friend_foe
	,update_time
from doris_idc.sa.dws_aircraft_list_all_info_day
where acquire_timestamp_format > DATE_SUB(now(),interval 1 day);

"
echo $(date)
echo "执行完成....."


