#!/bin/bash
mysql -h172.21.30.105 -P31030 -uadmin -pJingansi@110  -e "
insert into sa.dwd_aircraft_list_all_info
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
  ,sea_id
  ,sea_name
  ,now() as update_time
from doris_ecs.sa.dwd_aircraft_list_all_info
where acquire_timestamp_format > DATE_SUB(now(),interval 3 minute);


insert into sa.dws_aircraft_list_status_info
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
  ,sea_id
  ,sea_name
  ,now() as update_time
from doris_ecs.sa.dws_aircraft_list_status_info
where acquire_timestamp_format > DATE_SUB(now(),interval 3 minute);
"
echo $(date)
echo "执行完成....."

