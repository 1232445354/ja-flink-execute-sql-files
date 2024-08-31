#!/bin/bash

day=${1}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

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
where acquire_timestamp_format between '${day} 00:00:00' and '${day} 12:00:00';



select sleep(20) as tt;


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
where acquire_timestamp_format between '${day} 12:00:00' and '${day} 23:59:59'

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"