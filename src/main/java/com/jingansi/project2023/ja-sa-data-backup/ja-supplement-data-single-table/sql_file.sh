#!/bin/bash

start_day=${1}
end_day=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="


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
	,null
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
	,update_time
from doris_ecs.sa.dwd_aircraft_list_all_info
where acquire_timestamp_format between '${start_day}' and '${end_day}'

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"