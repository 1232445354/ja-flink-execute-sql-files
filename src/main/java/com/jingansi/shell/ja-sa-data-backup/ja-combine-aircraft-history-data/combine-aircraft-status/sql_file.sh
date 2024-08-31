#!/bin/bash

start_day=${1}
end_day=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dws_aircraft_combine_status_rt
select
		flight_id
		,src_code
		,acquire_time
		,icao_code
		,registration
		,flight_no
		,callsign
		,flight_type
		,is_military
		,pk_type
		,src_pk
		,flight_category
		,flight_category_name
		,lng
		,lat
		,speed
		,speed_km
		,altitude_baro
		,altitude_baro_m
		,altitude_geom
		,altitude_geom_m
		,heading
		,squawk_code
		,flight_status
		,special
		,origin_airport3_code
		,origin_airport_e_name
		,origin_airport_c_name
		,origin_lng
		,origin_lat
		,dest_airport3_code
		,dest_airport_e_name
		,dest_airport_c_name
		,dest_lng
		,dest_lat
		,flight_photo
		,flight_departure_time
		,expected_landing_time
		,to_destination_distance
		,estimated_landing_duration
		,airlines_icao
		,airlines_e_name
		,airlines_c_name
		,country_code
		,country_name
		,data_source
		,source
		,position_country_code2
		,position_country_name
		,friend_foe
		,sea_id
		,sea_name
		,h3_code
		,extend_info
		,update_time
from (
select
	*,
	row_number()over(partition by flight_id order by acquire_time desc) as rk
from sa.dws_aircraft_combine_list_rt
where acquire_time between '${start_day}' and '${end_day}'
) as t1
where rk = 1

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"