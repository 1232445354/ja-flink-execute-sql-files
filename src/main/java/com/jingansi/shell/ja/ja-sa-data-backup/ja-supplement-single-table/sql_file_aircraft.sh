#!/bin/bash

start_day=${1}
end_day=${2}
table_name=${3}
time_column=${4}
catalog_info=${5}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.${table_name}
select
 flight_id
,acquire_time
,src_code
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
,longitude
,latitude
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
,null as flight_photo
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
,null as source
,position_country_code2
,position_country_name
,null as friend_foe
,filter_col
,null as sea_id
,null as sea_name
,null as h3_code
,null as extend_info
,update_time
from ${catalog_info}.sa.${table_name}
where acquire_time >= '${start_day}' and acquire_time < '${end_day}'

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"