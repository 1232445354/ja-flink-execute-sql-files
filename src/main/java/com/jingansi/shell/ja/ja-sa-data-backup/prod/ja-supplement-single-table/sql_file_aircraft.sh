#!/bin/bash

start_day=${1}
end_day=${2}
table_name=${3}
time_column=${4}
catalog_info=${5}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.${table_name} (
flight_id
,acquire_time
,src_code
,registration
,flight_no
,longitude
,latitude
,speed_km
,altitude_baro_m
,heading
,squawk_code
,flight_status
,special
,origin_airport3_code
,origin_airport_e_name
,origin_airport_c_name
,dest_airport3_code
,dest_airport_e_name
,dest_airport_c_name
,airlines_icao
,airlines_e_name
,airlines_c_name
,flight_departure_time
,expected_landing_time
,to_destination_distance
,estimated_landing_duration
,data_source
,source
,position_country_code2
,position_country_name
,filter_col
,sea_id
,update_time
)
select
 flight_id
,acquire_time
,src_code
,registration
,flight_no
,longitude
,latitude
,speed_km
,altitude_baro_m
,heading
,squawk_code
,flight_status
,special
,origin_airport3_code
,origin_airport_e_name
,origin_airport_c_name
,dest_airport3_code
,dest_airport_e_name
,dest_airport_c_name
,airlines_icao
,airlines_e_name
,airlines_c_name
,flight_departure_time
,expected_landing_time
,to_destination_distance
,estimated_landing_duration
,data_source
,source
,position_country_code2
,position_country_name
,filter_col
,sea_id
,update_time
from ${catalog_info}.sa.${table_name}
where acquire_time >= '${start_day}' and acquire_time < '${end_day}'

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"