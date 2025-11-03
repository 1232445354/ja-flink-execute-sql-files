#!/bin/bash

table_name=${1}
time_column=${2}
start_time=${3}
end_time=${4}
type=${5}
catalog_name=${6}
min_lon=${7}
max_lon=${8}

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh

# 正常的执行sql
sql="
insert into sa.${table_name}
select
  *
from ${catalog_name}.sa.${table_name}
where \`${time_column}\` between '${start_time}' and '${end_time}';
"

sql_merge="
insert into sa.${table_name}
select
  *
from ${catalog_name}.sa.${table_name}
where ${time_column} between '${start_time}' and '${end_time}'
  and lng_key >= ${min_lon}
  and lng_key < ${max_lon};
"


sql_aircraft="
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
  ,null as country_code
  ,null as country_name
  ,data_source
  ,source
  ,position_country_code2
  ,position_country_name
  ,null as friend_foe
  ,filter_col
  ,sea_id
  ,sea_name
  ,h3_code
  ,null as extend_info
  ,update_time
from ${catalog_name}.sa.${table_name}
where \`${time_column}\` between '${start_time}' and '${end_time}';
"


if [ "$table_name" = "dwd_bhv_aircraft_combine_rt" ]; then
  mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql_aircraft}"

elif [ "$type" = "common" ]; then
  mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql}"
elif [ "$type" = "merge" ]; then
  mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql_merge}"
fi

