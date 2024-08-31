#!/bin/bash

cur_time=${1}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dws_bhv_aircraft_last_location_fd
select
	flight_id
	,date_trunc('${cur_time}','day') as merge_time
	,acquire_time
	,src_code
	,icao_code
	,registration
	,flight_no
	,callsign
	,flight_type
	,longitude
	,latitude
	,speed_km
	,altitude_baro
	,altitude_baro_m
	,altitude_geom
	,altitude_geom_m
	,heading
	,squawk_code
	,flight_status
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
	,flight_departure_time
	,expected_landing_time
	,to_destination_distance
	,estimated_landing_duration
	,data_source
	,source
	,position_country_code2
	,friend_foe
	,filter_col
	,sea_id
	,sea_name
	,h3_code
	,update_time
from (

select
	*,
	row_number()over(partition by flight_id order by acquire_time desc) as rk
from (
  select
    *
  from sa.dws_bhv_aircraft_last_location_fd
  where merge_time = days_sub(date_trunc('${cur_time}','day'),1)

  union all

  select
	  *
  from sa.dws_bhv_aircraft_last_location_dh
  where merge_time = date_trunc('${cur_time}','day')
  ) as t1
) as t2
where rk = 1


"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
