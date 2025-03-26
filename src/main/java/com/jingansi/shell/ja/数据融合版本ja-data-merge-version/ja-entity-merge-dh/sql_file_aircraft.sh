#!/bin/bash

cur_hour_time=${1}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dws_bhv_aircraft_last_location_dh
select
	flight_id
	,date_trunc('${cur_hour_time}','hour') as merge_time
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
    flight_id,acquire_time,src_code,icao_code,registration,flight_no,callsign,flight_type,longitude,latitude,speed_km,altitude_baro,altitude_baro_m
    ,altitude_geom,altitude_geom_m,heading,squawk_code,flight_status,origin_airport3_code,origin_airport_e_name
    ,origin_airport_c_name,origin_lng,origin_lat,dest_airport3_code,dest_airport_e_name,dest_airport_c_name
    ,dest_lng,dest_lat,flight_departure_time,expected_landing_time,to_destination_distance,estimated_landing_duration,data_source
    ,source,position_country_code2,friend_foe,filter_col,sea_id,sea_name,h3_code,update_time
  from sa.dwd_bhv_aircraft_combine_rt
  where acquire_time between date_trunc(hours_sub('${cur_hour_time}',1),'hour') and date_trunc('${cur_hour_time}','hour')
    and src_code in(1,2,3)
    and (data_source != 'ESTI' or  data_source is null)

  union all

  select
	  *
	  except(merge_time)
  from sa.dws_bhv_aircraft_last_location_dh
  where merge_time = date_trunc(hours_sub('${cur_hour_time}',1),'hour')
    and hour(hours_sub('${cur_hour_time}',1)) > 0
  ) as t1
) as t2
where rk = 1

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
