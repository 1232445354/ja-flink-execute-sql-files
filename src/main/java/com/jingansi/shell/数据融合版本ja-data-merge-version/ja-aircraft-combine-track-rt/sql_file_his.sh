#!/bin/bash

start_day=${1}
end_day=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dwd_bhv_aircraft_combine_rt
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
	,lng as longitude
	,lat as latitude
	,speed
	,speed_km
	,altitude_baro
	,altitude_baro_m
	,altitude_geom
	,altitude_geom_m
	,heading
	,squawk_code
	,flight_status1 as flight_status
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
  ,concat(
		ifnull(flight_no,''),'¥',
		ifnull(src_code,''),'¥',
   	ifnull(lng,''),'¥',
   	ifnull(lat,''),'¥',
   	ifnull(speed_km,''),'¥',
   	ifnull(altitude_baro_m,''),'¥',
   	ifnull(heading,''),'¥',
   	ifnull(squawk_code,''),'¥',
   	ifnull(flight_status1,''),'¥',
   	ifnull(origin_airport3_code,''),'¥',
   	ifnull(origin_airport_e_name,''),'¥',
   	ifnull(origin_airport_c_name,''),'¥',
   	ifnull(dest_airport3_code,''),'¥',
   	ifnull(dest_airport_e_name,''),'¥',
   	ifnull(dest_airport_c_name,''),'¥',
   	ifnull(flight_departure_time,''),'¥',
   	ifnull(expected_landing_time,''),'¥',
   	ifnull(position_country_code2,''),'¥',
   	ifnull(sea_id,''),'¥',
    ifnull(data_source,''),'¥',
		ifnull(source,'')
   	) as filter_col
	,sea_id
	,sea_name
	,h3_code
	,extend_info
	,update_time
from (
select
	*,
	case when src_code = 1 and flight_status = 'departed' then 0  -- 飞行
		when src_code = 2 and json_extract(extend_info,'$.airground') = 1 then 1 -- 地面
		when src_code = 2 and json_extract(extend_info,'$.airground') in (0,2,3) then 0 -- 飞行
		when src_code = 3 and json_extract(extend_info,'$.on_ground') = 1 then 1 -- 地面
		when src_code = 3 and json_extract(extend_info,'$.on_ground') = 0 then 0 -- 飞行
	else flight_status end
	as flight_status1
from sa.dws_aircraft_combine_list_rt
where acquire_time between '${start_day}' and '${end_day}'
) as t1


"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
