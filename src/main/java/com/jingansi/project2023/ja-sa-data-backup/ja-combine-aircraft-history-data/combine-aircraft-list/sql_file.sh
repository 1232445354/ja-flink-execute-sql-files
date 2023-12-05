#!/bin/bash

start_day=${1}
end_day=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dws_aircraft_combine_list_rt
select
	if(s_mode is not null and s_mode <> '',s_mode,flight_trace_id) as flight_id
	,tt.acquire_timestamp_format as acquire_time
	,1 as src_code
	,if(s_mode = '',null,s_mode) as icao_code
	,if(tt.registration is null or tt.registration in('','BLOCKED','VARIOUS','TACTICAL'),a.registration,tt.registration) as registration
	,if(flight_no = '',null,flight_no) as flight_no
	,null as callsign
	,if(tt.flight_type = '' or tt.flight_type is null,a.icao_type,tt.flight_type) as flight_type
	,a.is_mil as is_military
	,if(s_mode is not null and s_mode <> '','hex','trace_id') as pk_type
	,tt.flight_trace_id as src_pk
	,b.category_code as flight_category
	,b.category_c_name as flight_category_name
	,tt.longitude as lng
	,tt.latitude as lat
	,if(tt.speed = '',null,tt.speed) as speed
	,tt.speed_km
	,tt.altitude as altitude_baro
	,tt.altitude_m as altitude_baro_m
	,null as altitude_geom
	,null as altitude_geom_m
	,if(tt.heading = '',null,tt.heading) as heading
	, if(num2 = '',null,num2) as squawk_code
	,tt.flight_status
	,if(tt.flight_special_flag = true,1,0) as special
	,if(origin_airport3_code = '',null,origin_airport3_code) as origin_airport3_code
	,coalesce(t1.airport,t2.airport)  as origin_airport_e_name
	,coalesce(t1.airport_name,t2.airport_name) as origin_airport_c_name
	,tt.source_longitude as origin_lng
	,tt.source_latitude as origin_lat
	,if(tt.destination_airport3_code = '',null,tt.destination_airport3_code) as dest_airport3_code
	,coalesce(t3.airport,t4.airport) as dest_airport_e_name
	,coalesce(t3.airport_name,t4.airport_name) as dest_airport_c_name
	,tt.destination_longitude as dest_lng
	,tt.destination_latitude as dest_lat
	,if(flight_photo='',null,flight_photo) as flight_photo
	,tt.flight_departure_time_format as flight_departure_time
	,tt.expected_landing_time_format as expected_landing_time
	,tt.to_destination_distance
	,tt.estimated_landing_duration
	,if(tt.airlines_icao = '',null,tt.airlines_icao) as airlines_icao
	,c.e_name as airlines_e_name
	,c.c_name as airlines_c_name
	,tt.country_code
	,d.c_name as country_name
	,tt.data_source
	,tt.source
	,tt.position_country_code2
	,e.c_name as position_country_name
	,tt.friend_foe
	,tt.sea_id
	,tt.sea_name
	,null as h3_code
	,json_object(
		'num',if(num = '',null,num),
		'un_konwn',if(un_konwn= '',null,un_konwn),
		'station',if(station = '',null,station),
		'expected_landing_time',if(expected_landing_time = '',null,expected_landing_time),
		'flight_departure_time',if(flight_departure_time = '',null,flight_departure_time)
	) as extend_info
	,tt.update_time
from (
select
	*
from sa.dwd_aircraft_list_all_info
where acquire_timestamp_format between '${start_day}' and '${end_day}'

) as tt
left join sa.dws_airport_detail_info as t1
on tt.origin_airport3_code = t1.icao

left join sa.dws_airport_detail_info as t2
on tt.origin_airport3_code = t2.iata

left join sa.dws_airport_detail_info as t3
on tt.destination_airport3_code = t3.icao

left join sa.dws_airport_detail_info as t4
on tt.destination_airport3_code = t4.iata

left join sa.dws_aircraft_info as a
on if(tt.s_mode = '', null,tt.s_mode) = a.icao_code

left join (select * from sa.dim_aircraft_type_category where category_code is not null) as b
on if(tt.flight_type is not null and tt.flight_type <> '',tt.flight_type,a.icao_type) = b.id

left join sa.dim_airline_list_info as c
on if(tt.airlines_icao = '',null,tt.airlines_icao) = c.icao

left join sa.dim_country_code_name_info as d
on tt.country_code = d.country_code2

left join sa.dim_country_code_name_info as e
on tt.position_country_code2 = e.country_code2
"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"