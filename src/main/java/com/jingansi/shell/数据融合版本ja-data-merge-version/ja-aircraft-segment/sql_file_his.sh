#!/bin/bash

start_day=${1}
end_day=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dws_bhv_aircraft_segment_rt
select
	flight_id
	,registration
	,flight_no
	,flight_trace_id
	,start_time
	,end_time
	,flight_duration
	,icao_code
	,flight_type
	,is_military
	,country_code
	,country_name
	,origin_airport3_code
	,origin_airport_e_name
	,origin_airport_c_name
	,dest_airport3_code
	,dest_airport_e_name
	,dest_airport_c_name
	,flight_departure_time
	,expected_landing_time
	,src_cnt
	,concat(
        ifnull(icao_code,''),' ',
        ifnull(registration,''),' ',
        ifnull(flight_type,''),' ',
				ifnull(flight_no,'')
    ) as search_content
	,update_time
from sa.dws_flight_segment_rt
where start_time between '${start_day}' and '${end_day}'

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
