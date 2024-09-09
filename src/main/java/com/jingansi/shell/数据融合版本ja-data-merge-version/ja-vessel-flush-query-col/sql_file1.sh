#!/bin/bash

start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into sa.dws_vessel_bhv_track_rt

select
vessel_id
,acquire_time
,src_code
,src_pk
,vessel_name
,mmsi
,imo
,callsign
,lng
,lat
,speed
,speed_km
,rate_of_turn
,orientation
,heading
,draught
,nav_status
,eta
,dest_code
,dest_name
,ais_type_code
,big_type_num_code
,small_type_num_code
,length
,width
,ais_source_type
,flag_country
,block_map_index
,block_range_x
,block_range_y
,position_country_code2
,sea_id
,concat(
  ifnull(lng,''),'¥',
  ifnull(lat,''),'¥',
  ifnull(speed,''),'¥',
  ifnull(rate_of_turn,''),'¥',
  ifnull(orientation,''),'¥',
  ifnull(heading,''),'¥',
  ifnull(draught,''),'¥',
  ifnull(nav_status,''),'¥',
  ifnull(position_country_code2,''),'¥',
  ifnull(sea_id,'')
 ) as query_cols
,extend_info
,update_time
from sa.dws_vessel_bhv_track_rt
where acquire_time between '${start_time}' and '${end_time}';

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
