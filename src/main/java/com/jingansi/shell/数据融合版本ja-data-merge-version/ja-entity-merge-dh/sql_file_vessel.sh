#!/bin/bash

cur_hour_time=${1}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dws_bhv_vessel_last_location_dh
select
	  vessel_id
    ,date_trunc('${cur_hour_time}','hour') as merge_time
    ,src_code
    ,acquire_time
    ,src_pk
    ,e_name
    ,imo
    ,mmsi
    ,callsign
    ,rate_of_turn
    ,orientation
    ,heading
    ,longitude
    ,latitude
    ,speed
    ,speed_km
    ,draught
    ,width
    ,length
    ,nav_status
    ,ais_source_type
    ,ais_type_code
    ,vessel_class_code
    ,vessel_type_code
    ,country_code
    ,eta
    ,dest_code
    ,dest_name
    ,block_map_index
    ,block_range_x
    ,block_range_y
    ,position_country_code2
    ,extend_info
    ,query_cols
    ,sea_id
    ,update_time

from (

select
	*,
	row_number()over(partition by vessel_id order by acquire_time desc) as rk
from (
  select
    vessel_id,src_code,acquire_time,src_pk,
    vessel_name as e_name,
    imo,mmsi,callsign,rate_of_turn,orientation,heading,
    lng as longitude,
    lat as latitude,
    speed,speed_km,draught,width,length,nav_status,ais_source_type,ais_type_code,
    big_type_num_code as vessel_class_code,
    small_type_num_code as vessel_type_code,
    flag_country as country_code,
    eta,dest_code,dest_name,block_map_index,block_range_x,block_range_y,position_country_code2,extend_info,query_cols,sea_id,update_time
  from sa.dws_vessel_bhv_track_rt
  where acquire_time between date_trunc(hours_sub('${cur_hour_time}',1),'hour') and date_trunc('${cur_hour_time}','hour')

  union all

  select
	  *
	  except(merge_time)
  from sa.dws_bhv_vessel_last_location_dh
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
