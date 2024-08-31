#!/bin/bash

cur_time=${1}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dws_bhv_vessel_last_location_fd
select
	  vessel_id
    ,date_trunc('${cur_time}','day') as merge_time
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
    *
  from sa.dws_bhv_vessel_last_location_fd
  where merge_time = days_sub(date_trunc('${cur_time}','day'),1)

  union all

  select
	  *
  from sa.dws_bhv_vessel_last_location_dh
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
