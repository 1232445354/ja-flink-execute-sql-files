#!/bin/bash

# start_time:2023-12-01 00:00:00
# end_time:2023-12-01 06:00:00
start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dwd_ais_vessel_all_info
select
    vessel_id
     ,acquire_timestamp_format
     ,acquire_timestamp
     ,vessel_name
     ,c_name
     ,imo
     ,mmsi
     ,callsign
     ,rate_of_turn
     ,orientation
     ,master_image_id
     ,lng
     ,lat
     ,source
     ,speed
     ,speed_km
     ,vessel_class
     ,vessel_class_name
     ,vessel_type
     ,vessel_type_name
     ,draught
     ,cn_iso2
     ,country_name
     ,nav_status
     ,nav_status_name
     ,dimensions_01
     ,dimensions_02
     ,dimensions_03
     ,dimensions_04
     ,block_map_index
     ,block_range_x
     ,block_range_y
     ,position_country_code2
     ,friend_foe
     ,sea_id
     ,sea_name
     ,update_time
from doris_ecs.sa.dwd_ais_vessel_all_info
where update_time between '${start_time}' and '${end_time}';

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"