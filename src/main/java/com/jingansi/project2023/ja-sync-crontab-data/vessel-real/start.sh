#!/bin/bash

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
where acquire_timestamp_format > DATE_SUB(now(),interval 30 minute);


insert into sa.dws_ais_vessel_status_info
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
  ,nation_flag_minio_url_jpg
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
from doris_ecs.sa.dws_ais_vessel_status_info
where acquire_timestamp_format > DATE_SUB(now(),interval 30 minute);
"


echo -en "开始同步数据...$(date)\n"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"

echo -en "数据同步SUCCESS.......$(date)\n"
echo -en "------------------------------------------\n"



