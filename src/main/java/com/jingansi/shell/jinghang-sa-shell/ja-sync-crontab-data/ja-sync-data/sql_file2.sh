#!/bin/bash

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into doris_jh132.sa.dws_vessel_et_info_rt(
	vessel_id
  ,vessel_name
  ,vessel_c_name
  ,mmsi
  ,imo
  ,callsign
  ,length
  ,width
  ,height
  ,gross_tonnage
  ,deadweight
  ,build_year
  ,service_status_code
  ,service_status_name
  ,registry_port
  ,country_code
  ,country_name
  ,ais_type_code
  ,ais_type_name
  ,vessel_class_code
  ,vessel_class_name
  ,vessel_type_code
  ,vessel_type_name
  ,owner
  ,extend_info
  ,search_content
  ,friend_foe
  ,update_time
)

select
	vessel_id
  ,vessel_name
  ,vessel_c_name
  ,mmsi
  ,imo
  ,callsign
  ,length
  ,width
  ,height
  ,gross_tonnage
  ,deadweight
  ,build_year
  ,service_status_code
  ,service_status_name
  ,registry_port
  ,country_code
  ,country_name
  ,ais_type_code
  ,ais_type_name
  ,vessel_class_code
  ,vessel_class_name
  ,vessel_type_code
  ,vessel_type_name
  ,owner
  ,extend_info
  ,search_content
  ,friend_foe
  ,update_time
from doris_idc.sa.dws_vessel_et_info_rt

where update_time > to_date(date_sub(now(),interval 1 day));

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"