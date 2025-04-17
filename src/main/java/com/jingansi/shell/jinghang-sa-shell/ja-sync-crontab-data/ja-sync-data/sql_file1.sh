#!/bin/bash

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into sa.dws_et_satellite_info(
	id
	,acquire_time
	,satellite_no
	,intl_code
	,e_name
	,satellite_altname
	,type_code
	,type_name
	,usage_code
	,usage_name
	,constellation
	,country_code
	,country_name
	,country_code_flag
	,object_type_code
	,object_type_name
	,object_status_code
	,object_status_name
	,classification
	,owner_code
	,owner_name
	,dry_mass
	,total_mass
	,length
	,diamet
	,span
	,shape
	,rcs_size
	,launch_date
	,lauch_site_code
	,lauch_site_name
	,decay_date
	,period
	,inclination
	,apogee
	,perigee
	,sat_desc
	,vehicle
	,image_link
	,application
	,operator
	,contractors
	,equipment
	,configuration
	,propulsion
	,sat_power
	,lifetime
	,remark
	,extend_info
	,friend_foe
	,search_content
	,update_time
)

select
	id
	,acquire_time
	,satellite_no
	,intl_code
	,e_name
	,satellite_altname
	,type_code
	,type_name
	,usage_code
	,usage_name
	,constellation
	,country_code
	,country_name
	,country_code_flag
	,object_type_code
	,object_type_name
	,object_status_code
	,object_status_name
	,classification
	,owner_code
	,owner_name
	,dry_mass
	,total_mass
	,length
	,diamet
	,span
	,shape
	,rcs_size
	,launch_date
	,lauch_site_code
	,lauch_site_name
	,decay_date
	,period
	,inclination
	,apogee
	,perigee
	,sat_desc
	,vehicle
	,image_link
	,application
	,operator
	,contractors
	,equipment
	,configuration
	,propulsion
	,sat_power
	,lifetime
	,remark
	,extend_info
	,friend_foe
	,search_content
	,update_time
from doris_idc.sa.dws_et_satellite_info

where acquire_time > to_date(date_sub(now(),interval 1 day));

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"