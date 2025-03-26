#!/bin/bash

start_day=${1}
end_day=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dws_atr_aircraft_image_id_info
select
	flight_photo as image_id,
	flight_id,
	max(acquire_time) as acquire_time,
	'正常' as remark,
	null as create_by,
	now() as update_time
from sa.dws_aircraft_combine_list_rt
where acquire_time between '${start_day}' and '${end_day}'
	and src_code = 1
	and flight_photo is not null
	and flight_photo <> ''
group by flight_photo,flight_id

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
