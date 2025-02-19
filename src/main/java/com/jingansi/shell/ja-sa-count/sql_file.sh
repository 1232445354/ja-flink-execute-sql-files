#!/bin/bash

start_day=${1}
end_day=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dim_sa_count
select
DISTINCT flight_id ,
2 as type
from sa.dwd_bhv_aircraft_combine_rt
where acquire_time between '${start_day}' and '${end_day}'
	and (
		(longitude between 72.97840703184593 and 180       and latitude        between -52.943613087809844 and 54.75064979538746)
	 or (longitude between -180 and -172.68401771576896    and latitude       between -52.943613087809844 and 54.75064979538746)
	)

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"