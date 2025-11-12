#!/bin/bash

start_day=${1}
end_day=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dws_aircraft_combine_list_rt
select
	*
from doris_idc.sa.dws_aircraft_combine_list_rt
where acquire_time between '${start_day}' and '${end_day}'

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"

