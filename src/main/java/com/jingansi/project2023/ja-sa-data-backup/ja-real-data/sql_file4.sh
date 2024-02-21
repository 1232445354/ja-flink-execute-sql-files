#!/bin/bash

# start_time:2023-12-01 00:00:00
# end_time:2023-12-01 06:00:00
start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dwd_adsbexchange_aircraft_list_rt
select
    *
from doris_ecs.sa.dwd_adsbexchange_aircraft_list_rt
where acquire_timestamp_format
between '${start_time}' and '${start_time}';

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"