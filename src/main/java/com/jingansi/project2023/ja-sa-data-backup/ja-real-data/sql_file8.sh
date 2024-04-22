#!/bin/bash

# start_time:2023-12-01 00:00:00
# end_time:2023-12-01 02:00:00
start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dwd_ais_landbased_vessel_list
select
  *
from doris_ecs.sa.dwd_ais_landbased_vessel_list
where acquire_time between '${start_time}' and '${end_time}'

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"