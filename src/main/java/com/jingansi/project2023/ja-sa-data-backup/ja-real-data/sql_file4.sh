#!/bin/bash

# start_time:2023-12-01 00:00:00
# end_time:2023-12-01 06:00:00
start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dwd_vessel_list_all_rt
select
  *
from doris_ecs.sa.dwd_vessel_list_all_rt
where acquire_timestamp_format between '${start_time}' and '${end_time}';

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"