#!/bin/bash

# start_time:2023-12-01 01:00:00
# end_time:2023-12-01 02:00:00
start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

-- 表：dws_aircraft_combine_list_rt -- 飞机融合全量数据表  update_time
insert into sa.dws_aircraft_combine_list_rt
select
  *
from doris_idc.sa.dws_aircraft_combine_list_rt
where acquire_time
between '${start_time}' and '${end_time}';

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"