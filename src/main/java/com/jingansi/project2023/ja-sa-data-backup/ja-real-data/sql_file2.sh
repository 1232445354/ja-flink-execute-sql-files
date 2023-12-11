#!/bin/bash

start_day=${1}
end_day=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

-- 表：dws_aircraft_combine_list_rt -- 飞机融合全量数据表
insert into sa.dws_aircraft_combine_list_rt
select
  *
from doris_ecs.sa.dws_aircraft_combine_list_rt
where acquire_time
between '${start_day}' and '${end_day}';

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"