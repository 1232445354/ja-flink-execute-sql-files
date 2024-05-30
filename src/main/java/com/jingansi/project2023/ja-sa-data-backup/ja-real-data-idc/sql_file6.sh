#!/bin/bash

# start_time:2023-12-01 00:00:00
# end_time:2023-12-01 02:00:00
start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dwd_vt_vessel_all_info
select
  *
from doris_idc.sa.dwd_vt_vessel_all_info
where acquire_timestamp_format between '${start_time}' and '${end_time}'

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"