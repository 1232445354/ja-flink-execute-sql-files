#!/bin/bash

start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
update sa.dws_vessel_bhv_track_rt
set query_cols = concat(
query_cols,
ifnull(position_country_code2,''),'¥',
ifnull(sea_id,'')
)
where acquire_time between '${start_time}' and '${end_time}';

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
