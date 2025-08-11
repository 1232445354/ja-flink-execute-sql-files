#!/bin/bash

start_day=${1}
end_day=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dwd_ais_vessel_all_info
select
	*
from doris_202.sa.dwd_ais_vessel_all_info
where acquire_timestamp_format between '${start_day}' and '${end_day}'

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"

