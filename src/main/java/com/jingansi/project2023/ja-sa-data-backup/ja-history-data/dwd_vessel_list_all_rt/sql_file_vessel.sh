#!/bin/bash

day=${1}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into sa.dwd_vessel_list_all_rt
select
	*
from doris_ecs.sa.dwd_vessel_list_all_rt
where acquire_timestamp_format between '${day} 00:00:00' and '${day} 12:00:00';


insert into sa.dwd_vessel_list_all_rt
select
	*
from doris_ecs.sa.dwd_vessel_list_all_rt
where acquire_timestamp_format between '${day} 12:00:00' and '${day} 23:59:59'
"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"