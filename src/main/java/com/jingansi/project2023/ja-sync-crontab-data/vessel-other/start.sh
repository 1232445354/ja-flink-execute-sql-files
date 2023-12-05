#!/bin/bash

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into sa.dws_ais_vessel_all_info_day
select
	*
from doris_ecs.sa.dws_ais_vessel_all_info_day
where acquire_timestamp_format > DATE_SUB(now(),interval 1 day);
"

echo -en "开始同步数据...$(date)\n"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"

echo -en "数据同步SUCCESS.......$(date)\n"
echo -en "------------------------------------------\n"


