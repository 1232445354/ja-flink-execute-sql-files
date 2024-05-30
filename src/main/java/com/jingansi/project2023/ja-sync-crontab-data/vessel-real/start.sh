#!/bin/bash

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into sa.dwd_ais_vessel_all_info
select
  *
from doris_idc.sa.dwd_ais_vessel_all_info
where acquire_timestamp_format > DATE_SUB(now(),interval 30 minute);


insert into sa.dws_ais_vessel_status_info
select
  *
from doris_idc.sa.dws_ais_vessel_status_info
where acquire_timestamp_format > DATE_SUB(now(),interval 30 minute);
"


echo -en "开始同步数据...$(date)\n"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"

echo -en "数据同步SUCCESS.......$(date)\n"
echo -en "------------------------------------------\n"



