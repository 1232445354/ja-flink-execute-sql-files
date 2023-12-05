#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
echo ${DIR}

source ${DIR}/config.sh

sql="
  insert into sa.dwd_satellite_all_info
  select * from doris_202.sa.dwd_satellite_all_info
  where current_date = to_date(now());
"

echo -en "开始同步数据...$(date)\n"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"

echo -en "数据同步SUCCESS.......$(date)\n"
echo -en "------------------------------------------\n"


