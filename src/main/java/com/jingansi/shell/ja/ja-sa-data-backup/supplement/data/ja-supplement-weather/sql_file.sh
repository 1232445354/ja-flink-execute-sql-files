#!/bin/bash

start_time=${1}
end_time=${2}
table_name=${3}
time_column=${4}
catalog_info=${5}
min_lon=${6}
max_lon=${7}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh


sql="
insert into sa.${table_name}
select
  *
from ${catalog_info}.sa.${table_name}
where ${time_column} between '${start_time}' and '${end_time}'
  and lng_key >= ${min_lon}
  and lng_key < ${max_lon};
"


#echo "${sql}"
mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"