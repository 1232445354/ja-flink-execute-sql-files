#!/bin/bash

table_name=${1}
time_column=${2}
start_time=${3}
end_time=${4}
type=${5}
min_lon=${6}
max_lon=${7}

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh

# 正常的执行sql
sql="
insert into sa.${table_name}
select
  *
from doris_idc_track.sa.${table_name}
where \`${time_column}\` between '${start_time}' and '${end_time}';
"

sql_merge="
insert into sa.${table_name}
select
  *
from doris_idc_track.sa.${table_name}
where ${time_column} between '${start_time}' and '${end_time}'
  and lng_key >= ${min_lon}
  and lng_key < ${max_lon};
"

if [ "$type" = "common" ]; then
  mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql}"
elif [ "$type" = "merge" ]; then
  mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql_merge}"
fi

