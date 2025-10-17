#!/bin/bash

table_name=${1}
time_column=${2}
start_time=${3}
end_time=${4}
min_lon=${5}
max_lon=${6}

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh

# 正常的执行sql
sql="
insert into sa.${table_name}
select
  *
from doris_idc.sa.${table_name}
where \`${time_column}\` between '${start_time}' and '${end_time}';
"

# 船舶、飞机天聚合
sql_merge="
insert into sa.${table_name}
select
  *
from doris_idc.sa.${table_name}
where \`${time_column}\` = '${start_time}'
  and longitude >= ${min_lon}
  and longitude < ${max_lon};
"

if [ -z "$min_lon" ]; then
  mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql}"
else
  mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql_merge}"
fi

