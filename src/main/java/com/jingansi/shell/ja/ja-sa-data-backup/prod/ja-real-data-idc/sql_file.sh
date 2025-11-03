#!/bin/bash

table_name=${1}
time_column=${2}
start_time=${3}
end_time=${4}
type=${5}
catalog_name=${6}
min_lon=${7}
max_lon=${8}

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh

# 正常的执行sql
sql="
insert into sa.${table_name}
select
  *
from ${catalog_name}.sa.${table_name}
where \`${time_column}\` between '${start_time}' and '${end_time}';
"

# 船舶、飞机天聚合
sql_merge="
insert into sa.${table_name}
select
  *
from ${catalog_name}.sa.${table_name}
where ${time_column} = '${start_time}'
  and longitude >= ${min_lon}
  and longitude < ${max_lon};
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

