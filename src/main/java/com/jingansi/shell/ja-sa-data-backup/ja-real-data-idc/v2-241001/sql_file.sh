#!/bin/bash

table_name=${1}
time_column=${2}
start_time=${3}
end_time=${4}
src_code=${5}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

# 正常的执行sql
sql1="
insert into sa.${table_name}
select
  *
from doris_idc.sa.${table_name}
where \`${time_column}\` between '${start_time}' and '${end_time}';
"

# 船舶、飞机天聚合
sql2="
insert into sa.${table_name}
select
  *
from doris_idc.sa.${table_name}
where \`${time_column}\` = '${start_time}'
  and src_code = ${src_code};
"

if [ -z "$src_code" ]; then
  mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql1}"
else
  mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql2}"
fi

