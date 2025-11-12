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

sql2="
insert into sa.${table_name}
select
  *
from doris_idc.sa.${table_name}
where merge_time = '${start_time}'
  and longitude >= ${min_lon}
  and longitude < ${max_lon};
"

mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql2}"


