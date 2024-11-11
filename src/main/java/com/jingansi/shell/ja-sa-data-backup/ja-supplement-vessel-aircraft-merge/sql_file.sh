#!/bin/bash

table_name=${1}
start_time=${2}
src_code=${3}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql2="
insert into sa.${table_name}
select
  *
from doris_idc.sa.${table_name}
where merge_time = '${start_time}'
  and src_code = ${src_code};
"

mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql2}"


