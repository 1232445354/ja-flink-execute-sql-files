#!/bin/bash

table_name=${1}
time_column=${2}
pre_day_cnt=${3}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into sa.${table_name}
select
  *
from doris_idc.sa.${table_name}
where ${time_column} > to_date(date_sub(now(),interval ${pre_day_cnt} day));

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"