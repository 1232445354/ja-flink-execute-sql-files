#!/bin/bash

table_name=${1}
time_column=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into doris_jh132.sa.${table_name}
select
  *
from doris_idc.sa.${table_name}
where ${time_column} > to_date(date_sub(now(),interval 1 day));

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"