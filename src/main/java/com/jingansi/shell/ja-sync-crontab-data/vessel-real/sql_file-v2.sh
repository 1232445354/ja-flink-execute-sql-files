#!/bin/bash

table_name=${1}
time_column=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into sa.${table_name}
select
  *
from doris_idc.sa.${table_name}
where ${time_column} between date_sub(now(),interval 1 hour) and now();

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"