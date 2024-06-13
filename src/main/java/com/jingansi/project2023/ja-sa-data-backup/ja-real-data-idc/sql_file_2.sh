#!/bin/bash

table_name=${1}
time_column=${2}
start_time=${3}
end_time=${4}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into sa.${table_name}
select
  *
from doris_idc.sa.${table_name}
where ${time_column} between '${start_time}' and '${end_time}';

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"