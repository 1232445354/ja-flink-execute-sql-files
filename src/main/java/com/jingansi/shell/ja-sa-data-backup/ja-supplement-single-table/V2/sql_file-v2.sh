#!/bin/bash

start_day=${1}
end_day=${2}
table_name=${3}
time_column=${4}
catalog_info=${5}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.${table_name}
select
        *
from ${catalog_info}.sa.${table_name}
where ${time_column} between '${start_day}' and '${end_day}'

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"