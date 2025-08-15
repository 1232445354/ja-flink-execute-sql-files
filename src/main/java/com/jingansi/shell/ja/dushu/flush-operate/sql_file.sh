#!/bin/bash

start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql2="

update dushu.dwd_device_operate_report_info
set health_info = get_json_string(properties,'$.message')
where type = 'events'
  and acquire_timestamp_format >= '${start_time}'
  and acquire_timestamp_format < '${end_time}'
"

mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql2}"


