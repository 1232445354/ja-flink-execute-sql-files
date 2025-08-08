#!/bin/bash

start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql2="

update dushu.dwd_device_attr_info
set longitude = get_json_string(properties,'$.longitude'),
 latitude = get_json_string(properties,'$.latitude'),
 attitude_head = get_json_string(properties,'$.attitude_head'),
 gimbal_head = get_json_string(properties,'$.gimbal_head'),
 altitude = get_json_string(properties,'$.altitude'),
 height = get_json_string(properties,'$.height')

where acquire_timestamp_format >= '${start_time}'
  and acquire_timestamp_format < '${end_time}'
"

mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql2}"


