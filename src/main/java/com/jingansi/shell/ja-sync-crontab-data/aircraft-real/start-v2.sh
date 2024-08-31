#!/bin/bash

echo -en "idc飞机数据同步201...$(date +"%Y-%m-%d %H:%M:%S")\n"
DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sleep_time=1  # 每执行完一个表sleep时间
declare -a table_infos=(
"dwd_bhv_aircraft_combine_rt acquire_time"    # 飞机轨迹
"dws_bhv_aircraft_last_location_rt acquire_time" # 飞机轨迹
"dws_flight_segment_rt start_time"  # 飞机航班

#"dws_aircraft_combine_list_rt acquire_time"  # 飞机轨迹
#"dws_aircraft_combine_status_rt acquire_time" # 飞机状态
#"dws_flight_segment_rt start_time"  # 飞机航班
)

for table_info in "${table_infos[@]}"; do
  IFS=' ' read -r table_name time_column <<< "$table_info"
  echo -en "-----------${table_name}-----------\n"
  while true; do
    sh ${DIR}/sql_file.sh "$table_name" "$time_column"
    if [ $? -eq 0 ]; then
      echo -en "执行成功\n"
      break
    else
      echo -en "执行失败，重试中...\n"
      sleep 1s
    fi
  done
  echo "sleep中,时间：${sleep_time}s..."
  sleep ${sleep_time}s
done

echo -en "+++++++++++++++++++++++++++++++++++++++++++++++\n"
echo -en "执行SUCCESS------$(date +"%Y-%m-%d %H:%M:%S")\n"
echo -en "+++++++++++++++++++++++++++++++++++++++++++++++\n"

