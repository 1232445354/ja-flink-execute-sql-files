#!/bin/bash

echo -en "idc飞机数据同步201...$(date +"%Y-%m-%d %H:%M:%S")\n"
DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sleep_time=2  # 每执行完一个表sleep时间
declare -a table_infos=(
"dws_bhv_satellite_list_fd today_time"    # 卫星行为轨道表
"dws_et_satellite_info acquire_time"      # 卫星实体表
"dws_atr_satellite_image_info acquire_time"  # 卫星图片表
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

