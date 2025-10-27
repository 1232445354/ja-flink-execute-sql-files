#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
source ${DIR}/table_config.sh
echo -e "开始备份idc-态势数据...$(date "+%Y-%m-%d %H:%M:%S")"

# 定义一个执行 SQL 脚本的函数
execute_with_retry() {
  local table_name=$1
  local time_column=$2
  local pre_start_time=$3
  local next_end_time=$4
  local type=$5
  local min_lon=$6
  local max_lon=$7

  while true; do
    sh "${DIR}/sql_file.sh" "$table_name" "$time_column" "$pre_start_time" "$next_end_time" "$type" "$min_lon" "$max_lon"
    if [ $? -eq 0 ]; then
      echo -e "执行成功\n"
      break
    else
      echo -e "执行失败，重试中...\n"
      sleep 5s
    fi
  done
}

# 目前：2025-10-27 15:36:00  startTime: 2025-10-27 14:00:00  endTime: 2025-10-27 15:00:00
start_time=$(date -d '-1 hour' '+%Y-%m-%d %H:00:00')
end_time=$(date '+%Y-%m-%d %H:00:00')
echo "start_time = [${start_time}],end_time = [${end_time}]"

echo -e "----------------同步轨迹表数据-----------------------\n"
# 将时间格式转换为 Unix 时间戳
start_timestamp=$(date -d "$start_time" +"%s")
end_timestamp=$(date -d "$end_time" +"%s")
for item in "${arrayList[@]}"
do
    current_timestamp=$start_timestamp
    # 切分元素为多个值
    table_name=$(echo $item | awk '{print $1}')
    time_column=$(echo $item | awk '{print $2}')
    sleep_hour_time=$(echo $item | awk '{print $3}')
    interval_time=$(echo $item | awk '{print $4}')

    echo -e ".................${table_name}................."
    echo -e "该表每执行完一次sleep时间:${sleep_hour_time}s...\n"
    while [ $current_timestamp -lt $end_timestamp ];
    do
      # 将当前时间戳转换为可读时间格式
      pre_start_time=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")
      current_timestamp=$((current_timestamp + ${interval_time}))
      next_end_time=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")

      echo -e "${pre_start_time} + ${next_end_time}"
      execute_with_retry "$table_name" "$time_column" "$pre_start_time" "$next_end_time" "common"
      sleep ${sleep_hour_time}s
    done
done


echo -e "----------------sync气象数据-----------------------\n"
minLong=-18000
maxLong=18000
step=500
for item in "${weather_list[@]}"
do
  table_name=$(echo $item | awk '{print $1}')
  time_column=$(echo $item | awk '{print $2}')
  echo -e ".................${table_name}................."
  for ((current = minLong; current <= maxLong - step; current += step)); do
      current_min=$current
      current_max=$((current + step))
      echo -e "${pre_start_time} + ${next_end_time} 范围: [$current_min, $current_max]"
      execute_with_retry "$table_name" "$time_column" "$start_time" "$end_time" "merge" "$current_min" "$current_max"
      sleep 2s
  done
done


echo -e "+++++++++++++++++++++++++++++++++++++++++++++++"
echo -e "执行SUCCESS--------$(date "+%Y-%m-%d %H:%M:%S")"
echo -e "+++++++++++++++++++++++++++++++++++++++++++++++"

