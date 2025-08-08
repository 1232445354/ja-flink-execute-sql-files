#!/bin/bash

echo -e "程序开始执行.....$(date)\n"

start_day="2025-08-01 00:00:00"
end_day="2025-08-05 23:59:59"

sleep_time=1
interval_time=86400
date_format="+%Y-%m-%d %H:%M:%S"

DIR=$(cd `dirname $0`; pwd)

# 输出开始时间和结束时间
echo -en "开始时间: ${start_day},结束时间: ${end_day}\n"
# 将日期转换为时间戳
start_timestamp=$(date -d "$start_day" +%s)
end_timestamp=$(date -d "$end_day" +%s)
# 遍历日期并输出
current_timestamp1="$start_timestamp"
current_timestamp2=$((current_timestamp1 + $interval_time))

while [ "$current_timestamp1" -le "$end_timestamp" ]; do
  current_day1=$(date -d "@$current_timestamp1" "$date_format")
  current_day2=$(date -d "@$current_timestamp2" "$date_format")

  echo  -en "$current_day1" + "$current_day2\n"
  while true; do
    sh ${DIR}/sql_file.sh "$current_day1" "$current_day2"
    if [ $? -eq 0 ]; then
      echo -en "执行成功\n"
      break
    else
      echo -en "执行失败，重试中...\n"
      sleep 1s
    fi
  done
  current_timestamp1=$((current_timestamp1 + $interval_time))
  current_timestamp2=$((current_timestamp2 + $interval_time))
  echo -en "sleep中 时间: ${sleep_time}s...\n"
  sleep ${sleep_time}s
done


echo -en "-------------------------------------------\n"
echo -en "备份数据SUCCESS.......$(date)\n"
echo -en "-------------------------------------------\n"
