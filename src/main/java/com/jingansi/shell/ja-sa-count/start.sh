#!/bin/bash

echo -en "程序开始执行.....$(date)\n"

startTime="2024-02-19 00:00:00"
endTime="2025-02-19 00:00:00"
interval_time=8640000

date_format="+%Y-%m-%d %H:%M:%S"
sleep_time=10

DIR=$(cd `dirname $0`; pwd)

echo -en "开始时间: ${startTime},结束时间: ${endTime}\n"
# 将日期转换为时间戳
start_timestamp=$(date -d "$startTime" +%s)
end_timestamp=$(date -d "$endTime" +%s)
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
