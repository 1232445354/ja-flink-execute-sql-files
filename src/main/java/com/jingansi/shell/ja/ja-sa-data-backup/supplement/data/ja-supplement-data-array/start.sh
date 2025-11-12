#!/bin/bash

# 定义时间范围数组
time_ranges=(
  "2024-06-01 00:00:00 2024-06-02 23:59:59"
)

DIR=$(cd `dirname $0`; pwd)
date_format="+%Y-%m-%d %H:%M:%S"
interval_time=3600
sleep_time=20

# 遍历数组并输出每个时间范围的开始和结束时间
for range in "${time_ranges[@]}"; do
	start_time="$(echo $range | cut -d' ' -f1) $(echo $range | cut -d' ' -f2)"
	end_time="$(echo $range | cut -d' ' -f3) $(echo $range | cut -d' ' -f4)"
	echo -en "数组：开始时间: $start_time 结束时间: $end_time\n"
  start_timestamp=$(date -d "$start_time" +%s)
  end_timestamp=$(date -d "$end_time" +%s)
  current_timestamp1="$start_timestamp"
  current_timestamp2=$((current_timestamp1 + $interval_time))
  while [ "$current_timestamp1" -le "$end_timestamp" ]; do
    current_day1=$(date -d "@$current_timestamp1" "$date_format")
    current_day2=$(date -d "@$current_timestamp2" "$date_format")

    echo  -en "$current_day1" + "$current_day2\n"

    sh ${DIR}/sql_file.sh "$current_day1" "$current_day2"

    current_timestamp1=$((current_timestamp1 + $interval_time))
    current_timestamp2=$((current_timestamp2 + $interval_time))
  echo -en "sleep中 时间: ${sleep_time}s...\n"
  sleep ${sleep_time}s
  done
  echo -en "--------------\n"

done

echo -en "++++++++++++++++++++++++++++++++++++++++++\n"
echo -en "补充数据完成....\n"
echo -en "++++++++++++++++++++++++++++++++++++++++++\n"

