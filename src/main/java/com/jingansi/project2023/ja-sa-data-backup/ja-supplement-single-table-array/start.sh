#!/bin/bash

# 定义飞机时间范围数组
time_ranges=(
  "2024-03-15 00:00:00 2024-03-15 11:59:59"
  "2024-03-18 03:00:00 2024-03-18 03:59:59"
  "2024-03-19 00:00:00 2024-03-19 00:59:59"
  "2024-03-19 21:00:00 2024-03-21 22:59:59"
  "2024-03-24 00:00:00 2024-03-24 00:59:59"
  "2024-03-26 08:00:00 2024-03-26 08:59:59"
  "2024-03-27 01:00:00 2024-03-27 01:59:59"
  "2024-03-28 05:00:00 2024-03-28 05:59:59"
)

DIR=$(cd `dirname $0`; pwd)
date_format="+%Y-%m-%d %H:%M:%S"
interval_time=3600
sleep_time1=15

# 遍历数组并输出每个时间范围的开始和结束时间
for range in "${time_ranges[@]}"; do
	start_time="$(echo $range | cut -d' ' -f1) $(echo $range | cut -d' ' -f2)"
	end_time="$(echo $range | cut -d' ' -f3) $(echo $range | cut -d' ' -f4)"
	echo -en "飞机实体开始时间: $start_time 结束时间: $end_time\n"
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
  echo -en "sleep中 时间: ${sleep_time1}s...\n"
  sleep ${sleep_time1}s
  done
  echo -en "--------------\n"

done

echo -en "++++++++++++++++++++++++++++++++++++++++++\n"
echo -en "飞机实体同步完成...."
echo -en "++++++++++++++++++++++++++++++++++++++++++\n"

