#!/bin/bash

# 定义飞机时间范围数组
time_ranges=(
    "2023-06-12 00:00:00 2023-06-13 00:00:00"
    "2023-06-07 00:00:00 2023-06-08 01:00:00"
    "2023-06-30 00:00:00 2023-07-03 00:00:00"
    "2023-06-14 00:00:00 2023-06-15 00:00:00"
    "2023-08-22 23:00:00 2023-08-23 23:59:59"
    "2023-09-06 00:00:00 2023-09-07 00:00:00"
    "2023-10-12 11:00:00 2023-10-12 12:00:00"
    "2023-10-16 13:00:00 2023-10-18 23:59:59"
    "2023-10-28 23:00:00 2023-10-30 23:59:59"
    "2024-01-02 00:00:00 2024-01-03 00:00:00"
    "2024-02-15 00:00:00 2024-02-15 23:59:59"
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

    sh ${DIR}/sql_file_aircraft.sh "$current_day1" "$current_day2"

    current_timestamp1=$((current_timestamp1 + $interval_time))
    current_timestamp2=$((current_timestamp2 + $interval_time))
    echo -en "sleep中.......\n"
    sleep ${sleep_time1}s
  done
  echo -en "--------------\n"

done

echo -en "++++++++++++++++++++++++++++++++++++++++++\n"
echo -en "飞机实体同步完成...."
echo -en "++++++++++++++++++++++++++++++++++++++++++\n"



# 定义船舶时间范围数组
time_ranges1=(
    "2023-06-15 00:00:00 2023-06-17 23:59:59"
    "2023-06-25 22:00:00 2023-06-26 14:00:00"
    "2023-07-27 00:00:00 2023-07-27 23:59:59"
    "2023-06-21 08:00:00 2023-06-22 04:00:00"
    "2023-09-06 19:00:00 2023-09-09 19:59:59"
    "2023-10-18 08:00:00 2023-10-19 08:59:59"
    "2023-10-22 00:00:00 2023-10-23 00:00:00"
    "2023-11-09 00:00:00 2023-11-11 23:54:52"
    "2024-01-16 06:00:00 2024-01-17 06:59:59"
    "2024-01-16 06:00:00 2024-01-17 06:59:59"
)


interval_time2=7200
sleep_time2=10

# 遍历数组并输出每个时间范围的开始和结束时间
for range in "${time_ranges1[@]}"; do
	start_time="$(echo $range | cut -d' ' -f1) $(echo $range | cut -d' ' -f2)"
	end_time="$(echo $range | cut -d' ' -f3) $(echo $range | cut -d' ' -f4)"
	echo -en "船舶实体开始时间: $start_time 结束时间: $end_time\n"
  start_timestamp=$(date -d "$start_time" +%s)
  end_timestamp=$(date -d "$end_time" +%s)
  current_timestamp1="$start_timestamp"
  current_timestamp2=$((current_timestamp1 + $interval_time2))
  while [ "$current_timestamp1" -le "$end_timestamp" ]; do
    current_day1=$(date -d "@$current_timestamp1" "$date_format")
    current_day2=$(date -d "@$current_timestamp2" "$date_format")

    echo  -en "$current_day1" + "$current_day2\n"

    sh ${DIR}/sql_file_vessel.sh "$current_day1" "$current_day2"

    current_timestamp1=$((current_timestamp1 + $interval_time2))
    current_timestamp2=$((current_timestamp2 + $interval_time2))
    echo -en "sleep中.......\n"
    sleep ${sleep_time2}s
  done
  echo -en "--------------"

done

echo -en "++++++++++++++++++++++++++++++++++++++++++\n"
echo -en "船舶实体同步完成...."
echo -en "++++++++++++++++++++++++++++++++++++++++++\n"
