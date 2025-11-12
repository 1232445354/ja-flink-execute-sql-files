#!/bin/bash

# 多个时间范围

echo -en "程序开始执行.....$(date)\n"

DIR=$(cd `dirname $0`; pwd)

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
    sh "${DIR}/sql_file.sh" "$table_name" "$time_column" "$pre_start_time" "$next_end_time" "merge" "$min_lon" "$max_lon"
    if [ $? -eq 0 ]; then
      echo -e "执行成功\n"
      break
    else
      echo -e "执行失败，重试中...\n"
      sleep 5s
    fi
  done
}




declare -a time_ranges=(
"2025-03-01 00:00:00 2025-10-28 23:59:59"
)

col="dws_bhv_aircraft_last_location_fd merge_time"
#col="dws_bhv_vessel_last_location_fd merge_time"


aircraft_longitude_ranges=(
    "-180 -105"
    "-105 -85"
    "-85 -70"
    "-70 180"
)

vessel_longitude_ranges=(
      "-180 20"
      "20 110"
      "110 120"
      "120 180"
)

date_format="+%Y-%m-%d %H:%M:%S"
sleep_time=5
IFS=' ' read -r table_name time_column <<< "$col"
echo -e "------${table_name}------\n"

for time_range in "${time_ranges[@]}"; do
  IFS=' ' read -ra time_parts <<< "$time_range"
    # 输出开始时间和结束时间
    start_day="${time_parts[0]} ${time_parts[1]}"
    end_day="${time_parts[2]} ${time_parts[3]}"
    echo -en "时间范围---开始时间: ${start_day},结束时间: ${end_day}\n"
    # 将日期转换为时间戳
    start_timestamp=$(date -d "$start_day" +%s)
    end_timestamp=$(date -d "$end_day" +%s)
    while [ "$start_timestamp" -le "$end_timestamp" ]; do
      start_time=$(date -d "@$start_timestamp" "$date_format")
      if [ "$table_name" = "dws_bhv_aircraft_last_location_fd" ]; then
          for range in "${aircraft_longitude_ranges[@]}"; do
            min_lon=$(echo $range | cut -d' ' -f1)
            max_lon=$(echo $range | cut -d' ' -f2)
            echo -e "$start_time + min_lon:max_lon->{$min_lon,$max_lon}"
            execute_with_retry "$table_name" "$time_column" "$start_time" "$start_time" "merge" "$min_lon" "$max_lon"
            sleep ${sleep_time}s
          done
        else
          for range in "${vessel_longitude_ranges[@]}"; do
            min_lon=$(echo $range | cut -d' ' -f1)
            max_lon=$(echo $range | cut -d' ' -f2)
            echo -e "$start_time + min_lon:max_lon->{$min_lon,$max_lon}"
            execute_with_retry "$table_name" "$time_column" "$start_time" "$start_time" "merge" "$min_lon" "$max_lon"
            sleep ${sleep_time}s
          done
        fi
        start_timestamp=$((start_timestamp + 86400))
        sleep ${sleep_time}s
    done

done



echo  "+++++++++++++++++++++++++++++++++++++++++++++++"
echo  "执行SUCCESS--------$(date "+%Y-%m-%d %H:%M:%S")"
echo -e "+++++++++++++++++++++++++++++++++++++++++++++++\n"

