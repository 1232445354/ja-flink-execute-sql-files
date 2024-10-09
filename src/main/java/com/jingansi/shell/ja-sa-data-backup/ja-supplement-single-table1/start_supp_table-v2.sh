#!/bin/bash

# 多个时间范围

echo -en "程序开始执行.....$(date)\n"

DIR=$(cd `dirname $0`; pwd)

declare -a time_ranges=(
"2024-09-06 00:00:00 2024-09-08 23:59:59"
"2024-09-13 00:00:00 2024-09-30 23:59:59"
)

col="dws_bhv_aircraft_last_location_fd 1-2-3"
#col="dws_bhv_vessel_last_location_fd 1-2-3-4"

date_format="+%Y-%m-%d %H:%M:%S"
sleep_time=40
IFS=' ' read -r table_name src_code <<< "$col" && IFS='-' read -ra src_codes <<< "$src_code"
echo -en "表名------${table_name}...\n"


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
        for src_code in "${src_codes[@]}"; do
          echo  -en "$start_time + ${src_code}\n"
          while true; do
            sh ${DIR}/sql_file.sh "$table_name" "$start_time" "$src_code"
            if [ $? -eq 0 ]; then
              echo -en "执行成功\n"
              break
            else
              echo -en "执行失败，重试中...\n"
              sleep 1s
            fi
          done
          sleep ${sleep_time}s
        done

        start_timestamp=$((start_timestamp + 86400))
        sleep ${sleep_time}s
    done

done



echo -en "+++++++++++++++++++++++++++++++++++++++++++++++\n"
echo -en "执行SUCCESS--------$(date "+%Y-%m-%d %H:%M:%S")\n"
echo -en "+++++++++++++++++++++++++++++++++++++++++++++++\n"

