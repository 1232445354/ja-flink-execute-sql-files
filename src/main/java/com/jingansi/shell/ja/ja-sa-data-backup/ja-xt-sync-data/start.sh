#!/bin/bash
DIR=$(cd `dirname $0`; pwd)
echo -e "程序开始执行.....$(date)\n"

time="2024-10-01 00:00:00 2024-11-10 23:59:59"

# table_name、time_column、durtion_time
declare -a small_table_infos=(
  "dws_bhv_aircraft_last_location_dh merge_time 14400"               # 4hour
  "dws_bhv_vessel_last_location_dh merge_time 14400"                 # 4hour
  "dws_vessel_bhv_track_rt acquire_time 3600"                        # 1hour
  "dwd_bhv_aircraft_combine_rt acquire_time 1800"                    # 0.5hour
  "dws_flight_segment_rt start_time 86400"                           # 24 hour
  "dws_bhv_satellite_list_fd today_time 864000"                      # 10 day
)

date_format="+%Y-%m-%d %H:%M:%S"
sleep_time=5
catalog_info="doris_idc"

IFS=' ' read -ra time_parts <<< "$time_range"
# 输出开始时间和结束时间
split_start_day="${time_parts[0]} ${time_parts[1]}"
split_end_day="${time_parts[2]} ${time_parts[3]}"
echo -en "time ${time}\n"


for table_info in "${small_table_infos[@]}"; do
  IFS=' ' read -r table_name time_column interval_time<<< "$table_info"
  echo -e "------${table_name}------\n"
  start_day=${split_start_day}
  end_day=${split_end_day}
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
        sh ${DIR}/sql_file.sh "$current_day1" "$current_day2" "$table_name" "$time_column" "$catalog_info"
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
done





declare -a table_infos=(
"dws_bhv_aircraft_last_location_fd merge_time 1-2-3"
"dws_bhv_vessel_last_location_fd merge_time 1-2-3-4"
)


for table_info in "${table_infos[@]}"; do
  IFS=' ' read -r table_name time_column src_code <<< "$table_info" && IFS='-' read -ra src_codes <<< "$src_code"
  echo -e "------${table_name}------\n"
  start_day=${split_start_day}
  end_day=${split_end_day}
  echo -en "开始时间: ${start_day},结束时间: ${end_day}\n"

  # 将日期转换为时间戳
  start_timestamp=$(date -d "$start_day" +%s)
  end_timestamp=$(date -d "$end_day" +%s)

   while [ "$start_timestamp" -le "$end_timestamp" ]; do
      start_time=$(date -d "@$start_timestamp" "$date_format")
      for src_code in "${src_codes[@]}"; do
        echo  -en "$start_time + ${src_code}\n"
        while true; do
          sh ${DIR}/sql_file.sh "$start_time" "$start_time" "$table_name" "$time_column" "$catalog_info" "$src_code"
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

echo -en "-------------------------------------------\n"
echo -en "备份数据SUCCESS.......$(date)\n"
echo -en "-------------------------------------------\n"
