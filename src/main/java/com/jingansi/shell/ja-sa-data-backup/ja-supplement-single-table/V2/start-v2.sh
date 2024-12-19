#!/bin/bash

echo -en "程序开始执行.....$(date)\n"

declare -a time_ranges=(
"2024-10-01 00:00:00 2024-11-10 23:59:59"
)

#col="dws_bhv_airport_weather_info acquire_time 864000"
#col="dws_bhv_aircraft_last_location_dh merge_time 43200"
#col="dws_bhv_vessel_last_location_dh merge_time 28800"
#col="dws_vessel_bhv_track_rt acquire_time 3600"
#col="dwd_vessel_list_all_rt acquire_timestamp_format 7200"
#col="dwd_adsbexchange_aircraft_list_rt acquire_timestamp_format 3600"
#col="dwd_vt_vessel_all_info acquire_timestamp_format 7200"
#col="dwd_ais_landbased_vessel_list acquire_time 14400"
#col="dwd_fr24_aircraft_list_rt acquire_time 7200"
#col="dwd_bhv_aircraft_combine_rt acquire_time 1800"
#col="dwd_ais_vessel_all_info acquire_timestamp_format 3600"
#col="dws_aircraft_combine_list_rt acquire_time 1800"
#col="dwd_ais_vessel_port_all_info acquire_timestamp_format 172800"
#col="dws_ais_vessel_all_info_day acquire_timestamp_format 432000"
#col="dwd_ais_vessel_all_info acquire_timestamp_format 7200"
#col="dws_aircraft_combine_list_rt acquire_time 1800"
#col="dws_flight_segment_rt start_time 86400"
#col="dws_bhv_airport_weather_info acquire_time 432000"
#col="dwd_bhv_sentinel_info photograph_datetime 864000"
#col="dws_bhv_satellite_list_fd today_time 864000"
#col="ods_airport_flight_arrival acquire_time 432000"
col="ods_airport_flight_departure acquire_time 432000"

date_format="+%Y-%m-%d %H:%M:%S"
sleep_time=40
catalog_info="doris_idc"

IFS=' ' read -r table_name time_column interval_time <<< "$col"

DIR=$(cd `dirname $0`; pwd)

for time_range in "${time_ranges[@]}"; do
  IFS=' ' read -ra time_parts <<< "$time_range"
  # 输出开始时间和结束时间
  start_day="${time_parts[0]} ${time_parts[1]}"
  end_day="${time_parts[2]} ${time_parts[3]}"
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

echo -en "-------------------------------------------\n"
echo -en "备份数据SUCCESS.......$(date)\n"
echo -en "-------------------------------------------\n"
