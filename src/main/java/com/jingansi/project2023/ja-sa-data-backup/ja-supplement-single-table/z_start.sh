#!/bin/bash

echo -en "程序开始执行.....$(date)\n"
if [ $# -eq 0 ]; then
    # 没有参数
    start_day="2024-06-20 00:00:00"
    end_day="2024-07-01 00:00:00"
    table_name="dws_aircraft_combine_list_rt"
    time_column="acquire_time"
    echo -en "--------------------------------\n"
    echo -en "没有输入参数...\n"
    echo -en "start_time -> ${start_day}\n"
    echo -en "end_time -> ${end_day}\n"
    echo -en "table_name -> ${table_name}\n"
    echo -en "time_column -> ${time_column}\n"
    echo -en "--------------------------------\n"

else
    # 有参数
    start_day=$1
    if [ $# -ge 4 ]; then
        end_day=$2
        table_name=$3
        time_column=$4
        echo -en "--------------------------------\n"
        echo -en "输入参数...\n"
        echo -en "start_time -> ${start_day}\n"
        echo -en "end_time -> ${end_day}\n"
        echo -en "table_name -> ${table_name}\n"
        echo -en "time_column -> ${time_column}\n"
        echo -en "--------------------------------\n"
    fi
fi


date_format="+%Y-%m-%d %H:%M:%S"
interval_time=3600
sleep_time=30
catalog_info="doris_idc"


DIR=$(cd `dirname $0`; pwd)

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

  sh ${DIR}/sql_file.sh "$current_day1" "$current_day2" "$table_name" "$time_column" "$catalog_info"

  current_timestamp1=$((current_timestamp1 + $interval_time))
  current_timestamp2=$((current_timestamp2 + $interval_time))
  echo -en "sleep中 时间: ${sleep_time}s...\n"
  sleep ${sleep_time}s
done

echo -en "-------------------------------------------\n"
echo -en "备份数据SUCCESS.......$(date)\n"
echo -en "-------------------------------------------\n"

