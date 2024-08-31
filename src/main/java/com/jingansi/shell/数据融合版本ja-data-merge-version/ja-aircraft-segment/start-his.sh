#!/bin/bash

start_day="2024-02-28 00:00:00"
end_day="2024-07-01 00:00:00"
date_format="+%Y-%m-%d %H:%M:%S"
interval=86400
sleep_time=1

echo -en "开始刷新数据...$(date)\n"

DIR=$(cd `dirname $0`; pwd)

# 将日期转换为时间戳
start_timestamp=$(date -d "$start_day" +%s)
end_timestamp=$(date -d "$end_day" +%s)

# 遍历日期并输出
current_timestamp1="$start_timestamp"
current_timestamp2=$((current_timestamp1 + ${interval}))


while [ "$current_timestamp1" -le "$end_timestamp" ]; do
  current_day1=$(date -d "@$current_timestamp1" "$date_format")
  current_day2=$(date -d "@$current_timestamp2" "$date_format")

  echo  -en "$current_day1" + "$current_day2\n"

  sh ${DIR}/sql_file_his.sh "$current_day1" "$current_day2"

  current_timestamp1=$((current_timestamp1 + ${interval}))
  current_timestamp2=$((current_timestamp2 + ${interval}))
  echo -en "sleep中,时间为：${sleep_time}s.......\n"
  sleep ${sleep_time}s
done

echo -en "====================================\n"
echo -en "刷新数据SUCCESS.......$(date)\n"
echo -en "====================================\n"
