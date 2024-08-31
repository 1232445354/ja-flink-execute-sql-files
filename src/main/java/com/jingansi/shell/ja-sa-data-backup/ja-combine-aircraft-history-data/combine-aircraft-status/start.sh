#!/bin/bash

start_day=2023-05-01
end_day=2023-10-30
date_format="+%Y-%m-%d"

echo -en "开始刷新数据...$(date)\n"

DIR=$(cd `dirname $0`; pwd)

# 将日期转换为时间戳
start_timestamp=$(date -d "$start_day" +%s)
end_timestamp=$(date -d "$end_day" +%s)

# 遍历日期并输出
current_timestamp1="$start_timestamp"
current_timestamp2=$((current_timestamp1 + 86400))

while [ "$current_timestamp1" -le "$end_timestamp" ]; do
  current_day1=$(date -d "@$current_timestamp1" "$date_format")
  current_day2=$(date -d "@$current_timestamp2" "$date_format")

  echo -en "$current_day1" + "$current_day2\n"

  sh ${DIR}/sql_file.sh "$current_day1" "$current_day2"

  current_timestamp1=$((current_timestamp1 + 86400))
  current_timestamp2=$((current_timestamp2 + 86400))
  echo -en "sleep中...\n"
  sleep 20s
done

echo -en "备份数据SUCCESS.......$(date)\n"
echo -en "-----------------------\n"