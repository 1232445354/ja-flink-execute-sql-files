#!/bin/bash

yesterday_start_day="2024-04-02"
yesterday_end_day="2024-04-02 23:00:00"
today_day=$(date "+%Y-%m-%d")

# 公共使用
date_format="+%Y-%m-%d %H:%M:%S"
# 将日期转换为时间戳,秒
start_timestamp=$(date -d "$yesterday_start_day" +%s)
end_timestamp=$(date -d "$yesterday_end_day" +%s)


# 遍历日期并输出
current_timestamp3="${start_timestamp}"
current_timestamp4=$((current_timestamp3 + 3600))

while [ "$current_timestamp3" -le "$end_timestamp" ]; do
  current_day3=$(date -d "@$current_timestamp3" "$date_format")
  current_day4=$(date -d "@$current_timestamp4" "$date_format")

  echo -en "${current_day3} + ${current_day4}\n"
#  sh /data1/bigdata/apps/ja-sa-data-backup/ja-real-data/sql_file5.sh "$current_day3" "$current_day4"

  current_timestamp3=$((current_timestamp3 + 3600))
  current_timestamp4=$((current_timestamp4 + 3600))
  echo -en "sleep中...\n"
  sleep 15s
done

