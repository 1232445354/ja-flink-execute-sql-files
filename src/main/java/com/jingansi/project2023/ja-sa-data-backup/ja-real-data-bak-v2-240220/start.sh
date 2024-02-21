#!/bin/bash

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

echo -en "开始备份数据...$(date)\n"


yesterday_start_day=$(date -d "yesterday" "+%Y-%m-%d")
yesterday_end_day=$(date -d "yesterday" "+%Y-%m-%d 23:00:00")
today_day=$(date "+%Y-%m-%d")

echo -en "${yesterday_start_day} + ${today_day}\n"

sh ${DIR}/sql_file1.sh "$yesterday_start_day" "$today_day"
sleep 10s
echo -en "其他表同步完成,开始同步飞机融合表按小时同步----$(date)\n"


date_format="+%Y-%m-%d %H:%M:%S"

# 将日期转换为时间戳
start_timestamp=$(date -d "$yesterday_start_day" +%s)
end_timestamp=$(date -d "$yesterday_end_day" +%s)

# 遍历日期并输出
current_timestamp1="${start_timestamp}"
current_timestamp2=$((current_timestamp1 + 3600))

while [ "$current_timestamp1" -le "$end_timestamp" ]; do
  current_day1=$(date -d "@$current_timestamp1" "$date_format")
  current_day2=$(date -d "@$current_timestamp2" "$date_format")

  echo -en "$current_day1" + "$current_day2\n"

	# 飞机融合表按小时同步
  sh ${DIR}/sql_file2.sh "$current_day1" "$current_day2"

  current_timestamp1=$((current_timestamp1 + 3600))
  current_timestamp2=$((current_timestamp2 + 3600))
  echo -en "sleep中...\n"
  sleep 20s
done

echo -en "备份数据SUCCESS.......$(date)\n"
echo -en "----------------------------------\n"
