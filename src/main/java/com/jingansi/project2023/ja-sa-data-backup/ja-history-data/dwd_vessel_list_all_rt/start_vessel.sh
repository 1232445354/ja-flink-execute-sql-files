#!/bin/bash
#dwd_vessel_list_all_rt

start_day=2023-07-05
# end_day=2023-07-06
end_day=2023-10-24
date_format="+%Y-%m-%d"

echo -en "开始备份数据...$(date)"

DIR=$(cd `dirname $0`; pwd)

# 将日期转换为时间戳
start_timestamp=$(date -d "$start_day" +%s)
end_timestamp=$(date -d "$end_day" +%s)

# 遍历日期并输出
current_timestamp="$start_timestamp"

while [ "$current_timestamp" -le "$end_timestamp" ]; do
  current_day=$(date -d "@$current_timestamp" "$date_format")
  echo "$current_day"
  sh ${DIR}/sql_file_vessel.sh "$current_day"
  current_timestamp=$((current_timestamp + 86400))  # 增加一天的时间戳
  echo -en "sleep中..."
  sleep 20s
done

echo -en "备份数据SUCCESS.......$(date)"
echo -en "-----------------------"