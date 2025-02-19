#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
echo -en "统计实体点位数据...$(date "+%Y-%m-%d %H:%M:%S")\n"

sleep_time=3
interval_time=86400
start_time="2024-10-21 00:00:00"
end_time="2025-02-19 00:00:00"


start_timestamp=$(date -d "$start_time" +"%s")
end_timestamp=$(date -d "$end_time" +"%s")

current_timestamp=$start_timestamp

while [ $current_timestamp -lt $end_timestamp ];
do
  # 将当前时间戳转换为可读时间格式
  start_time=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")
  current_timestamp=$((current_timestamp + ${interval_time}))
  end_time=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")

  echo -en "${start_time} + ${end_time}\n"
  while true;
  do
    sh ${DIR}/sql_file.sh "$start_time" "$end_time"
    if [ $? -eq 0 ]; then
      echo -en "执行成功\n"
      break
    else
      echo -en "执行失败，重试中...\n"
      sleep 1s
    fi
  done
  echo -en "sleep中,时间为${sleep_time}s..."
  sleep ${sleep_time}s
done


echo -en "+++++++++++++++++++++++++++++++++++++\n"
echo -en "执行SUCCESS--------$(date "+%Y-%m-%d %H:%M:%S")\n"
echo -en "+++++++++++++++++++++++++++++++++++++\n"


