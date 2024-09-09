#!/bin/bash

DIR=$(cd `dirname $0`; pwd)

echo -en "开始备份idc-态势数据...$(date "+%Y-%m-%d %H:%M:%S")\n"


# 检查是否提供了参数
if [ $# -eq 0 ]; then
    # 没有参数
    start_time=$(date -d "yesterday" "+%Y-%m-%d")
    end_time=$(date "+%Y-%m-%d")
    echo "没有参数,则start_time = [${start_time}],end_time = [${end_time}]" # 昨天0点、今天0点
else
    # 有参数
    start_time=$1
    if [ $# -ge 2 ]; then
        end_time=$2
        echo "传入参数,则start_time = [${start_time}],end_time = [${end_time}]"
    fi
fi


# 定义一个执行 SQL 脚本的函数
execute_with_retry() {
  local table_name=$1
  local time_column=$2
  local pre_start_time=$3
  local next_end_time=$4
  local src_code=$5

  while true; do
    echo -en "$table_name $time_column $pre_start_time $next_end_time\n"
    if [ $? -eq 0 ]; then
      echo -en "执行成功\n"
      break
    else
      echo -en "执行失败，重试中...\n"
      sleep 1s
    fi
  done
}


echo -en "开始同步船舶、飞机按天聚合数据......\n"
declare -a table_infos=(
"dws_bhv_aircraft_last_location_fd acquire_time 1-2-3"
"dws_bhv_vessel_last_location_fd acquire_time 1-2-3-4"
)

merge_start_time=$(date -d "${start_time}" "+%Y-%m-%d 00:00:00")
merge_end_time=$(date -d "${end_time}" "+%Y-%m-%d 00:00:00")

for table_info in "${table_infos[@]}"; do
  IFS=' ' read -r table_name time_column src_code <<< "$table_info" && IFS='-' read -ra src_codes <<< "$src_code"
  echo -en ".................${table_name}.................\n"
  for src_code in "${src_codes[@]}"; do
    echo -en "$merge_start_time + ${merge_end_time} + src_code:{$src_code}\n"
    execute_with_retry "$table_name" "$time_column" "$merge_start_time" "$merge_end_time" "$src_code"
  done

done
