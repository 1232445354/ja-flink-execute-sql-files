#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
source ${DIR}/table_config.sh
echo -e "开始备份idc-态势数据...$(date "+%Y-%m-%d %H:%M:%S")"

catalog_name="doris_idc"
echo -e "开始重新创建catalog:${catalog_name}..\n"
#sh "${DIR}/catalog.sh" "$catalog_name"


# 定义一个执行 SQL 脚本的函数
execute_with_retry() {
  local table_name=$1
  local time_column=$2
  local pre_start_time=$3
  local next_end_time=$4
  local type=$5
  local min_lon=$6
  local max_lon=$7

  local max_retries=10
  local retry_count=0

  while true; do
    local output
    output=$(sh "${DIR}/sql_file.sh" "$table_name" "$time_column" "$pre_start_time" "$next_end_time" "$type" "$catalog_name" "$min_lon" "$max_lon" 2>&1)
    local exit_code=$?
    if [ $exit_code -eq 0 ] && ! echo "$output" | grep -q "Query timeout"; then
      echo -e "执行成功\n"
      break
    else
      retry_count=$((retry_count + 1))
        echo -e "执行失败（尝试 $retry_count/$max_retries），重试中...\n"
        echo "错误信息: $output"
        if [ $retry_count -ge $max_retries ]; then
          echo -e "达到最大重试次数，退出\n"
          return 1
        fi
      sleep 5s
    fi
  done
}

start_time=$(date -d "yesterday" "+%Y-%m-%d")
end_time=$(date "+%Y-%m-%d")
echo "start_time = [${start_time}],end_time = [${end_time}]" # 昨天0点、今天0点

per_table_sleep_time=5  # 每执行完一个表sleep时间
echo -en "小表数据同步中......\n"
for table_info in "${small_table_infos[@]}"; do
  IFS=' ' read -r table_name time_column front_day after_day<<< "$table_info"
  pre_start_time=$(date -d "${start_time} -${front_day} day" "+%Y-%m-%d 00:00:00")
  next_end_time=$(date -d "${end_time} +${after_day} day" "+%Y-%m-%d 00:00:00")

  echo -e "------${table_name}:::{$pre_start_time---$next_end_time}------"
  execute_with_retry "$table_name" "$time_column" "$pre_start_time" "$next_end_time" "common"
  sleep ${per_table_sleep_time}s
done
echo -e "小表同步完成"
echo -e "---------------------------------------\n"


echo -e "同步船舶、飞机按天聚合数据......\n"
minLong=-180
maxLong=180
step=5

merge_start_time=$(date -d "${start_time}" "+%Y-%m-%d 00:00:00")
merge_end_time=$(date -d "${end_time}" "+%Y-%m-%d 00:00:00")

for table_info in "${table_infos[@]}"; do
  IFS=' ' read -r table_name time_column <<< "$table_info"
  echo -e ".................${table_name}.................\n"

  for ((current = minLong; current <= maxLong - step; current += step)); do
      current_min=$current
      current_max=$((current + step))
      echo -e "${start_time} + ${end_time} 范围: [$current_min, $current_max]"
      execute_with_retry "$table_name" "$time_column" "$merge_start_time" "$merge_end_time" "merge" "$current_min" "$current_max"
      sleep 2s
  done

done
echo -e "聚合表同步完成"
echo -e "---------------------------------------\n"



echo -e "同步轨迹表数据......"
echo -e "---------------------------------------\n"
# 将时间格式转换为 Unix 时间戳
start_timestamp=$(date -d "$start_time" +"%s")
end_timestamp=$(date -d "$end_time" +"%s")

for item in "${arrayList[@]}"
do
    current_timestamp=$start_timestamp
    # 切分元素为多个值
    table_name=$(echo $item | awk '{print $1}')
    time_column=$(echo $item | awk '{print $2}')
    sleep_hour_time=$(echo $item | awk '{print $3}')
    interval_time=$(echo $item | awk '{print $4}')
    echo -e ".................${table_name}................."
    echo -e "该表每执行完一次sleep时间:${sleep_hour_time}s...\n"

    while [ $current_timestamp -lt $end_timestamp ];
    do
      # 将当前时间戳转换为可读时间格式
      pre_start_time=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")
      current_timestamp=$((current_timestamp + ${interval_time}))
      next_end_time=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")

      echo -e "${pre_start_time} + ${next_end_time}"
      execute_with_retry "$table_name" "$time_column" "$pre_start_time" "$next_end_time" "common"
      sleep ${sleep_hour_time}s
    done
done



echo -e "+++++++++++++++++++++++++++++++++++++++++++++++\n"
echo -e "执行SUCCESS--------$(date "+%Y-%m-%d %H:%M:%S")\n"
echo -e "+++++++++++++++++++++++++++++++++++++++++++++++\n"

