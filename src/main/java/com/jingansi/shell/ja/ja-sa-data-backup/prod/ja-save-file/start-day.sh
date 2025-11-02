#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
source ${DIR}/table_config.sh
echo -e "将态势数据save minio file...$(date "+%Y-%m-%d %H:%M:%S")"


# 定义一个执行 SQL 脚本的函数
execute_with_retry() {
  local table_name=$1
  local time_column=$2
  local pre_start_time=$3
  local next_end_time=$4
  local type=$5
  local start_time_y=$6
  local start_time_ymd=$7
  local parallelism=$8

  sh "${DIR}/sql_file.sh" "$table_name" "$time_column" "$pre_start_time" "$next_end_time" "$type" "$start_time_y" "$start_time_ymd" "$parallelism"
  if [ $? -eq 0 ]; then
    echo -e "执行成功\n"
  else
    echo -e "执行失败，重试中...\n"
    sleep 5s
  fi
}

# 前一天的时间
start_time=$(date -d "yesterday" "+%Y-%m-%d 00:00:00")
end_time=$(date "+%Y-%m-%d 00:00:00")
echo "start_time = [${start_time}],end_time = [${end_time}]" # 昨天0点、今天0点
start_time_ymd=$(date -d "yesterday" "+%Y%m%d")
start_time_y=$(date -d "yesterday" "+%Y")

# 将时间格式转换为 Unix 时间戳
for item in "${array_list_day[@]}"
do
    # 切分元素为多个值
    table_name=$(echo $item | awk '{print $1}')
    time_column=$(echo $item | awk '{print $2}')
    parallelism=$(echo $item | awk '{print $3}')
    echo -e "...........${table_name}............."
    execute_with_retry "$table_name" "$time_column" "$start_time" "$end_time" "day" "$start_time_y" "$start_time_ymd" "$parallelism"
    sleep 2s
done


echo -e "+++++++++++++++++++++++++++++++++++++++++++++++"
echo -e "执行SUCCESS--------$(date "+%Y-%m-%d %H:%M:%S")"
echo -e "+++++++++++++++++++++++++++++++++++++++++++++++"

