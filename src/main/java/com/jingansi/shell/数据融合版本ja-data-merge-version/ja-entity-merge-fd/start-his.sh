#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
echo -en "开始按天聚合生成数据...$(date)\n"

# 实体类型
arrayList=(
  "aircraft"
)

sleep_time=5
start_time="2023-05-19 00:00:00"
end_time="2024-08-02 00:00:00"

start_timestamp=$(date -d "$start_time" +"%s")
end_timestamp=$(date -d "$end_time" +"%s")

for item in "${arrayList[@]}"
do
    # 切分元素为多个值
    type=$(echo $item | awk '{print $1}')
    echo -en "..............表数据生成中,类型：${type} 开始时间:${start_time},结束时间:${end_time}..............\n"

    sql_file="sql_file_"
    if [ "$type" == "aircraft" ]; then
      sql_file="${sql_file}${type}.sh"
    elif [ "$type" == "vessel" ]; then
      sql_file="${sql_file}${type}.sh"
    fi

    current_timestamp=$start_timestamp
    while [ $current_timestamp -lt $end_timestamp ];
    do
#      将当前时间戳转换为可读时间格式
      cur_time=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:00:00")
      echo -en "${cur_time}\n"

      sh ${DIR}/${sql_file} "${cur_time}"
      current_timestamp=$((current_timestamp + 86400))

      echo "sleep中,时间：${sleep_time}s"
      sleep ${sleep_time}s
    done
done

echo -en "+++++++++++++++++++++++++++++++++++++\n"
echo -en "执行SUCCESS--------$(date)\n"
echo -en "+++++++++++++++++++++++++++++++++++++\n"

