#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
echo -en "开始按小时聚合生成数据...$(date)\n"

# 判断是否传入参数
if [ $# -eq 1 ]; then
  # 使用传入的时间
  cur_time=$1
  cur_hour_time=$(date -d "$cur_time" +"%Y-%m-%d %H:00:00")
  echo -en "传入时间参数，即当前时间为:${cur_hour_time}\n"
else
  # 使用当前小时时间
  cur_hour_time=$(date +"%Y-%m-%d %H:%M:%S")
  echo -en "未传入时间参数，即当前时间为:${cur_hour_time}\n"
fi

# 类型
arrayList=(
  "aircraft"
  "vessel"
)

sleep_time=1

for item in "${arrayList[@]}"
do
    # 切分元素为多个值
    type=$(echo $item | awk '{print $1}')
    echo -en "..............表数据生成中,类型：${type}..............\n"

    sql_file="sql_file_"
    if [ "$type" == "aircraft" ]; then
      sql_file="${sql_file}${type}.sh"
    elif [ "$type" == "vessel" ]; then
      sql_file="${sql_file}${type}.sh"
    fi

    sh ${DIR}/${sql_file} "$cur_hour_time"
    echo "sleep中,时间：${sleep_time}s"
    sleep ${sleep_time}s
done

echo -en "+++++++++++++++++++++++++++++++++++++\n"
echo -en "执行SUCCESS--------$(date)\n"
echo -en "+++++++++++++++++++++++++++++++++++++\n"

