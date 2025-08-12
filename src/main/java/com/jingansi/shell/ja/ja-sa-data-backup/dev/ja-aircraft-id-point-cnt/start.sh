#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
echo -en "统计实体点位数据...$(date "+%Y-%m-%d %H:%M:%S")\n"

# 检查是否提供了参数
if [ $# -eq 0 ]; then
    # 没有参数
    start_time=$(date -d "yesterday" "+%Y-%m-%d 00:00:00")
    end_time=$(date "+%Y-%m-%d 00:00:00")
    echo "没有参数,则start_time = [${start_time}],end_time = [${end_time}]" # 昨天0点、今天0点
else
    # 有参数
    start_time=$1
    if [ $# -ge 2 ]; then
        end_time=$2
        echo "传入参数,则start_time = [${start_time}],end_time = [${end_time}]"
    fi
fi


while true; do
#  sh "${DIR}/sql_file.sql" "$start_time" "$end_time"
  if [ $? -eq 0 ]; then
    echo -en "执行成功\n"
    break
  else
    echo -en "执行失败，重试中...\n"
    sleep 10s
  fi
done



echo -en "-----------------------------------------------\n"
echo -en "执行SUCCESS--------$(date "+%Y-%m-%d %H:%M:%S")\n"
echo -en "-----------------------------------------------\n"
