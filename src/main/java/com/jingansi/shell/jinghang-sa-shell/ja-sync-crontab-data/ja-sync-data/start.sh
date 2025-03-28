#!/bin/bash

echo -e "数据同步...$(date +"%Y-%m-%d %H:%M:%S")\n"
DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sleep_time=3  # 每执行完一个表sleep时间

declare -a table_infos=(
"dws_bhv_satellite_list_fd today_time 1"        # 卫星行为轨道表
"dws_atr_satellite_image_info acquire_time 1"   # 卫星图片表
)

max_retries=10
retry_count=0
for table_info in "${table_infos[@]}"; do
  IFS=' ' read -r table_name time_column pre_day_cnt<<< "$table_info"
  echo -e "-----------${table_name}-----------\n"

  while [ $retry_count -lt $max_retries ]; do
    sh ${DIR}/sql_file.sh "$table_name" "$time_column" "$pre_day_cnt"
    if [ $? -eq 0 ]; then
      echo -e "执行成功\n"
      break
    else
      retry_count=$((retry_count+1))
      echo -e "执行失败，重试中...\n"
      sleep 1s
    fi
  done
  echo "sleep中,时间：${sleep_time}s..."
  sleep ${sleep_time}s
done

# 卫星实体表
echo -e "卫星实体表 同步中 ...\n"
sh ${DIR}/sql_file1.sh

echo -e "船舶实体表 同步中 ...\n"
sh ${DIR}/sql_file2.sh

echo -e "飞机实体表 同步中 ...\n"
sh ${DIR}/sql_file3.sh


echo -e "+++++++++++++++++++++++++++++++++++++++++++++++\n"
echo -e "执行SUCCESS------$(date +"%Y-%m-%d %H:%M:%S")\n"
echo -e "+++++++++++++++++++++++++++++++++++++++++++++++\n"