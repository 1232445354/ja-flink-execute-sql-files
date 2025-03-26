#!/bin/bash

echo -en "idc实时数据同步201...$(date +"%Y-%m-%d %H:%M:%S")\n"
DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sleep_time=1  # 每执行完一个表sleep时间
declare -a table_infos=(
"dws_bhv_satellite_list_fd today_time"  # 卫星每日聚合
)


for table_info in "${table_infos[@]}"; do
  IFS=' ' read -r table_name time_column <<< "$table_info"
  echo -en "------------${table_name}------------\n"
  sql="insert into sa.${table_name} select * from doris_idc.sa.${table_name} where ${time_column} >= to_date(date_sub(now(),interval 1 day))"
  while true; do

    mysql -h${host} -P${port} -u${username} -p${password} -e "${sql}"
    if [ $? -eq 0 ]; then
      echo -en "执行成功\n"
      break
    else
      echo -en "执行失败，重试中...\n"
      sleep 1s
    fi
  done
  echo "sleep中,时间：${sleep_time}s..."
  sleep ${sleep_time}s
done

echo -en "+++++++++++++++++++++++++++++++++++++++++++++++++\n"
echo -en "执行SUCCESS------$(date +"%Y-%m-%d %H:%M:%S")\n"
echo -en "+++++++++++++++++++++++++++++++++++++++++++++++++\n"

