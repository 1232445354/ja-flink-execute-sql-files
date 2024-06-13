#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
echo -en "开始备份互联网态势数据...$(date)\n"

# 检查是否提供了参数
if [ $# -eq 0 ]; then
    # 没有参数
    start_time=$(date -d "yesterday" "+%Y-%m-%d 00:00:00")
    end_time=$(date "+%Y-%m-%d 00:00:00")
    echo "没有参数,则start_time = [${start_time}],end_time = [${end_time}]"
else
    # 有参数
    start_time=$1
    if [ $# -ge 2 ]; then
        end_time=$2
        echo "传入参数,则start_time = [${start_time}],end_time = [${end_time}]"
    fi
fi


echo -en "开始同步小表数据....\n"
sh ${DIR}/sql_file_1.sh "$start_time" "$end_time"
sleep 30s
echo -en "-----\n"
echo -en "开始同步大表数据....\n"


# 表名称、没一个小时的间隔、时间字段
arrayList=(
  "dwd_adsbexchange_aircraft_list_rt 5 acquire_timestamp_format"
  "dwd_vessel_list_all_rt 5 acquire_timestamp_format"
  "dwd_vt_vessel_all_info 5 acquire_timestamp_format"
  "dwd_ais_vessel_all_info 10 acquire_timestamp_format"
  "dws_aircraft_combine_list_rt 10 acquire_time"
  "dwd_fr24_aircraft_list_rt 5 acquire_time"
  "dwd_ais_landbased_vessel_list 5 acquire_time"
)

# 将时间格式转换为 Unix 时间戳
start_timestamp=$(date -d "$start_time" +"%s")
end_timestamp=$(date -d "$end_time" +"%s")


for item in "${arrayList[@]}"
do
    current_timestamp=$start_timestamp
    # 切分元素为多个值
    table_name=$(echo $item | awk '{print $1}')
    sleep_time=$(echo $item | awk '{print $2}')
    time_column=$(echo $item | awk '{print $3}')
    echo "..............备份表${table_name}.............."

    while [ $current_timestamp -lt $end_timestamp ];
    do
      # 将当前时间戳转换为可读时间格式
      current_time1=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")
      current_timestamp=$((current_timestamp + 3600))
      next_time1=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")

      echo -en "${current_time1} + ${next_time1}\n"
      sh ${DIR}/sql_file_2.sh "$table_name" "$time_column" "$current_time1" "$next_time1"
      echo "sleep中,时间：${sleep_time}s"
      sleep ${sleep_time}s
    done

done

echo -en "+++++++++++++++++++++++++++++++++++++\n"
echo -en "执行SUCCESS--------$(date)\n"
echo -en "+++++++++++++++++++++++++++++++++++++\n"

