#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
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

# table_name、time_column、front_day、after_day、front_operator、after_operator
per_table_sleep_time=5  # 每执行完一个表sleep时间
declare -a small_table_infos=(
"dws_ais_vessel_detail_static_attribute update_time 1 0 - +"    # 船舶实体属性
"dwd_mtf_ship_info update_time 1 0 - +"                         # marinetraffic的详情表
"dws_ais_vessel_status_info acquire_timestamp_format 0 0 - +"   # 船舶融合状态表
"dws_vessel_list_status_rt acquire_timestamp_format 0 0 - +"    # marinetraffic的船舶状态数据单独入库
"dws_vt_vessel_status_info acquire_timestamp_format 0 0 - +"    # vt的船舶状态数据单独入库
"dwd_bhv_satellite_rt acquire_time 1 0 - +"                     # 卫星全量数据表
"dws_bhv_satellite_list_fd today_time 1 0 - +"                  # 卫星每日聚合表
"dws_et_satellite_info update_time 1 0 - +"                     # 卫星实体表
"dws_atr_satellite_image_info update_time 1 0 - +"              # 卫星图片
"dws_ais_vessel_all_info_day acquire_timestamp_format 0 0 - +"  # 船舶按天的数据
"dws_flight_segment_rt start_time 0 0 - +"                      # 飞机航班表
"dws_et_aircraft_info acquire_time 1 0 - +"                     # 飞机实体表
"dws_bhv_aircraft_last_location_rt acquire_time 0 0 - +"        # 飞机最后位置-新表、数据融合版
"dws_bhv_aircraft_last_location_dh merge_time 0 0 - +"          # 飞机按小时聚合表
"dws_bhv_aircraft_last_location_fd merge_time 0 1 - -"          # 飞机按天聚合表
"dws_airport_flight_info update_time 1 0 - +"                   # 飞机起飞航班表

#"dws_aircraft_combine_status_rt acquire_time 0 0 - +"           # 飞机实体融合状态表
#"dwd_satellite_tle_list update_time 2 0" # 卫星tle融合表,全量表1
#"dws_satellite_tle_info current_date 2 0" # 卫星tle融合表2
#"dws_satellite_entity_info update_time 1 0" # 卫星实体表

)


echo -en "小表数据备份中......\n"
for table_info in "${small_table_infos[@]}"; do
  IFS=' ' read -r table_name time_column front_day after_day front_operator after_operator<<< "$table_info"

  pre_start_time=$(date -d "${start_time} ${front_operator}${front_day} day" "+%Y-%m-%d 00:00:00")
  next_end_time=$(date -d "${end_time} ${front_operator}${after_day} day" "+%Y-%m-%d 00:00:00")

  echo "------${table_name}:::{${pre_start_time}---${next_end_time}}------"

  while true; do
    sh ${DIR}/sql_file.sh "$table_name" "$time_column" "$pre_start_time" "$next_end_time"
    if [ $? -eq 0 ]; then
      echo -en "执行成功\n"
      break
    else
      echo -en "执行失败，重试中...\n"
      sleep 1s
    fi
  done
  sleep ${per_table_sleep_time}s
done

echo -en "小表备份完成，sleep:10s，开始同步大表...\n"
sleep 10s
echo -en "---------------------------------------\n"





# 表名称、时间字段、同一个表不同时间之间的间隔、每次同步几小时(s)
arrayList=(
"dwd_ais_vessel_all_info acquire_timestamp_format 10 7200"            # 2 hour
"dwd_vessel_list_all_rt acquire_timestamp_format 5 7200"              # 2 hour
"dwd_adsbexchange_aircraft_list_rt acquire_timestamp_format 10 3600"  # 1 hour
"dwd_vt_vessel_all_info acquire_timestamp_format 5 7200"              # 2 hour
"dwd_ais_landbased_vessel_list acquire_time 5 14400"                  # 4 hour
"dwd_fr24_aircraft_list_rt acquire_time 5 7200"                       # 2 hour
"dwd_bhv_aircraft_combine_rt acquire_time 10 1800"                   # 0.5 hour
)

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

    echo ".................${table_name}................."
    echo "sleep时间：${sleep_hour_time}s..."

    while [ $current_timestamp -lt $end_timestamp ];
    do
      # 将当前时间戳转换为可读时间格式
      pre_start_time=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")
      current_timestamp=$((current_timestamp + ${interval_time}))
      next_end_time=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")

      echo -en "${pre_start_time} + ${next_end_time}\n"
      while true; do
        sh ${DIR}/sql_file.sh "$table_name" "$time_column" "$pre_start_time" "$next_end_time"
        if [ $? -eq 0 ]; then
          echo -en "执行成功\n"
          break
        else
          echo -en "执行失败，重试中...\n"
          sleep 1s
        fi
      done
      sleep ${sleep_hour_time}s
    done

done

echo -en "+++++++++++++++++++++++++++++++++++++++++++++++\n"
echo -en "执行SUCCESS--------$(date "+%Y-%m-%d %H:%M:%S")\n"
echo -en "+++++++++++++++++++++++++++++++++++++++++++++++\n"

