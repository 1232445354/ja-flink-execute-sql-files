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


# 定义一个执行 SQL 脚本的函数
execute_with_retry() {
  local table_name=$1
  local time_column=$2
  local pre_start_time=$3
  local next_end_time=$4
  local src_code=$5

  while true; do
    sh "${DIR}/sql_file.sh" "$table_name" "$time_column" "$pre_start_time" "$next_end_time" "$src_code"
    if [ $? -eq 0 ]; then
      echo -en "执行成功\n"
      break
    else
      echo -en "执行失败，重试中...\n"
      sleep 1s
    fi
  done
}


# table_name、time_column、front_day、after_day
per_table_sleep_time=5  # 每执行完一个表sleep时间
declare -a small_table_infos=(
"dwd_mtf_ship_info update_time 1 0"                         # marinetraffic的详情表
"dws_vessel_list_status_rt acquire_timestamp_format 0 0"    # marinetraffic的船舶状态数据单独入库
"dws_vt_vessel_status_info acquire_timestamp_format 0 0"    # vt的船舶状态数据单独入库
"dws_ais_landbased_vessel_status acquire_time 0 0"          # lb岸基数据的状态数据入库
"dwd_bhv_satellite_rt acquire_time 1 0"                     # 卫星全量数据表
"dws_bhv_satellite_list_fd today_time 1 0"                  # 卫星每日聚合表
"dws_et_satellite_info update_time 1 0"                     # 卫星实体表
"dws_atr_satellite_image_info update_time 1 0"              # 卫星图片
"dws_et_aircraft_info acquire_time 1 0"                     # 飞机实体表
"dws_bhv_aircraft_last_location_rt acquire_time 0 0"        # 飞机最后位置-新表、数据融合版
"dws_airport_flight_info update_time 1 0"                   # 飞机起飞航班表
"dws_atr_aircraft_image_info update_time 0 0"               # 飞机图片表
"dws_atr_aircraft_image_id_info update_time 0 0"            # 飞机图片id表
"dws_vessel_et_info_rt update_time 1 0"                     # 船舶实体表
"dws_vessel_bhv_status_rt acquire_time 0 0"                 # 船舶最后位置状态表
"dws_flight_segment_rt start_time 0 0"                      # 飞机航段表 - 自己计算的
"dws_bhv_airport_weather_info acquire_time 0 0"             # 机场天气数据表

#"dws_ais_vessel_all_info_day acquire_timestamp_format 0 0 - +"  # 船舶按天的数据
#"dws_ais_vessel_detail_static_attribute update_time 1 0 - +"    # 船舶实体属性
#"dws_ais_vessel_status_info acquire_timestamp_format 0 0 - +"   # 船舶融合状态表
#"dws_aircraft_combine_status_rt acquire_time 0 0 - +"           # 飞机实体融合状态表
#"dwd_satellite_tle_list update_time 2 0" # 卫星tle融合表,全量表1
#"dws_satellite_tle_info current_date 2 0" # 卫星tle融合表2
#"dws_satellite_entity_info update_time 1 0" # 卫星实体表
)

echo -en "小表数据同步中......\n"
for table_info in "${small_table_infos[@]}"; do
  IFS=' ' read -r table_name time_column front_day after_day<<< "$table_info"

  pre_start_time=$(date -d "${start_time} -${front_day} day" "+%Y-%m-%d 00:00:00")
  next_end_time=$(date -d "${end_time} +${after_day} day" "+%Y-%m-%d 00:00:00")

  # shellcheck disable=SC2028
  echo "------${table_name}:::{$pre_start_time---$next_end_time}------\n"
  execute_with_retry "$table_name" "$time_column" "$pre_start_time" "$next_end_time"
  sleep ${per_table_sleep_time}s
done

echo -en "小表同步完成"
echo -en "---------------------------------------\n"



echo -en "同步船舶、飞机按天聚合数据......\n"
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
    execute_with_retry "$table_name" "$time_column" "$merge_start_time" "$merge_end_time" "${src_code}"
  done

done



# 表名称、时间字段、同一个表不同时间之间的间隔、每次同步几小时(s)
arrayList=(
"dws_bhv_aircraft_last_location_dh merge_time 3 43200"                # 12 hour
"dws_bhv_vessel_last_location_dh merge_time 3 21600"                  # 6 hour
"dws_vessel_bhv_track_rt acquire_time 10 7200"                        # 2 hour
"dwd_vessel_list_all_rt acquire_timestamp_format 5 7200"              # 2 hour
"dwd_adsbexchange_aircraft_list_rt acquire_timestamp_format 10 3600"  # 1 hour
"dwd_vt_vessel_all_info acquire_timestamp_format 5 7200"              # 2 hour
"dwd_ais_landbased_vessel_list acquire_time 5 14400"                  # 4 hour
"dwd_fr24_aircraft_list_rt acquire_time 5 7200"                       # 2 hour
"dwd_bhv_aircraft_combine_rt acquire_time 10 1800"                    # 0.5 hour
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
      execute_with_retry "$table_name" "$time_column" "$pre_start_time" "$next_end_time"
      sleep ${sleep_hour_time}s
    done

done

echo -en "+++++++++++++++++++++++++++++++++++++++++++++++\n"
echo -en "执行SUCCESS--------$(date "+%Y-%m-%d %H:%M:%S")\n"
echo -en "+++++++++++++++++++++++++++++++++++++++++++++++\n"

