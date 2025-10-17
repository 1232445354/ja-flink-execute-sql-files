#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
echo -e "开始备份idc-态势数据...$(date "+%Y-%m-%d %H:%M:%S")"

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
  local min_lon=$5
  local max_lon=$6

  while true; do
     sh "${DIR}/sql_file.sh" "$table_name" "$time_column" "$pre_start_time" "$next_end_time" "$min_lon" "$max_lon"
    if [ $? -eq 0 ]; then
      echo -e "执行成功\n"
      break
    else
      echo -e "执行失败，重试中...\n"
      sleep 5s
    fi
  done
}


# table_name、time_column、front_day、after_day
per_table_sleep_time=5  # 每执行完一个表sleep时间
declare -a small_table_infos=(
"dwd_et_sa_share_info update_time 0 0"                      # 分享态势数据
"dwd_mtf_ship_info update_time 1 0"                         # marinetraffic单独的详情表
"dws_vessel_list_status_rt acquire_timestamp_format 0 0"    # marinetraffic的船舶状态数据单独
"dws_vt_vessel_status_info acquire_timestamp_format 0 0"    # vt的船舶状态数据单独
"dws_ais_landbased_vessel_status acquire_time 0 0"          # lb岸基数据的状态数据
"dwd_bhv_satellite_rt update_time 0 0"                      # 卫星全量数据
"dwd_bhv_satellite_sense_info photograph_datetime 0 0"      # 遥感融合表数据
"dwd_bhv_landsat_sense_list photograph_datetime 0 0"        # landsat单独的遥感图
"dws_bhv_aircraft_last_location_rt acquire_time 0 0"        # 飞机最后位置-新表、数据融合版本
"dws_vessel_bhv_status_rt acquire_time 0 0"                 # 船舶最后位置状态表
"dws_flight_segment_rt start_time 0 0"                      # 飞机航段表 - 自己计算的
)

echo -en "小表数据同步中......\n"
for table_info in "${small_table_infos[@]}"; do
  IFS=' ' read -r table_name time_column front_day after_day<<< "$table_info"
  pre_start_time=$(date -d "${start_time} -${front_day} day" "+%Y-%m-%d 00:00:00")
  next_end_time=$(date -d "${end_time} +${after_day} day" "+%Y-%m-%d 00:00:00")

  echo -e "------${table_name}:::{$pre_start_time---$next_end_time}------"
  execute_with_retry "$table_name" "$time_column" "$pre_start_time" "$next_end_time"
  sleep ${per_table_sleep_time}s
done

echo -e "---------------------------------------"
echo -e "小表同步完成"
echo -e "---------------------------------------\n"



echo -e "同步船舶、飞机按天聚合数据......\n"
declare -a table_infos=(
"dws_bhv_aircraft_last_location_fd merge_time"
"dws_bhv_vessel_last_location_fd merge_time"
)

aircraft_longitude_ranges=(
    "-180 -105"
    "-105 -85"
    "-85 -70"
    "-70 180"
)


vessel_longitude_ranges=(
      "-180 20"
      "20 110"
      "110 120"
      "120 180"
)

merge_start_time=$(date -d "${start_time}" "+%Y-%m-%d 00:00:00")
merge_end_time=$(date -d "${end_time}" "+%Y-%m-%d 00:00:00")

for table_info in "${table_infos[@]}"; do
  IFS=' ' read -r table_name time_column <<< "$table_info"
  echo -e ".................${table_name}.................\n"
  if [ "$table_name" = "dws_bhv_aircraft_last_location_fd" ]; then
    for range in "${aircraft_longitude_ranges[@]}"; do
      min_lon=$(echo $range | cut -d' ' -f1)
      max_lon=$(echo $range | cut -d' ' -f2)
      echo -e "$merge_start_time + ${merge_end_time} + min_lon:max_lon->{$min_lon,$max_lon}"
      execute_with_retry "$table_name" "$time_column" "$merge_start_time" "$merge_end_time" "${min_lon}" "${max_lon}"
    done
  else
    for range in "${vessel_longitude_ranges[@]}"; do
      min_lon=$(echo $range | cut -d' ' -f1)
      max_lon=$(echo $range | cut -d' ' -f2)
      echo -e "$merge_start_time + ${merge_end_time} + min_lon:max_lon->{$min_lon,$max_lon}"
      execute_with_retry "$table_name" "$time_column" "$merge_start_time" "$merge_end_time" "${min_lon}" "${max_lon}"
    done
  fi

done

echo -e "---------------------------------------"
echo -e "同步轨迹表数据......"
echo -e "---------------------------------------\n"
# 表名称、时间字段、同一个表不同时间之间的间隔、每次同步几小时(s)
arrayList=(
"dws_bhv_aircraft_last_location_dh merge_time 3 14400"                # 4 hour   飞机按小时聚合表
"dws_bhv_vessel_last_location_dh merge_time 3 14400"                  # 4 hour    船舶按小时聚合表
# "dws_vessel_bhv_track_rt acquire_time 10 3600"                      # 1 hour
"dwd_vessel_list_all_rt acquire_timestamp_format 5 7200"              # 2 hour    marinetraffic单独表
"dwd_adsbexchange_aircraft_list_rt acquire_timestamp_format 10 3600"  # 1 hour    adsbexchange单独表
"dwd_vt_vessel_all_info acquire_timestamp_format 5 3600"              # 1 hour    vt单独表
"dwd_ais_landbased_vessel_list acquire_time 5 14400"                  # 4 hour    lb单独表
"dwd_fr24_aircraft_list_rt acquire_time 5 3600"                       # 1 hour    f24单独表
#"dwd_bhv_aircraft_combine_rt acquire_time 10 1800"                    # 0.5 hour
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

    echo -e ".................${table_name}................."
    echo -e "该表每执行完一次sleep时间:${sleep_hour_time}s...\n"

    while [ $current_timestamp -lt $end_timestamp ];
    do
      # 将当前时间戳转换为可读时间格式
      pre_start_time=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")
      current_timestamp=$((current_timestamp + ${interval_time}))
      next_end_time=$(date -d "@$current_timestamp" +"%Y-%m-%d %H:%M:%S")

      echo -e "${pre_start_time} + ${next_end_time}"
      execute_with_retry "$table_name" "$time_column" "$pre_start_time" "$next_end_time"
      sleep ${sleep_hour_time}s
    done

done

echo -e "+++++++++++++++++++++++++++++++++++++++++++++++\n"
echo -e "执行SUCCESS--------$(date "+%Y-%m-%d %H:%M:%S")\n"
echo -e "+++++++++++++++++++++++++++++++++++++++++++++++\n"

