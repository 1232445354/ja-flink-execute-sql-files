echo -e "同步船舶、飞机按天聚合数据......\n"
declare -a table_infos=(
"dws_bhv_aircraft_last_location_fd merge_time 1-2-3"
"dws_bhv_vessel_last_location_fd merge_time 1-2-3-4"
)

# 定义经度范围数组，将 -180 到 180 分成四个部分
declare -a longitude_ranges=(
  "-180 -90"
  "-90 0"
  "0 90"
  "90 180"
)


merge_start_time=$(date -d "${start_time}" "+%Y-%m-%d 00:00:00")
merge_end_time=$(date -d "${end_time}" "+%Y-%m-%d 00:00:00")

for table_info in "${table_infos[@]}"; do
  IFS=' ' read -r table_name time_column src_code <<< "$table_info" && IFS='-' read -ra src_codes <<< "$src_code"
  echo -e ".................${table_name}.................\n"

  for src_code in "${src_codes[@]}"; do
    echo -e "处理源代码: src_code = ${src_code}\n"
    # 遍历经度范围
    for lon_range in "${longitude_ranges[@]}"; do
      IFS=' ' read -r lon_start lon_end <<< "$lon_range"
      echo -e "经度范围: ${lon_start} 到 ${lon_end}\n"

      # 调用执行函数
      echo -e "$merge_start_time + ${merge_end_time} + src_code:{$src_code},,,\n"


#      execute_with_retry "$table_name" "$time_column" "$merge_start_time" "$merge_end_time" "${src_code}" "$lon_start" "$lon_end"
  done



#    echo -e "$merge_start_time + ${merge_end_time} + src_code:{$src_code}\n"
#    execute_with_retry "$table_name" "$time_column" "$merge_start_time" "$merge_end_time" "${src_code}"
  done


done