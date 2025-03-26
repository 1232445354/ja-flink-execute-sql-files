#!/bin/bash

yesterday=$(date -d "yesterday" +"%Y%m%d")
echo -e "开始同步数据 \n"
echo -e "天气数据 yesterday = $yesterday \n"

current_date=$(date +"%Y-%m-%d")
yesterday1=$(date -d "yesterday" +"%Y-%m-%d")

echo -e "gps数据 yesterday = $yesterday1 current_date=$current_date\n"  # 输出格式：20231231

weather_command="nohup rclone copy oss:/ja-acquire-images/ja-weather/$yesterday  jh-minio:/ja-acquire-images/ja-weather/$yesterday/ > root_weather.log &"

gps_yes_command="nohup rclone copy oss:/ja-acquire-images/ja-gps/gpsjam2/data_${yesterday1}.csv  jh-minio:/ja-acquire-images/ja-gps/gpsjam2/data_${yesterday1}.csv > root_gps_yes.log &"

gps_curr_command="nohup rclone copy oss:/ja-acquire-images/ja-gps/gpsjam2/data_${current_date}.csv  jh-minio:/ja-acquire-images/ja-gps/gpsjam2/data_${current_date}.csv > root_gps_curr.log &"

echo -e "开始执行rclone命令\n"

eval $weather_command
eval $gps_yes_command
eval $gps_curr_command


echo -e "执行SUCESS \n"
