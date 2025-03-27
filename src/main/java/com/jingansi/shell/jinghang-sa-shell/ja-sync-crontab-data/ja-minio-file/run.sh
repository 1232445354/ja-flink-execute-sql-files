#!/bin/bash

echo -e "开始同步数据 \n"

yesterday=$(date -d "yesterday" +"%Y%m%d")
two_days_ago=$(date -d "2 days ago" +"%Y-%m-%d")
echo -e "天气数据 yesterday = $yesterday two_days_ago=$two_days_ago\n"


current_date=$(date +"%Y-%m-%d")
yesterday1=$(date -d "yesterday" +"%Y-%m-%d")
two_days_ago=$(date -d "2 days ago" +"%Y-%m-%d")
echo -e "gps数据 yesterday = $yesterday1 current_date=$current_date two_days_ago=$two_days_ago\n"  # 输出格式：20231231


weather_command="nohup rclone copy oss:/ja-acquire-images/ja-weather/$yesterday  jh-minio:/ja-acquire-images/ja-weather/$yesterday/ > root_weather.log &"
weather_yes2="nohup rclone copy oss:/ja-acquire-images/ja-weather/$two_days_ago  jh-minio:/ja-acquire-images/ja-weather/$two_days_ago/ > root_weather_yes2.log &"



gps_yes2_command="nohup rclone copy oss:/ja-acquire-images/ja-gps/gpsjam2/data_${two_days_ago}.csv  jh-minio:/ja-acquire-images/ja-gps/gpsjam2/ > root_gps_yes2.log &"
gps_yes_command="nohup rclone copy oss:/ja-acquire-images/ja-gps/gpsjam2/data_${yesterday1}.csv  jh-minio:/ja-acquire-images/ja-gps/gpsjam2/ > root_gps_yes.log &"
gps_curr_command="nohup rclone copy oss:/ja-acquire-images/ja-gps/gpsjam2/data_${current_date}.csv  jh-minio:/ja-acquire-images/ja-gps/gpsjam2/ > root_gps_curr.log &"

echo -e "开始执行rclone命令\n"

eval $weather_command
eval $weather_yes2
eval $gps_yes2_command
eval $gps_yes_command
eval $gps_curr_command


echo -e "执行SUCESS \n"
