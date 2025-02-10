#!/bin/bash
# 当天同步前5天的数据 示例：2025-01-10 同步2025-01-05

# 1. 获取时间
formatted_date=$(date -d "-5 days" +"%Y%m%d")
year=2025

echo -en "执行命令：rclone copy oss:/ja-acquire-images/ja-sentinel2/${year}/${formatted_date} minio:/ja-acquire-images/ja-sentinel2/${year}/${formatted_date}/"

nohup rclone copy oss:/ja-acquire-images/ja-sentinel2/${year}/${formatted_date} minio:/ja-acquire-images/ja-sentinel2/${year}/${formatted_date}/ > root.log &
