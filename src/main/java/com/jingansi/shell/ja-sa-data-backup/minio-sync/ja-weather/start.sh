#!/bin/bash
# 当天同步前5天的数据 示例：2025-01-10 同步2025-01-05

# 1. 获取时间
formatted_date=$(date -d "-5 days" +"%Y%m%d")

command="nohup rclone copy oss:/ja-acquire-images/ja-weather/${formatted_date} minio:/ja-acquire-images/ja-weather/${formatted_date} > root.log &"

# 输出命令（或者直接执行）
echo "$command"

# 执行命令
eval $command
