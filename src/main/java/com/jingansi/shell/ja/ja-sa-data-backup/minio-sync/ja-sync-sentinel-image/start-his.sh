#!/bin/bash

# 定义开始时间和结束时间
start_date="2025-01-04"
end_date="2025-02-24"

# 将日期转换为时间戳（秒数）
start_timestamp=$(date -d "$start_date" +%s)
end_timestamp=$(date -d "$end_date" +%s)

# 遍历日期
current_timestamp=$start_timestamp
while [ $current_timestamp -le $end_timestamp ]; do
    # 将时间戳转换为日期格式（YYYYMMDD）
    current_date=$(date -d "@$current_timestamp" +%Y%m%d)

    # 拼接命令
    command="nohup rclone copy oss:/ja-acquire-images/ja-sentinel2/2025/$current_date minio:/ja-acquire-images/ja-sentinel2/2025/$current_date/ > root_$current_date.log &"

    # 输出命令（或者直接执行）
    echo "$command"

    # 执行命令
    eval $command

    # 增加一天
    current_timestamp=$((current_timestamp + 86400))  # 86400秒 = 1天
done

echo -e "-------------------------------------\n"
echo -e "执行完成\n"
echo -e "-------------------------------------\n"

