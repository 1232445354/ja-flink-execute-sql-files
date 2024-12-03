#!/bin/bash

echo -en "START替换飞机追踪id数据...$(date)\n"

DIR=$(cd `dirname $0`; pwd)

# 年月日时分秒
current_minute=$(date +"%Y-%m-%d %H:%M:00")

# 减去一个小时，使用 -1 hours 作为时间偏移
new_time_stamp=$(date -d "$current_minute" +"%s")
# 减去一个小时的秒数
new_time_stamp=$((new_time_stamp - 3600))
# 格式化
last1hour_minute=$(date -d "@$new_time_stamp" +"%Y-%m-%d %H:%M:00")

echo -en "当前时间分钟${current_minute},上一个小时的时间分钟${last1hour_minute}\n"

sh ${DIR}/sql_file.sh "$current_minute" "$last1hour_minute"


echo -en "刷新SUCCESS.......$(date)\n"
echo -en "----------------------------------\n"