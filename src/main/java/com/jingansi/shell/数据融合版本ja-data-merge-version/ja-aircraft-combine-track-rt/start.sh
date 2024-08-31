#!/bin/bash
echo -en "--同步数据$(date +"%Y-%m-%d %H:%M:%S")---"
DIR=$(cd `dirname $0`; pwd)

time=$(date +"%Y-%m-%d %H:%M:%S")

# 获取减去1小时的时间
start_time=$(date -d "-1 hour" +"%Y-%m-%d %H:%M:%S")

echo -en "${start_time},${time}\n"


sh ${DIR}/sql_file_his.sh "${start_time}" "${time}"

echo -en "====================================\n"
echo -en "刷新数据SUCCESS.......$(date +"%Y-%m-%d %H:%M:%S")\n"
echo -en "====================================\n"

