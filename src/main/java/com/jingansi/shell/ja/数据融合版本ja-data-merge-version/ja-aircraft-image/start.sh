#!/bin/bash

start_time=$(date +"%Y-%m-%d %H:%M:%S")
echo -en "开始刷新飞机图片数据...$(date)\n"
echo -en "时间：${start_time} \n"
DIR=$(cd `dirname $0`; pwd)

sh ${DIR}/sql_file.sh "$start_time"

echo -en "+++++++++++++++++++++++++++++++++++++\n"
echo -en "执行SUCCESS--------$(date)\n"
echo -en "+++++++++++++++++++++++++++++++++++++\n"



