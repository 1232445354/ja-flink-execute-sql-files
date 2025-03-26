#!/bin/bash
start_time=`date -d "today 00:00:00" +"%Y-%m-%d %H:%M:%S"`
echo -en "开始聚合数据...$(date)\n"
echo ${start_time}
DIR=$(cd `dirname $0`; pwd)

sh ${DIR}/sql_file.sh "$start_time"

echo -en "================================\n"
echo -en "执行success     $(date)\n"
echo -en "================================\n"


#50 10 * * * sh /data1/bigdata/apps/ja-satellite-tle-data/start-v1.sh



