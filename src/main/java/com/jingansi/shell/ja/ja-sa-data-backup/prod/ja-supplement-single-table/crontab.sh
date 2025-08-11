#!/bin/bash

30 6 * * * sh /data1/bigdata/apps/ja-sa-data-backup/ja-supplement-data-single-table/start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-supplement-data-single-table/root.log

# 一个时间一个表的同步
# sql_file_aircraft.sh 是为了飞机表的减少数据量

sh start.sh "2024-06-22 13:00:00" "2024-06-22 13:30:00" "dws_aircraft_combine_list_rt" "acquire_time"

