#!/bin/bash

# 部署位置 : 172.21.30.201
	/data1/bigdata/apps/ja-sa-data-backup/ja-real-data

# 定时
35 5 * * * sh /data1/bigdata/apps/ja-sa-data-backup/ja-real-data-idc/start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-real-data-idc/root.log

nohup sh start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-real-data-idc/root.log &

<<comment
同步记录以语雀文档为准：https://jingan.yuque.com/staff-ycgiyb/od1rat/phks427fr8n7wdnz

1. 第1版本：执行start-v1.sh 里面执行的是sql_file_1-v1.sh和sql_file_2-v1.sh
2. 第2版本：执行start-v2.sh 里面执行的是sql_file-v2.sh    更新时间2024-08-01

comment




