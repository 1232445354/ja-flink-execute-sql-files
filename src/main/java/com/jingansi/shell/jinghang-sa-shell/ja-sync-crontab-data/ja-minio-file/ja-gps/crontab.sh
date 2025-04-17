#!/bin/bash

# 部署警航135.100.11.132服务器
# gps数据每天8点生成，所以9点执行该程序

0 9 * * * nohup sudo sh /data1/bigdata/apps/ja-sync-crontab-data/ja-minio-file/ja-gps/run.sh  > /data1/bigdata/apps/ja-sync-crontab-data/ja-minio-file/ja-gps/root.log 2>&1 &


