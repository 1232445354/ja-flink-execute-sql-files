#!/bin/bash

# 部署204服务器
# gps数据每天8点生成，所以9点执行该程序

10 2,8,14,20 * * * nohup sudo sh /opt/bigdata/sync-jinghang/ja-minio-file/ja-weather/run.sh  > /opt/bigdata/sync-jinghang/ja-minio-file/ja-weather/root.log 2>&1 &


