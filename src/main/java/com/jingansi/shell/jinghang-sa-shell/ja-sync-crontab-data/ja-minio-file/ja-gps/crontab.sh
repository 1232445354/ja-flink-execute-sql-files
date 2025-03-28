#!/bin/bash

# 部署204服务器
# gps数据每天8点生成，所以9点执行该程序

0 9 * * * nohup sudo sh /opt/bigdata/sync-jinghang/ja-minio-file/ja-gps/run.sh  > /opt/bigdata/sync-jinghang/ja-minio-file/ja-gps/root.log 2>&1 &


