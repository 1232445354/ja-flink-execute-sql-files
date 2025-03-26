#!/bin/bash

# 部署警航服务器
10 2 * * * nohup sudo sh /data1/bigdata/apps/ja-sync-crontab-data/satellite-shanghai/start.sh  > /data1/bigdata/apps/ja-sync-crontab-data/satellite-shanghai/root.log 2>&1 &


