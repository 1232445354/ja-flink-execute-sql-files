#!/bin/bash

# 5分钟一次 - 实时
10 2 * * * nohup sudo sh /data1/bigdata/apps/ja-sync-crontab-data/satellite-shanghai/start.sh  > /data1/bigdata/apps/ja-sync-crontab-data/satellite-shanghai/root.log 2>&1 &



