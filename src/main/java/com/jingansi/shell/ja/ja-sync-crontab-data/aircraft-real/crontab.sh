#!/bin/bash

# 5分钟一次 - 实时
*/5 * * * * nohup sudo sh /data1/bigdata/apps/ja-sync-crontab-data/aircraft-real/start.sh  > /data1/bigdata/apps/ja-sync-crontab-data/aircraft-real/root.log 2>&1 &


# 1小时一次 - 实时
20 * * * * nohup sudo sh /data1/bigdata/apps/ja-sync-crontab-data/aircraft-real/start.sh  > /data1/bigdata/apps/ja-sync-crontab-data/aircraft-real/root.log 2>&1 &


