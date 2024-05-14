#!/bin/bash

# 30分钟一次，实时
*/30 * * * * nohup sudo sh /data1/bigdata/apps/ja-sync-crontab-data/vessel-real/start.sh  > /data1/bigdata/apps/ja-sync-crontab-data/vessel-real/root.log 2>&1 &





