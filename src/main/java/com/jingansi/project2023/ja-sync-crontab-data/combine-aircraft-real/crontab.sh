#!/bin/bash

# 2分钟一次 - 实时
*/2 * * * * nohup sudo sh /data1/bigdata/apps/ja-sync-crontab-data/combine-aircraft-real/start.sh  > /data1/bigdata/apps/ja-sync-crontab-data/combine-aircraft-real/root.log 2>&1 &
