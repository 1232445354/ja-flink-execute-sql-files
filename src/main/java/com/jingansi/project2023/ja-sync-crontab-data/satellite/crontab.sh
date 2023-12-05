#!/bin/bash

1 3 * * * nohup sudo sh /data1/bigdata/apps/ja-sync-crontab-data/satellite/start.sh  > /data1/bigdata/apps/ja-sync-crontab-data/satellite/root.log 2>&1 &


