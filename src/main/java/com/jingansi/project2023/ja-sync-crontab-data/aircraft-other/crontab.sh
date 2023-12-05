#!/bin/bash

1 1 * * * nohup sudo sh /data1/bigdata/apps/ja-sync-crontab-data/aircraft-other/start.sh  > /data1/bigdata/apps/ja-sync-crontab-data/aircraft-other/root.log 2>&1 &
