#!/bin/bash

1 2 * * * nohup sudo sh /data1/bigdata/apps/ja-sync-crontab-data/vessel-other/start.sh  > /data1/bigdata/apps/ja-sync-crontab-data/vessel-other/root.log 2>&1 &

