#!/bin/bash

*/2 * * * * nohup sudo sh /data1/bigdata/apps/ja-sync-crontab-data/aircraft-real/start.sh  > /data1/bigdata/apps/ja-sync-crontab-data/aircraft-real/root.log 2>&1 &
