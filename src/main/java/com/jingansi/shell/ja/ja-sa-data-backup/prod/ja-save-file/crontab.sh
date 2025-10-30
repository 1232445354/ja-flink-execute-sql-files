#!/bin/bash


# 按天执行
1 7 * * * sh /data1/bigdata/apps/ja-sa-data-backup/ja-save-file/start-day.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-save-file/root-day.log


# 按月执行
1 0 1 * * sh /data1/bigdata/apps/ja-sa-data-backup/ja-save-file/start-month.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-save-file/root-month.log

