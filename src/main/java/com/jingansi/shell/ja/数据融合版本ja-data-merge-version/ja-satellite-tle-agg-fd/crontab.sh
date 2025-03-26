#!/bin/bash
# 卫星每日聚合数据脚本

<<comment
刷新历史数据
  1. 先执行sql_file_only_once.sql 直接navicat执行
  2. 在执行sh start-his.sh 记得改刷新的时间，对应的是sql_file_his.sh

实时数据每日刷新
  1. 将历史数据全部刷新完成之后在执行后续
  2. 定时调度sh start.sh  对应的是sql_file.sh
  3. crontab调度(每日0点10分、5点10分执行)
    10 0,10 * * * sh /data1/bigdata/apps/ja-data-merge-version/ja-satellite-tle-agg-fd/start.sh > /data1/bigdata/apps/ja-data-merge-version/ja-satellite-tle-agg-fd/root.log 2>&1

comment




