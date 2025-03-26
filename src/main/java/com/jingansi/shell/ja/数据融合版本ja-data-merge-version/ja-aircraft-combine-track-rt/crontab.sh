#!/bin/bash
# 飞机刷新历史数据

<<comment
刷新历史数据
  1. 先执行sh start-his.sh 记得改刷新的时间，对应的是sql_file_his.sh
  2. 实时数据在flink修改



实时数据-105环境开发使用的，之后不需要的
  1. 定时执行start.sh 对应sql_file.sh

5 * * * * sh /data1/bigdata/apps/ja-data-merge-version/ja-aircraft-combine-track-rt/start.sh /data1/bigdata/apps/ja-data-merge-version/ja-aircraft-combine-track-rt/root.log 2>&1

sh start.sh "2024-08-01 12:00:00"

comment






