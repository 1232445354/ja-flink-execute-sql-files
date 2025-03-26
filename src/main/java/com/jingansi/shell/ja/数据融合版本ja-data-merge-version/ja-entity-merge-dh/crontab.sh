#!/bin/bash
# 聚合数据脚本

<<comment
刷新历史数据
  1. 先执行sh start-his.sh 刷新历史数据按小时聚合 - 记得修改时间和里面的类型对应飞机和船舶

实时数据每日刷新
  1. 将历史数据全部刷新完成之后在执行后续
  2. 定时调度sh start.sh  对应的还是sql_file_aircraft.sh、sql_file_vessel.sh
  3. crontab调度(每小时的40分执行)
    40 * * * * sh /data1/bigdata/apps/ja-data-merge-version/ja-entity-merge-dh/start.sh > /data1/bigdata/apps/ja-data-merge-version/ja-entity-merge-dh/root.log 2>&1


sh start.sh "2024-08-16 12:30:00"


comment




