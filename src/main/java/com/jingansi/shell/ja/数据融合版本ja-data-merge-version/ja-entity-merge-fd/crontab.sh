#!/bin/bash
# 聚合数据脚本

# 聚合数据脚本

<<comment
刷新历史数据
  1. 先执行sh start-his.sh 刷新历史数据按天聚合 - 记得修改时间和里面的类型对应飞机和船舶

实时数据每日刷新
  1. 将历史数据全部刷新完成之后在执行后续
  2. 定时调度sh start.sh  对应的还是sql_file_aircraft.sh、sql_file_vessel.sh
  3. crontab调度(每小时的50分执行)

50 00 * * * sh /data1/bigdata/apps/ja-data-merge-version/ja-entity-merge-fd/start.sh > /data1/bigdata/apps/ja-data-merge-version/ja-entity-merge-fd/root.log 2>&1

comment




