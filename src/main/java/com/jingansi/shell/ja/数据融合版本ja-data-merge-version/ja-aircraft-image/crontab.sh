#!/bin/bash

<<comment
1. 历史数据刷新
  先创建一个临时表，建表语句在sql_file_ddl.sql
  然后执行start-his.sh脚本，将每个飞机所有的图片都分组到这个表中 对应的sql_file_his.sh 注意修改时间
  然后执行sql_file_ddl.sql下面的sql语句，关联原本图片表，生成新的飞机图片表

2.离线每日数据刷新
  sh start.sh 对应sql_file.sh
  crontab
50 02 * * * sh /data1/bigdata/apps/ja-data-merge-version/ja-aircraft-image/start.sh > /data1/bigdata/apps/ja-data-merge-version/ja-aircraft-image/root.log 2>&1


comment