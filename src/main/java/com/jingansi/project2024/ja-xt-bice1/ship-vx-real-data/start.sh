#!/bin/bash
#通过后台提交
flink_path=/data1/bigdata/flink-1.16.0
task_path=/data1/bigdata/apps/ship-vx-real-data
sql_file_name=sql_file.sql
cd ${flink_path}
./bin/sql-client.sh -f ${task_path}/${sql_file_name}



