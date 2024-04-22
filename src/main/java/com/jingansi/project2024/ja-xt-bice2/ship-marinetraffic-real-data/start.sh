#!/bin/bash
#通过后台提交
flink_path=/data1/bigdata/flink-1.16.0
task_path=/data1/bigdata/apps/ship-marinetraffic-real-data
sql_file_name=sql_file.sql
cd ${flink_path}
./bin/sql-client.sh -f ${task_path}/${sql_file_name}



#./bin/sql-client.sh -j ${task_path}/geo-udf-1.0-SNAPSHOT-jar-with-dependencies.jar -f ${task_path}/${sql_name}


