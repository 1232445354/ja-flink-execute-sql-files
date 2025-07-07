#!/bin/bash

30 6 * * * sh /data1/bigdata/apps/ja-sa-data-backup/ja-supplement-data-single-table/start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-supplement-data-single-table/root.log


#第一种不传入参数 start-v1.sh
#第二种传入参数  start_supp_table-v1.sh
#  start_day、end_day、table_name、time_column、

sh start.sh "2024-06-22 13:00:00" "2024-06-22 13:30:00" "dws_aircraft_combine_list_rt" "acquire_time"

