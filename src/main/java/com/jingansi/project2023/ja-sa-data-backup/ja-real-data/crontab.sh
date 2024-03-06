#!/bin/bash

# 部署位置 : 172.21.30.201
	/data1/bigdata/apps/ja-sa-data-backup/ja-real-data

# 定时
30 5 * * * sh /data1/bigdata/apps/ja-sa-data-backup/ja-real-data/start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-real-data/root.log

nohup sh start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-real-data/root.log &



# 小表
dws_ais_vessel_detail_static_attribute  # 全量静态属性表 update_time
#  2024-02-27 同步完成

dws_ais_vessel_status_info # 船舶状态表  update_time
#  2024-02-27 同步完成

dws_ais_vessel_all_info_day # 船舶按天  acquire_timestamp_format
#  2024-02-27 同步完成

dwd_ais_vessel_port_all_info # 船舶出发到达港口全量 acquire_timestamp_format
#  2024-02-27 同步完成

dws_ais_vessel_port_status_info # 船舶出发到达港口状态  acquire_timestamp_format
#  2024-02-27 同步完成

dws_vessel_list_status_rt # marinetraffic船舶状态数据 acquire_timestamp_format
#  2024-02-27 同步完成

dwd_satellite_all_info # 卫星 current_date
#  2024-02-27 同步完成

dws_aircraft_combine_status_rt # 飞机融合状态数据表  update_time
#  2024-02-27 同步完成

dws_flight_segment_rt # 飞机融合航班表 update_time
#  2024-02-27 同步完成




# 大表
 dwd_ais_vessel_all_info # 船舶全量表		需要update_time、观察中
#  2024-02-27 同步完成

dwd_vessel_list_all_rt # marinetraffic船舶全量数据  acquire_timestamp_format
#  2024-02-27 同步完成

dwd_adsbexchange_aircraft_list_rt # adsbexchange飞机全量数据  acquire_timestamp_format
#  2024-02-20 同步完成

#2024-01-18
#2024-01-19
#2024-01-20
#2024-01-21
#2024-01-22
#2024-01-28
#2024-01-31
#2024-02-01
#...
#2024-02-20
2024-02-21
2024-02-22
2024-02-23
2024-02-25


dws_aircraft_combine_list_rt  # 飞机融合全量数据表 update_time
#  2024-02-27 同步完成


# 取消同步表
dwd_aircraft_list_all_info  # 飞机全量数据表 acquire_timestamp_format
#  2024-01-01 同步完成

dws_aircraft_list_status_info # 飞机状态数据表 acquire_timestamp_format
#  2024-01-01 同步完成


