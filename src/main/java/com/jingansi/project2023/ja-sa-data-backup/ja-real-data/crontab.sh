#!/bin/bash

# 部署位置 : 172.21.30.201
	/data1/bigdata/apps/ja-sa-data-backup/ja-real-data

# 定时
30 5 * * * sh /data1/bigdata/apps/ja-sa-data-backup/ja-real-data/start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-real-data/root.log

nohup sh start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-real-data/root.log &



# 小表
dws_ais_vessel_detail_static_attribute
# 船舶实体属性 update_time
# 2024-04-23 同步完成

dws_ais_vessel_status_info
# 船舶融合状态表  update_time
# 2024-04-23 同步完成

dws_ais_vessel_all_info_day
# 船舶按天的数据  acquire_timestamp_format
# 2024-04-23 同步完成

dws_vessel_list_status_rt
# marinetraffic船舶状态数据 update_time
# 2024-04-23 同步完成

dws_aircraft_combine_status_rt
# 飞机融合状态数据表  update_time
# 2024-04-23 同步完成

dws_flight_segment_rt
# 飞机融合航班表 update_time
# 2024-04-23 同步完成

dwd_mtf_ship_info
# marinetraffic的详情表 update_time
# 2024-04-23 同步完成

dws_vt_vessel_status_info
# vt的船舶状态数据单独入库 update_time
# 2024-04-23 同步完成

dwd_satellite_tle_list
# 卫星tle融合表1,卫星全量数据入库 epoch_time
# 2024-04-23 同步完成

dws_satellite_tle_info
# 卫星tle融合表2，轨道表，由dwd_satellite_tle_list表离线生成的 current_date
# 2024-04-23 同步完成



# 大表
dwd_ais_vessel_all_info
# 船舶全量表	acquire_timestamp_format
# 2024-04-23 同步完成

dws_aircraft_combine_list_rt
# 飞机融合全量数据表 update_time -> 换成acquire_time
# 2024-04-23 同步完成

dwd_vessel_list_all_rt
# marinetraffic船舶全量数据  acquire_timestamp_format
# 2024-04-23 同步完成

dwd_adsbexchange_aircraft_list_rt
# adsbexchange飞机全量数据  acquire_timestamp_format
# 2024-04-23 同步完成

dwd_vt_vessel_all_info
# vt的船舶全量数据单独入库
# 2024-04-23 同步完成

dwd_fr24_aircraft_list_rt
# f24飞机轨迹表 acquire_time
# 2024-04-23 开始同步

dwd_ais_landbased_vessel_list
# 船舶的网站数据 acquire_time
# 2024-04-23 开始同步





# 暂时不用同步的表
dwd_ais_vessel_port_all_info
# 船舶出发到达港口全量 acquire_timestamp_format
# 数据量：69735040 最大时间：2024-03-01 05:14:12	 最小时间：2023-05-18 00:00:00
# 同步完成

dws_ais_vessel_port_status_info
# 船舶出发到达港口状态  acquire_timestamp_format
# 数据量：637450 最大时间：2024-03-01 05:14:12		 最小时间：2023-05-18 07:05:20
# 同步完成

dwd_aircraft_list_all_info
# 飞机全量数据表 acquire_timestamp_format
# 数据量：5770928371 最大时间：2024-02-01 20:35:24		 最小时间：2023-05-20 00:00:00
# 同步完成

dws_aircraft_list_status_info
# 飞机状态数据表 acquire_timestamp_format
# 数据量：51812252 最大时间：2024-02-01 20:35:24			 最小时间：2023-05-13 11:08:06
# 同步完成

dwd_satellite_all_info
# 卫星tle之前采集的 current_date   可以停止了
# 数据量：8368397 	最大时间：2024-04-07	最小时间：2023-05-13
# 2024-04-07 同步完成





