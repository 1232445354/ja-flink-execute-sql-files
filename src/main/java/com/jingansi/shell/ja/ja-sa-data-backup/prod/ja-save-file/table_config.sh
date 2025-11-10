#！/bin/bash

# 按天的数据文件
array_list_day=(
"dwd_bhv_satellite_rt update_time 1"                            # 卫星全量数据
"dws_bhv_satellite_list_fd today_time 1"                        # 卫星按天聚合表
"dwd_bhv_satellite_sense_info update_time 1"                    # 遥感融合表数据
"dwd_bhv_landsat_sense_list update_time 1"                      # landsat单独的遥感图
"dwd_bhv_airport_flight_rt acquire_time 1"                      # 出发到达航班
"dwd_bhv_airport_onground_rt acquire_time 1"                    # 地面航班
"dws_flight_segment_rt start_time 1"                            # 飞机航段表 - 自己计算的
"dws_bhv_airport_weather_info acquire_time 1"                   # 机场天气表
"dws_airport_liveatc_audio_files update_time 1"                 # 机场ATC音频文件表
"dws_bhv_aircraft_last_location_fd merge_time 2"                # 飞机天聚合 8907800
"dws_bhv_vessel_last_location_fd merge_time 1"                  # 船舶天聚合 2860932
"dws_bhv_aircraft_last_location_dh merge_time 1"                # 飞机按小时聚合表 2629326
"dws_bhv_vessel_last_location_dh merge_time 2"                  # 船舶按小时聚合表
"dwd_vessel_list_all_rt acquire_timestamp_format 2"             # marinetraffic单独表
"dwd_adsbexchange_aircraft_list_rt acquire_timestamp_format 4"  # adsbexchange单独表
#"dwd_ais_landbased_vessel_list acquire_time 1"                  # lb单独表
"dwd_fr24_grpc_aircraft_list_rt acquire_time 4"                 # f24单独表
)

# 按天 + 按小时
array_list_time_day=(
"dws_vessel_bhv_track_rt acquire_time 1 3600"                        # 船舶轨迹表
"dwd_bhv_aircraft_combine_rt acquire_time 1 3600"                    # 飞机轨迹表
"dwd_weather_indices_all weather_time 1 3600"                        # 气象数据表
"dwd_vt_vessel_all_info acquire_timestamp_format 4 7200"             # vt单独表
)

# 按月的数据文件
array_list_month=(
"dwd_gps_url_rt update_time 1"                            # gps
"dwd_weather_save_url_rt update_time 1"                   # 天气
"dwd_airport_liveatc_info update_time 1"                  # liveatc设施表"
"dws_et_aircraft_info update_time 1"                      # 飞机实体表
"dws_vessel_et_info_rt update_time 1"                     # 船舶实体表
"dws_bhv_aircraft_last_location_rt acquire_time 1"        # 飞机最后位置-新表、数据融合版本
"dws_vessel_bhv_status_rt acquire_time 1"                 # 船舶最后位置状态表
"dwd_et_sa_share_info update_time 1"                      # 分享态势数据"
"dws_et_satellite_info acquire_time 1"                    # 卫星实体表
"dws_atr_satellite_image_info update_time 1"              # 卫星图片表
"dwd_mtf_ship_info update_time 1"                         # marinetraffic单独的详情表
"dws_vessel_list_status_rt acquire_timestamp_format 1"    # marinetraffic的船舶状态数据单独
"dws_vt_vessel_status_info acquire_timestamp_format 1"    # vt的船舶状态数据单独
"dws_ais_landbased_vessel_status acquire_time 1"          # lb岸基数据的状态数据
"dws_airport_flight_info update_time 1"                   # 飞机起飞航班表 - 主搜航班
)


