#！/bin/bash

# 按天的数据文件
array_list_day=(
"dwd_bhv_satellite_rt update_time 1"                            # 卫星全量数据
#"dws_bhv_satellite_list_fd today_time 1"                        # 卫星按天聚合表
#"dwd_bhv_satellite_sense_info update_time 1"                    # 遥感融合表数据
#"dwd_bhv_landsat_sense_list update_time 1"                      # landsat单独的遥感图
#"dwd_bhv_airport_flight_rt acquire_time 1"                      # 出发到达航班
#"dwd_bhv_airport_onground_rt acquire_time 1"                    # 地面航班
#"dws_flight_segment_rt start_time 1"                            # 飞机航段表 - 自己计算的
#"dws_bhv_airport_weather_info acquire_time 1"                   # 机场天气表
#"dws_airport_liveatc_audio_files update_time 1"                 # 机场ATC音频文件表
#"dws_bhv_aircraft_last_location_fd merge_time 2"                # 飞机天聚合
#"dws_bhv_vessel_last_location_fd merge_time 2"                  # 船舶天聚合
#"dws_bhv_aircraft_last_location_dh merge_time 2"                # 飞机按小时聚合表
#"dws_bhv_vessel_last_location_dh merge_time 2"                  # 船舶按小时聚合表
#"dwd_vessel_list_all_rt acquire_timestamp_format 2"             # marinetraffic单独表
#"dwd_adsbexchange_aircraft_list_rt acquire_timestamp_format 6"  # adsbexchange单独表
#"dwd_vt_vessel_all_info acquire_timestamp_format 4"             # vt单独表
#"dwd_ais_landbased_vessel_list acquire_time 1"                  # lb单独表
#"dwd_fr24_aircraft_list_rt acquire_time 4"                      # f24单独表
#"dws_vessel_bhv_track_rt acquire_time 4"                        # 船舶轨迹表
#"dwd_bhv_aircraft_combine_rt acquire_time 6"                    # 飞机轨迹表
#"dwd_weather_indices_all weather_time 4"                        # 气象数据表
)




