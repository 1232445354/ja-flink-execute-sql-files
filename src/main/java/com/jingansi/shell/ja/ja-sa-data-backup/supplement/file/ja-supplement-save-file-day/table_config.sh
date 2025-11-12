#！/bin/bash

# 按天的数据文件
array_list_day=(

#"dwd_adsbexchange_aircraft_list_rt acquire_timestamp_format 6"  # adsbexchange单独表
#"dwd_vt_vessel_all_info acquire_timestamp_format 4"             # vt单独表
#"dwd_ais_landbased_vessel_list acquire_time 1"                  # lb单独表
#"dwd_fr24_aircraft_list_rt acquire_time 4"                      # f24单独表
#"dws_vessel_bhv_track_rt acquire_time 4"                        # 船舶轨迹表
#"dwd_bhv_aircraft_combine_rt acquire_time 6"                    # 飞机轨迹表
#"dwd_weather_indices_all weather_time 4"                        # 气象数据表
"dwd_fr24_grpc_aircraft_list_rt acquire_time 6"                   # f24单独表
)



