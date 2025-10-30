#！/bin/bash

# 小表
# table_name、time_column、front_day、after_day
declare -a small_table_infos=(
#"dwd_gps_url_rt update_time 1 0"                            # gps
#"dwd_weather_save_url_rt update_time 1 0"                   # 天气
#"dwd_et_sa_share_info update_time 0 0"                      # 分享态势数据
#"dwd_mtf_ship_info update_time 1 0"                         # marinetraffic单独的详情表
#"dws_vessel_list_status_rt acquire_timestamp_format 0 0"    # marinetraffic的船舶状态数据单独
#"dws_vt_vessel_status_info acquire_timestamp_format 0 0"    # vt的船舶状态数据单独
#"dws_ais_landbased_vessel_status acquire_time 0 0"          # lb岸基数据的状态数据
#"dwd_bhv_satellite_rt update_time 0 0"                      # 卫星全量数据
#"dws_bhv_satellite_list_fd today_time 0 0"                  # 卫星按天聚合表
#"dwd_bhv_satellite_sense_info update_time 0 0"              # 遥感融合表数据
#"dwd_bhv_landsat_sense_list update_time 0 0"                # landsat单独的遥感图
#"dws_airport_flight_info update_time 0 0"                   # 飞机起飞航班表 - 主搜航班
#"dwd_bhv_airport_flight_rt acquire_time 0 0"                # 出发到达航班
#"dwd_bhv_airport_onground_rt acquire_time 0 0"              # 地面航班
#"dws_et_satellite_info acquire_time 0 0"                    # 卫星实体表
#"dws_atr_satellite_image_info update_time 1 0"              # 卫星图片表
#"dws_et_aircraft_info update_time 0 0"                      # 飞机实体表
#"dws_vessel_et_info_rt update_time 0 0"                     # 船舶实体表
#"dws_bhv_aircraft_last_location_rt acquire_time 0 0"        # 飞机最后位置-新表、数据融合版本
#"dws_vessel_bhv_status_rt acquire_time 0 0"                 # 船舶最后位置状态表
#"dws_flight_segment_rt start_time 0 0"                      # 飞机航段表 - 自己计算的
#"dws_bhv_airport_weather_info acquire_time 0 0"             # 机场天气表
#"dws_airport_liveatc_audio_files update_time 0 0"           #机场ATC音频文件表
#"dwd_airport_liveatc_info update_time 0 0"                  #liveatc设施表"
)

# 聚合表
declare -a table_infos=(
#"dws_bhv_aircraft_last_location_fd merge_time"
#"dws_bhv_vessel_last_location_fd merge_time"
)

# 表名称、时间字段、同一个表不同时间之间的间隔、每次同步几小时(s)
arrayList=(
#"dws_bhv_aircraft_last_location_dh merge_time 3 14400"                # 4 hour   飞机按小时聚合表
#"dws_bhv_vessel_last_location_dh merge_time 3 14400"                  # 4 hour    船舶按小时聚合表
#"dwd_vessel_list_all_rt acquire_timestamp_format 5 7200"              # 2 hour    marinetraffic单独表
"dwd_adsbexchange_aircraft_list_rt acquire_timestamp_format 10 600"  # 1 hour    adsbexchange单独表
"dwd_vt_vessel_all_info acquire_timestamp_format 5 3600"              # 1 hour    vt单独表
"dwd_ais_landbased_vessel_list acquire_time 5 14400"                  # 4 hour    lb单独表
"dwd_fr24_aircraft_list_rt acquire_time 5 3600"                       # 1 hour    f24单独表
)