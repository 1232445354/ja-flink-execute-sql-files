#！/bin/bash

# 表名称、时间字段、同一个表不同时间之间的间隔、每次同步几小时(s)
arrayList=(
"dwd_bhv_aircraft_combine_rt acquire_time 10 600"   # 10 minute
"dwd_adsbexchange_aircraft_list_rt acquire_timestamp_format 10 600"   # 1 hour    adsbexchange单独表
)

weather_list=(
"dwd_weather_indices_all weather_time"
)
