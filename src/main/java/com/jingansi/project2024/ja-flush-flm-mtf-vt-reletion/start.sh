#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
echo -en "开始备份互联网态势数据...$(date)\n"

echo -en "开始同步小表数据....\n"
# 当前：2024-02-21 05:30:00
# yesterday_start_day 上一天年月日:2024-02-20
# yesterday_end_day 上一天最后:2024-02-20 23:00:00
# today_day 当前天:2024-02-21
yesterday_start_day=$(date -d "yesterday" "+%Y-%m-%d")
yesterday_end_day=$(date -d "yesterday" "+%Y-%m-%d 23:00:00")
today_day=$(date "+%Y-%m-%d")
echo -en "${yesterday_start_day} + ${today_day}\n"
sh ${DIR}/sql_file1.sh "$yesterday_start_day" "$today_day"
echo -en "小表数据同步完成,sleep20s同步单表----$(date)\n"
sleep 20s



# 公共使用 | 将日期转换为时间戳,秒
date_format="+%Y-%m-%d %H:%M:%S"
start_timestamp=$(date -d "$yesterday_start_day" +%s)
end_timestamp=$(date -d "$yesterday_end_day" +%s)



echo -en "序号2----dwd_ais_vessel_all_info----$(date)\n"
interval_time2=21600
sleep_time2=40

# 遍历日期并输出
current_timestamp1="${start_timestamp}"
current_timestamp1_1=$((current_timestamp1 + ${interval_time2}))

while [ "$current_timestamp1" -le "$end_timestamp" ]; do
  current_day1=$(date -d "@$current_timestamp1" "$date_format")
  current_day1_1=$(date -d "@$current_timestamp1_1" "$date_format")

  echo -en "${current_day1} + ${current_day1_1}\n"
  sh ${DIR}/sql_file2.sh "$current_day1" "$current_day1_1"

  current_timestamp1=$((current_timestamp1 + ${interval_time2}))
  current_timestamp1_1=$((current_timestamp1_1 + ${interval_time2}))
  echo -en "sleep中 时间: ${sleep_time2}s...\n"
  sleep ${sleep_time2}s
done



echo -en "序号3----dws_aircraft_combine_list_rt----$(date)\n"
interval_time3=3600
sleep_time3=20

# 遍历日期并输出
current_timestamp3="${start_timestamp}"
current_timestamp3_1=$((current_timestamp3 + ${interval_time3}))

while [ "$current_timestamp3" -le "$end_timestamp" ]; do
  current_day3=$(date -d "@$current_timestamp3" "$date_format")
  current_day3_1=$(date -d "@$current_timestamp3_1" "$date_format")

  echo -en "${current_day3} + ${current_day3_1}\n"
  sh ${DIR}/sql_file3.sh "$current_day3" "$current_day3_1"

  current_timestamp3=$((current_timestamp3 + ${interval_time3}))
  current_timestamp3_1=$((current_timestamp3_1 + ${interval_time3}))
  echo -en "sleep中 时间: ${sleep_time3}s...\n"
  sleep ${sleep_time3}s
done



echo -en "序号4----dwd_vessel_list_all_rt----$(date)\n"
interval_time4=21600
sleep_time4=40

# 遍历日期并输出
current_timestamp4="${start_timestamp}"
current_timestamp4_1=$((current_timestamp4 + ${interval_time4}))

while [ "$current_timestamp4" -le "$end_timestamp" ]; do
  current_day4=$(date -d "@$current_timestamp4" "$date_format")
  current_day4_1=$(date -d "@$current_timestamp4_1" "$date_format")

  echo -en "${current_day4} + ${current_day4_1}\n"
  sh ${DIR}/sql_file4.sh "$current_day4" "$current_day4_1"

  current_timestamp4=$((current_timestamp4 + ${interval_time4}))
  current_timestamp4_1=$((current_timestamp4_1 + ${interval_time4}))
  echo -en "sleep中 时间: ${sleep_time4}s...\n"
  sleep ${sleep_time4}s
done



echo -en "序号5----dwd_adsbexchange_aircraft_list_rt----$(date)\n"
interval_time5=7200
sleep_time5=40

# 遍历日期并输出
current_timestamp5="${start_timestamp}"
current_timestamp5_1=$((current_timestamp5 + ${interval_time5}))

while [ "$current_timestamp5" -le "$end_timestamp" ]; do
  current_day5=$(date -d "@$current_timestamp5" "$date_format")
  current_day5_1=$(date -d "@$current_timestamp5_1" "$date_format")

  echo -en "${current_day5} + ${current_day5_1}\n"
  sh ${DIR}/sql_file5.sh "$current_day5" "$current_day5_1"

  current_timestamp5=$((current_timestamp5 + ${interval_time5}))
  current_timestamp5_1=$((current_timestamp5_1 + ${interval_time5}))
  echo -en "sleep中 时间: ${sleep_time5}s...\n"
  sleep ${sleep_time5}s
done



echo -en "序号6----dwd_vt_vessel_all_info----$(date)\n"
interval_time6=7200
sleep_time6=30

# 遍历日期并输出
current_timestamp6="${start_timestamp}"
current_timestamp6_1=$((current_timestamp6 + ${interval_time6}))

while [ "$current_timestamp6" -le "$end_timestamp" ]; do
  current_day6=$(date -d "@$current_timestamp6" "$date_format")
  current_day6_1=$(date -d "@$current_timestamp6_1" "$date_format")

  echo -en "${current_day6} + ${current_day6_1}\n"
  sh ${DIR}/sql_file6.sh "$current_day6" "$current_day6_1"

  current_timestamp6=$((current_timestamp6 + ${interval_time6}))
  current_timestamp6_1=$((current_timestamp6_1 + ${interval_time6}))
  echo -en "sleep中 时间: ${sleep_time6}s...\n"
  sleep ${sleep_time6}s
done



echo -en "--备份数据SUCCESS--------$(date)\n"
echo -en "----------------------------------\n"
