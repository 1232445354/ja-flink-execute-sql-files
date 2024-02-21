#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
echo -en "开始备份数据...$(date)\n"

echo -en "开始同步小表数据....\n"
# 当前：2024-02-21 05:30:00
# yesterday_start_day 上一天年月日:2024-02-20
# yesterday_start_day 上一天最后:2024-02-20 23:00:00
# today_day 当前天:2024-02-21
yesterday_start_day=$(date -d "yesterday" "+%Y-%m-%d")
yesterday_end_day=$(date -d "yesterday" "+%Y-%m-%d 23:00:00")
today_day=$(date "+%Y-%m-%d")
echo -en "${yesterday_start_day} + ${today_day}\n"
sh ${DIR}/sql_file1.sh "$yesterday_start_day" "$today_day"
echo -en "小表数据同步完成,sleep20s同步单表----$(date)\n"
sleep 20s


# 公共使用
date_format="+%Y-%m-%d %H:%M:%S"
# 将日期转换为时间戳,秒
start_timestamp=$(date -d "$yesterday_start_day" +%s)
end_timestamp=$(date -d "$yesterday_end_day" +%s)



echo -en "同步dwd_ais_vessel_all_info表中...$(date)\n"
# 遍历日期并输出
current_timestamp1="${start_timestamp}"
current_timestamp2=$((current_timestamp1 + 21600))

while [ "$current_timestamp1" -le "$end_timestamp" ]; do
  current_day1=$(date -d "@$current_timestamp1" "$date_format")
  current_day2=$(date -d "@$current_timestamp2" "$date_format")

  echo -en "${current_day1} + ${current_day2}\n"
  sh ${DIR}/sql_file2.sh "$current_day1" "$current_day2"

  current_timestamp1=$((current_timestamp1 + 21600))
  current_timestamp2=$((current_timestamp2 + 21600))
  echo -en "sleep中...\n"
  sleep 10s
done



echo -en "同步dwd_vessel_list_all_rt表中...$(date)\n"
# 遍历日期并输出
current_timestamp3="${start_timestamp}"
current_timestamp4=$((current_timestamp3 + 21600))

while [ "$current_timestamp3" -le "$end_timestamp" ]; do
  current_day3=$(date -d "@$current_timestamp3" "$date_format")
  current_day4=$(date -d "@$current_timestamp4" "$date_format")

  echo -en "${current_day3} + ${current_day4}\n"
  sh ${DIR}/sql_file3.sh "$current_day3" "$current_day4"

  current_timestamp3=$((current_timestamp3 + 21600))
  current_timestamp4=$((current_timestamp4 + 21600))
  echo -en "sleep中...\n"
  sleep 10s
done



echo -en "同步dwd_adsbexchange_aircraft_list_rt表中...$(date)\n"
# 遍历日期并输出
current_timestamp5="${start_timestamp}"
current_timestamp6=$((current_timestamp5 + 7200))

while [ "$current_timestamp5" -le "$end_timestamp" ]; do
  current_day5=$(date -d "@$current_timestamp5" "$date_format")
  current_day6=$(date -d "@$current_timestamp6" "$date_format")

  echo -en "${current_day5} + ${current_day6}\n"
  sh ${DIR}/sql_file4.sh "$current_day5" "$current_day6"

  current_timestamp5=$((current_timestamp5 + 7200))
  current_timestamp6=$((current_timestamp6 + 7200))
  echo -en "sleep中...\n"
  sleep 10s
done



echo -en "同步dws_aircraft_combine_list_rt表中...$(date)\n"
# 遍历日期并输出
current_timestamp7="${start_timestamp}"
current_timestamp8=$((current_timestamp7 + 3600))

while [ "$current_timestamp7" -le "$end_timestamp" ]; do
  current_day7=$(date -d "@$current_timestamp7" "$date_format")
  current_day8=$(date -d "@$current_timestamp8" "$date_format")

  echo -en "${current_day7} + ${current_day8}\n"
  sh ${DIR}/sql_file5.sh "$current_day7" "$current_day8"

  current_timestamp7=$((current_timestamp7 + 3600))
  current_timestamp8=$((current_timestamp8 + 3600))
  echo -en "sleep中...\n"
  sleep 20s
done


echo -en "--备份数据SUCCESS--------$(date)\n"
echo -en "----------------------------------\n"
