
#!/bin/bash
# 降序
# start_day="2023-10-20 00:00:00"
# end_day="2023-11-29 00:00:00"

# start_day="2023-10-01 00:00:00"
# end_day="2023-11-29 00:00:00"
# date_format="+%Y-%m-%d %H:%M:%S"

# echo -en "开始刷新数据...$(date)"

# DIR=$(cd `dirname $0`; pwd)

# # 将日期转换为时间戳
# start_timestamp=$(date -d "$start_day" +%s)
# end_timestamp=$(date -d "$end_day" +%s)

# # 遍历日期并输出
# current_timestamp1="$end_timestamp"
# current_timestamp2=$((current_timestamp1 - 3600))


# while [ "$current_timestamp2" -ge "$start_timestamp" ]; do
#   current_day1=$(date -d "@$current_timestamp1" "$date_format")
#   current_day2=$(date -d "@$current_timestamp2" "$date_format")

#   echo  "$current_day2" + "$current_day1"

#   sh ${DIR}/sql_file.sh "$current_day2" "$current_day1"

#   current_timestamp1=$((current_timestamp1 - 3600))
#   current_timestamp2=$((current_timestamp2 - 3600))
#   echo -en "sleep中.......\n"
#   sleep 30s
# done

# echo -en "备份数据SUCCESS.......$(date)"
# echo -en "-----------------------"





#!/bin/bash
# 升序
# start_day="2023-10-20 00:00:00"
# end_day="2023-11-29 00:00:00"

start_day="2023-11-05 00:00:00"
end_day="2023-11-12 23:59:59"
date_format="+%Y-%m-%d %H:%M:%S"

echo -en "开始刷新数据...$(date)\n"

DIR=$(cd `dirname $0`; pwd)

# 将日期转换为时间戳
start_timestamp=$(date -d "$start_day" +%s)
end_timestamp=$(date -d "$end_day" +%s)

# 遍历日期并输出
current_timestamp1="$start_timestamp"
current_timestamp2=$((current_timestamp1 + 3600))


while [ "$current_timestamp1" -le "$end_timestamp" ]; do
  current_day1=$(date -d "@$current_timestamp1" "$date_format")
  current_day2=$(date -d "@$current_timestamp2" "$date_format")

  echo  -en "$current_day1" + "$current_day2\n"

  sh ${DIR}/sql_file.sh "$current_day1" "$current_day2"

  current_timestamp1=$((current_timestamp1 + 3600))
  current_timestamp2=$((current_timestamp2 + 3600))
  echo -en "sleep中.......\n"
  sleep 5s
done

echo -en "备份数据SUCCESS.......$(date)\n"
echo -en "-----------------------"