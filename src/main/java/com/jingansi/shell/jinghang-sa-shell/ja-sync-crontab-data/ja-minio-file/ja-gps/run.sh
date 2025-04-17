#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh

table_name="dwd_gps_url_rt"
time_column="file_time"
catalog_name="doris_idc"
per_day_cnt=0

echo -e "摆渡数据中，table_name =${table_name},开始time = $(date +"%Y-%m-%d %H:%M:%S")\n"

sql1="
insert into sa.${table_name}
select
  *
from ${catalog_name}.sa.${table_name}
where ${time_column} >= to_date(date_sub(now(),interval ${per_day_cnt} day));
"

echo -e "开始同步表数据\n"
mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql1}"


echo -e "开始同步minio数据\n"
sql2="

select
 distinct file_url
from ${catalog_name}.sa.${table_name}
where ${time_column} >= to_date(date_sub(now(),interval ${per_day_cnt} day))

"

query_result=$(mysql -h"$host" -P"$port" -u"$username" -p"$password" -N -e "${sql2}")

# 检查查询是否成功
if [ $? -ne 0 ]; then
    echo "数据库查询失败"
    exit 1
fi

# 检查是否获取到数据
if [ -z "$query_result" ]; then
    echo "没有查询到任何数据"
    exit 0
fi

# ========== 第二部分：将结果转换成数组并遍历 ==========
# 使用 while 循环逐行读取并填充数组
while IFS= read -r line; do
  echo -e "开始同步这条minio文件：${line}"
  gps_command="nohup rclone copy oss:/ja-acquire-images/${line} jh-minio:/ja-acquire-images/ja-gps/gpsjam2/ > root_rclone.log &"
  eval ${gps_command}
done <<< "$query_result"


echo -e "+++++++++++++++++++++++++++++++++++++++++++++++\n"
echo -e "执行SUCCESS------$(date +"%Y-%m-%d %H:%M:%S")\n"
echo -e "+++++++++++++++++++++++++++++++++++++++++++++++\n"
