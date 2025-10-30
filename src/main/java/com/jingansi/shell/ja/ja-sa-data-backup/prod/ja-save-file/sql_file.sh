#!/bin/bash

table_name=${1}
time_column=${2}
start_time=${3}
end_time=${4}
type=${5}
start_time_y=${6}
start_time_ymd=${7}
parallelism=$8

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh

# day
sql_day="

EXPORT TABLE ${table_name}
where ${time_column} >= '${start_time}'
  and ${time_column} < '${end_time}'
TO \"s3://${S3_BUCKET}/${table_name}/${start_time_y}/${start_time_ymd}/\"
PROPERTIES (
	\"format\" = \"${S3_FORMAT}\",
	\"max_file_size\"=\"${S3_MAX_FILE_SIZE}\",
  \"parallelism\"=\"${parallelism}\"
) WITH S3 (
  \"s3.endpoint\" = \"${S3_ENDPOINT}\",
  \"s3.secret_key\"=\"${S3_SECRET}\",
  \"s3.access_key\" = \"${S3_ACCESS_KEY}\",
	\"use_path_style\" = \"true\"
)

"


sql_month="

EXPORT TABLE ${table_name}
TO \"s3://${S3_BUCKET}/${table_name}/${start_time_y}/${start_time_ymd}/\"
PROPERTIES (
	\"format\" = \"${S3_FORMAT}\",
	\"max_file_size\"=\"${S3_MAX_FILE_SIZE}\",
  \"parallelism\"=\"${parallelism}\"
) WITH S3 (
  \"s3.endpoint\" = \"${S3_ENDPOINT}\",
  \"s3.secret_key\"=\"${S3_SECRET}\",
  \"s3.access_key\" = \"${S3_ACCESS_KEY}\",
	\"use_path_style\" = \"true\"
)

"


if [ "$type" = "day" ]; then
  mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql_day}"
elif [ "$type" = "month" ]; then
  mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql_month}"
fi


