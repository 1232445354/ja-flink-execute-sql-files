#!/bin/bash

catalog_name=${1}
DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
# 船舶、飞机天聚合
sql="
drop catalog ${catalog_name};

CREATE CATALOG ${catalog_name} PROPERTIES (
\"user\" = \"root\",
\"use_meta_cache\" = \"false\",
\"type\" = \"jdbc\",
\"password\" = \"Jingansi@110\",
\"metadata_refresh_interval_sec\" = \"86400\",
\"jdbc_url\" = \"jdbc:mysql://115.231.236.108:9030/sa?useUnicode=true&characterEncoding=utf8&useSSL=false&yearIsDateType=false&tinyInt1isBit=false&characterEncoding=utf-8&rewriteBatchedStatements=true\",
\"driver_url\" = \"http://172.21.30.201:32205/storage/ja-resource/java/20231025/mysql-connector-java-8.0.25.jar\",
\"driver_class\" = \"com.mysql.jdbc.Driver\",
\"create_time\" = \"2025-09-04 17:40:20.660\",
\"checksum\" = \"fdf55dcef04b09f2eaf42b75e61ccc9a\"
);

"

mysql -h${host} \
  -P${port} \
  -u${username} \
  -p${password} \
  -e "${sql}"

