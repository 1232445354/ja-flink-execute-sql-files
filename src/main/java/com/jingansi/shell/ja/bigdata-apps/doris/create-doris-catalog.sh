#!/bin/bash

# 默认服务器IP
DEFAULT_IP="127.0.0.1"
DORIS_NAMESPACE="bigdata-doris"
DORIS_PASSWORD="Jingansi@110"
DORIS_QUERY_PORT=31030
MINIO_FILE_DATE="20250312"

# 获取Doris FE容器的名称
FE_CONTAINER=$(kubectl get pods -n ${DORIS_NAMESPACE} --no-headers -o custom-columns=":metadata.name" | grep -i "fe")
echo "DORIS的FE容器名称：${FE_CONTAINER}"

# 检查是否传入了IP参数，没有则使用默认值
if [ $# -ge 1 ]; then
    DORIS_FE_IP=$1
else
    DORIS_FE_IP=$DEFAULT_IP
    echo "未提供IP参数,使用默认IP: $SERVER_IP"
fi

SQL_STATEMENT="CREATE CATALOG mysql PROPERTIES (
\"user\" = \"root\",
\"use_meta_cache\" = \"false\",
\"type\" = \"jdbc\",
\"password\" = \"jingansi110\",
\"metadata_refresh_interval_sec\" = \"20\",
\"jdbc_url\" = \"jdbc:mysql://${DORIS_FE_IP}:31306/dushu?useSSL=false&yearIsDateType=false&tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&rewriteBatchedStatements=true\",
\"driver_url\" = \"http://${DORIS_FE_IP}:32205/storage/ja-resource/java/${MINIO_FILE_DATE}/mysql-connector-java-8.0.25.jar\",
\"driver_class\" = \"com.mysql.cj.jdbc.Driver\",
\"create_time\" = \"2025-04-22 16:35:28.731\",
\"checksum\" = \"fdf55dcef04b09f2eaf42b75e61ccc9a\"
);"

kubectl exec -it -n $DORIS_NAMESPACE $FE_CONTAINER -- mysql -h$DORIS_FE_IP -uroot -P$DORIS_QUERY_PORT -p$DORIS_PASSWORD -e "$SQL_STATEMENT"

# 检查执行结果
if [ $? -eq 0 ]; then
    echo "SQL executed successfully in Doris FE"
else
    echo "Failed to execute SQL in Doris FE"
    exit 1
fi