#!/bin/bash

# 获取当前脚本所在目录
SCRIPT_DIR=$(cd $(dirname $0); pwd)

echo -e "跨区程序开始执行.....$(date)\n"

# 检查并加载配置文件
CONFIG_FILE="${SCRIPT_DIR}/config.sh"
source "${CONFIG_FILE}"

# 检查SQL文件是否存在
SQL_FILE="${SCRIPT_DIR}/sql_file.sql"

# 使用kubectl进入容器并执行SQL
kubectl -n base exec -i mysql-0 -- \
    mysql -h "${host}" -P "${port}" -u "${username}" -p"${password}" < "${SQL_FILE}"

if [ $? -eq 0 ]; then
    echo "SQL执行成功!"
else
    echo "SQL执行失败!"
    exit 1
fi

echo -en "-------------------------------------------\n"
echo -en "执行SUCCESS.......$(date)\n"
echo -en "-------------------------------------------\n"
