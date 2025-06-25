#!/bin/bash

# 默认服务器IP
DEFAULT_IP="127.0.0.1"
MYSQL_CONTAINER_NAME="mysql-0"
MYSQL_NAMESPACE="base"
NEW_PASSWORD="Jingansi@110"
DORIS_QUERY_PORT=31030

# 检查是否传入了IP参数，没有则使用默认值
if [ $# -ge 1 ]; then
    SERVER_IP=$1
else
    SERVER_IP=$DEFAULT_IP
    echo "未提供IP参数,使用默认IP: $SERVER_IP"
fi

# 进入容器并执行MySQL命令
kubectl exec -it -n $MYSQL_NAMESPACE $MYSQL_CONTAINER_NAME -- bash -c "
    echo '正在连接到DORIS服务器 $SERVER_IP:$DORIS_QUERY_PORT...'
    echo '修改密码并校准时区中...'
    mysql -h $SERVER_IP -P$DORIS_QUERY_PORT -uroot -e \"
      SET PASSWORD FOR 'root' = PASSWORD('Jingansi@110');
      SET PASSWORD FOR 'admin' = PASSWORD('Jingansi@110');
      SET GLOBAL time_zone = 'Asia/Shanghai';
    \"

    if [ \$? -eq 0 ]; then
        echo -e '\n[执行结果]'
        echo '√ root密码已修改为: $NEW_PASSWORD'
        echo '√ admin密码已修改为: $NEW_PASSWORD'
        echo '√ 服务器时区已设置为: Asia/Shanghai'
        echo '√ 所有操作已完成'
    else
        echo -e '\n[错误] 命令执行失败'
        exit 1
    fi
"