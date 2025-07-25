
#!/bin/bash

# 目标IP地址
TARGET_BACKEND="135.100.11.110"
DECOMMISSION_BACKEND="135.100.11.110:31050"

# 进入mysql-0容器并执行MySQL命令，然后筛选结果
result=$(kubectl exec -it mysql-0 -n base -- mysql -h135.100.11.110 -P31030 -uroot -pJingansi@110 -e "show backends" 2>&1)

echo "检查后端节点列表..."
echo "$result"


# 检查是否包含目标IP（完全匹配）
if echo "$result" | grep -q -w "$TARGET_BACKEND"; then
    echo -e "\n找到完全匹配 $TARGET_BACKEND 的数据，开始下线后端 $DECOMMISSION_BACKEND..."
    echo "执行下节点操作"
    kubectl exec -it mysql-0 -n base -- mysql -h172.21.30.105 -P31030 -uroot -pJingansi@110 -e "ALTER SYSTEM DECOMMISSION BACKEND \"$DECOMMISSION_BACKEND\";"
    echo -e "\n下线命令已执行，请检查状态："
else
    echo -e "\n未找到完全匹配 $TARGET_BACKEND 的数据，不执行下线操作"
fi