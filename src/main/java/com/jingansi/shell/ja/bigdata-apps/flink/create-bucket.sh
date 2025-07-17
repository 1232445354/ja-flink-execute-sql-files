#!/bin/bash

DIR=$(cd `dirname $0`; pwd) # /home/jingansi/k8s/init/bigdata-apps
echo "当前文件目录：${DIR}"

# 1.获取minio的容器名称
CONTAINER_NAME=$(kubectl get pod -n minio --field-selector=status.phase=Running --no-headers -o custom-columns=":metadata.name" | head -n 1)
echo "获取到minio的容器名称：${CONTAINER_NAME}"   # MinIO 容器的名称或ID

# 设置相关变量
ALIAS="myminio"                     # mc 配置中的 MinIO 服务别名
MINIO_URL="http://minio.minio.svc.cluster.local:9000"     # MinIO 服务的 URL
ACCESS_KEY="minio"                                # MinIO 访问密钥
SECRET_KEY="jingansi110"                          # MinIO 私密密钥
BUCKET_NAME="flink"                               # MinIO 存储桶名称


# 1、创建mc和minio的连接
echo "Setting up MinIO connection in the container..."
kubectl exec -it "${CONTAINER_NAME}" -nminio -- sh -c " mc alias set "${ALIAS}" "${MINIO_URL}" "${ACCESS_KEY}" "${SECRET_KEY}" "

#3、在容器内运行 mc 命令上传文件到 MinIO
echo "Create MinIO bucket--flink"
kubectl exec -it "${CONTAINER_NAME}" -nminio -- sh -c " mc mb "${ALIAS}/${BUCKET_NAME}" "

echo "Bucket flink Create completed!"

