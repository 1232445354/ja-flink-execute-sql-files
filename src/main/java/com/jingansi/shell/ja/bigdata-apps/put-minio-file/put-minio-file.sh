#!/bin/bash

DIR=$(cd `dirname $0`; pwd) # /home/jingansi/k8s/init/bigdata-apps
echo "当前文件目录：${DIR}"

# 设置相关变量
JAR_FILE_PATH="${DIR}/geo-udf-1.0-SNAPSHOT-jar-with-dependencies.jar" # 宿主机上的 .jar 文件路径
CONTAINER_NAME="minio-0"            # MinIO 容器的名称或ID
CONTAINER_DEST_PATH="/opt/bitnami/minio-client/bin/geo-udf-1.0-SNAPSHOT-jar-with-dependencies.jar"  # 容器内存放文件的路径
ALIAS="myminio"                     # mc 配置中的 MinIO 服务别名
MINIO_URL="http://minio.minio.svc.cluster.local:9000"     # MinIO 服务的 URL
ACCESS_KEY="minio"                                # MinIO 访问密钥
SECRET_KEY="jingansi110"                          # MinIO 私密密钥
BUCKET_NAME="ja-resource"                         # MinIO 存储桶名称
OBJECT_PATH="java/20241218/"


# 1、将 .jar 文件从宿主机复制到 MinIO 容器内
echo "Copying ${JAR_FILE_PATH} to MinIO container..."
kubectl -n minio cp ${JAR_FILE_PATH} ${CONTAINER_NAME}:${CONTAINER_DEST_PATH}

# 2、创建mc和minio的连接
echo "Setting up MinIO connection in the container..."
kubectl exec -it "${CONTAINER_NAME}" -nminio -- sh -c " mc alias set "${ALIAS}" "${MINIO_URL}" "${ACCESS_KEY}" "${SECRET_KEY}" "


#3、在容器内运行 mc 命令上传文件到 MinIO
echo "Uploading ${CONTAINER_DEST_PATH} to MinIO bucket ${BUCKET_NAME}..."
kubectl exec -it "${CONTAINER_NAME}" -nminio -- sh -c " mc cp "${CONTAINER_DEST_PATH}" "${ALIAS}/${BUCKET_NAME}/${OBJECT_PATH}" "

#4、删除容器内文件
echo "Cleaning up container's file..."
kubectl exec -it "${CONTAINER_NAME}" -nminio -- sh -c " rm -rf ${CONTAINER_DEST_PATH} "

echo "File upload completed!"

