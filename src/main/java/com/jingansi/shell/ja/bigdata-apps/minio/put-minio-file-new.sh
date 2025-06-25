#!/bin/bash

DIR=$(cd `dirname $0`; pwd)       # /home/jingansi/k8s/init/bigdata-apps
echo "当前文件目录：${DIR}"

# 1.获取minio的容器名称
CONTAINER_NAME=$(kubectl get pod -n minio --field-selector=status.phase=Running --no-headers -o custom-columns=":metadata.name" | head -n 1)
echo "获取到minio的容器名称：${CONTAINER_NAME}"   # MinIO 容器的名称或ID

# 2.设置相关变量
CONTAINER_PRE_PATH="/opt/bitnami/minio-client/bin"    # 容器内存放文件的路径,先上传
ALIAS="myminio"                                       # mc 配置中的 MinIO 服务别名
MINIO_URL="http://minio.minio.svc.cluster.local:9000" # MinIO 服务的 URL
ACCESS_KEY="minio"                                    # MinIO 访问密钥
SECRET_KEY="jingansi110"                              # MinIO 私密密钥
BUCKET_NAME="ja-resource"                             # MinIO 存储桶名称

# 3.需要上传的jar
JAR_FILES=(
"geo-udf-1.0-SNAPSHOT-jar-with-dependencies.jar java/20241218"
"mysql-connector-java-8.0.25.jar java/20250312"
)

# 4.创建minio容器内mc的连接
echo "Setting up MinIO connection in the container..."
kubectl exec -it "${CONTAINER_NAME}" -nminio -- sh -c " mc alias set "${ALIAS}" "${MINIO_URL}" "${ACCESS_KEY}" "${SECRET_KEY}" "

# 5.遍历jar数组上传
for JAR_INFO in "${JAR_FILES[@]}"; do
  # 使用 awk 或 read 分割数组元素（文件名 和 MinIO路径）
  JAR_FILE=$(echo "$JAR_INFO" | awk '{print $1}')
  OBJECT_PATH=$(echo "$JAR_INFO" | awk '{print $2}')
  echo "Processing ${JAR_FILE} -> MinIO Path: ${OBJECT_PATH}..."

  JAR_FILE_PATH="${DIR}/${JAR_FILE}"
  CONTAINER_DEST_PATH="${CONTAINER_PRE_PATH}/${JAR_FILE}"

  echo "Copying ${JAR_FILE_PATH} to MinIO container..."
  kubectl -n minio cp ${JAR_FILE_PATH} ${CONTAINER_NAME}:${CONTAINER_DEST_PATH}

  echo "Uploading ${JAR_FILE} to MinIO bucket ${BUCKET_NAME}/${OBJECT_PATH}..."
  kubectl exec -it "${CONTAINER_NAME}" -n minio -- sh -c "mc cp ${CONTAINER_DEST_PATH} ${ALIAS}/${BUCKET_NAME}/${OBJECT_PATH}/"

  echo "Cleaning up container's file..."
  kubectl exec -it "${CONTAINER_NAME}" -nminio -- sh -c " rm -rf ${CONTAINER_DEST_PATH} "

  echo "------------------------------------------------------"
done

echo -en "================================================================\n"
echo "File upload completed!"
echo -en "================================================================\n"


