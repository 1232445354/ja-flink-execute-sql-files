#!/bin/bash

DIR=$(cd `dirname $0`; pwd) # /home/jingansi/k8s/init/bigdata-apps
echo -e "当前文件目录：${DIR}"
CONTAINER_NAME="kafka-0"     # Kafka 容器的名称或ID

# 定义Topic数组
topicList=("jinghang_detection_result" "ja-ai-detection-output" "iot-device-message")

# 使用 for 循环遍历数组
for value in "${topicList[@]}"
do
  echo -e "创建Topic-->$value..."
  output=$(kubectl exec -it "${CONTAINER_NAME}" -c kafka -nbase -- sh -c "kafka-topics.sh --create --bootstrap-server kafka.base.svc.cluster.local:9092 --replication-factor 3 --partitions 1 --topic ${value} --if-not-exists ")

  # 过滤掉不需要的输出
  filtered_output=$(echo "$output" | grep -v -e "WARNING" -e "Defaulted container")
  echo "$filtered_output"
  if [ $? -eq 0 ]; then
      echo "Topic $value 创建成功！"
    else
      echo "Topic $value 创建失败！"
  fi
  echo -e "-------"
done

echo -e "topic create completed "
