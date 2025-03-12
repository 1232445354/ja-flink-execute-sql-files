#!/bin/bash

DIR=$(cd `dirname $0`; pwd)  # /home/jingansi/k8s/init/bigdata-apps
echo -e "当前文件目录：${DIR}"
CONTAINER_NAME="kafka-0"     # Kafka 容器的名称或ID

# 定义Topic数组
topicList=(
"jinghang_detection_result"   # 快出易培的图片
"ja-ai-detection-output"      # ai检测数据-望楼的可见光红外-重庆南岸
"iot-device-message"          # 无人机日志-雷达
"ja-detection-output"         # 209检测事件数据
)

# 使用 for 循环遍历数组
for value in "${topicList[@]}"
do
  echo -e "创建Topic-->$value..."
  output=$(kubectl exec "${CONTAINER_NAME}" -c kafka -nbase -- sh -c "kafka-topics.sh --create --bootstrap-server kafka.base.svc.cluster.local:9092 --replication-factor 3 --partitions 1 --topic ${value} --if-not-exists ")

  # 过滤掉不需要的输出
  filtered_output=$(echo "$output" | grep -v -e "WARNING" -e "Defaulted container")
#  echo "$filtered_output"
  if [ -z "$filtered_output" ]; then
      echo "Topic $value 创建成功！"
    else
      echo "Topic $value 创建失败！错误信息：$output"
  fi
  echo -e "-------"
done

echo -e "Topic Create Completed!!"
