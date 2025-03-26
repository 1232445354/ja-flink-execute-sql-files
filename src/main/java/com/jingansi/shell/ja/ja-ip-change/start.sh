#!/bin/bash

echo -en "检测当前jingan-ip是否变化中,$(date +"%Y-%m-%d %H:%M:%S")\n"
# 获取当前服务器的IP地址
current_ip=$(curl -s cip.cc | sed -n 's/^.*IP\s*:\s*\([0-9\.]*\).*$/\1/p')
echo -en "当前ip为：${current_ip}\n"

# 定义保存IP地址的文件
file="ip.txt"

# 钉钉告警接口
webhook_url="https://oapi.dingtalk.com/robot/send?access_token=f0041dd1738b0ce61b816ac53dac9798c1e3839614447f9db68d68b9325fb13b"

# 检查文件是否存在
if [ -f "$file" ]; then
    # 读取文件中的IP地址
    saved_ip=$(cat "$file")
    echo -en "上一次请求ip为：${saved_ip}\n"

    # 比较当前IP和文件中的IP
    if [ "$current_ip" == "$saved_ip" ]; then
        echo "ip一致，直接退出..."
    else
      echo "ip检测不一致,新ip写入文件,并告警.."
      # 保存当前IP到文件
      echo "$current_ip" > "$file"
      # 发送钉钉告警

      title="靖安-IP地址变化告警"
      content="服务器IP地址已从 $saved_ip 变更为 $current_ip"
      curl -s -o /dev/null -X POST "$webhook_url" \
        -H 'Content-Type: application/json' \
        -d "{\"msgtype\": \"text\", \"text\": {\"content\": \"$title: $content\"}}"
    fi

else
    echo "IP文件不存在，将创建新文件。"
fi

echo -en "$(date +"%Y-%m-%d %H:%M:%S")\n"
echo -en "------------------检测完成------------------\n"


