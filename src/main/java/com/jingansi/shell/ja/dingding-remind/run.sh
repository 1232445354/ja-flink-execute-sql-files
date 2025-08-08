#!/bin/bash

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 值班人员配置
CONFIG_DUTY="${SCRIPT_DIR}/config-duty.sh"
# 值班人员记录
VALUE_DUTY="${SCRIPT_DIR}/value-duty.sh"

# 周会人员配置
CONFIG_WEEK="${SCRIPT_DIR}/config-week.sh"
# 周会人员记录
VALUE_WEEK="${SCRIPT_DIR}/value-week.sh"


duty_res=""
week_res=""

read_data() {
  local config_file=$1
  local value_file=$2
  local type=$3
  local cnt=$4
  # 检查config.sh文件是否存在
  if [ ! -f "$config_file" ]; then
    echo "错误:${config_file}文件不存在!"
    exit 1
  fi

  echo "正在读取配置文件: ${config_file}"
  declare -i current_number
  # 读取读取当前值班编号
  current_number=$(head -n 1 "$value_file" | awk '{$1=$1};1')
  echo "当前值班序号: $current_number"
  if [ -z "$current_number" ]; then
    next_number=0
  else
    next_number=$(( (current_number + 1) % $cnt ))
  fi
  # 将下周值班人员写入文件
  # sed -i "s/^current_value=.*/current_value=$next_number/" ${value_file}
  echo "下周值班序号: ${next_number}"

  line_num=0
  while IFS= read -r line || [ -n "$line" ]; do
    # 跳过空行
    if [ -z "$line" ]; then
        continue
    fi
    # 行号计数
    line_num=$((line_num + 1))
    # 打印行号和内容
    id=$(echo "$line" | awk -F' ' '{print $1}')

    if [ "$id" -eq "$next_number" ]; then
      break
    fi
  done < "${config_file}"

  if [ "duty" = "$type" ]; then
    duty_res=${line}
  else 
    week_res=${line}
  fi 
  echo "${next_number}" > ${value_file}
  echo "---------"
}

read_data "$CONFIG_DUTY" "$VALUE_DUTY" "duty" 4

read_data "$CONFIG_WEEK" "$VALUE_WEEK" "week" 16


IFS=' ' read -ra duty_line <<< "$duty_res"
name1=${duty_line[1]}
name2=${duty_line[2]}

IFS=' ' read -ra week_line <<< "$week_res"
name3=${week_line[1]}

echo "本周值班人员：${duty_res}  name1:${name1} name2:${name2}"
echo "本周周会主持人员：${week_res} name3:${name3}"



declare -a webhook_url_ranges=(
"研发群 https://oapi.dingtalk.com/robot/send?access_token=37548884da2fbf2883a5b2984193e0f8ef1116d3b4826d23b6658b9af12abae4"
)



# 定义发送函数
send_ding_msg() {
  local i=$1
  local type=$2
  local webhook_url=$3
  local name1=$4
  local name2=$5
  local name3=$6
  if [ "研发群" = "$type" ]; then
    json_data=$(cat <<EOF
      {
        "msgtype": "text",
        "text": {
          "content": "工作提醒通知${i},下周值班人员:@${name1},@${name2}\n周会主持人员@${name3}"
        },
        "at": {
          "atMobiles": ["${name1}", "${name2}", "${name3}"], 
          "isAtAll": false
        }
      }
EOF
    )
    
    curl "$webhook_url" \
    -H 'Content-Type: application/json' \
    -d "$json_data"
  else
    json_data=$(cat <<EOF
      {
        "msgtype": "text",
        "text": {
          "content": "工作提醒通知${i},下周值班人员:@${name1},@${name2}"
        },
        "at": {
          "atMobiles": ["${name1}", "${name2}"], 
          "isAtAll": false
        }
      }
EOF
    )
    
    curl "$webhook_url" \
    -H 'Content-Type: application/json' \
    -d "$json_data"
  fi
}



for webhook_url_range in "${webhook_url_ranges[@]}"; do
  IFS=' ' read -ra webhook_info <<< "$webhook_url_range"
  comment=${webhook_info[0]}
  webhook_url=${webhook_info[1]}
  for i in {1..3}; do
    send_ding_msg "$i" "${comment}" "${webhook_url}" "${name1}" "${name2}" "${name3}"

  done
done



