#!/bin/bash
current_dir=$(pwd)
echo "当前文件目录：${current_dir}"

CRON_JOB="*/10 * * * * sh ${current_dir}/delete-error-pod.sh > ${current_dir}/error-pod-root.log 2>&1"

check_existing_cronjob() {
    # 精确匹配整行配置（包括重定向）
    if crontab -l 2>/dev/null | grep -Fxq "$CRON_JOB"; then
        existing=$(crontab -l | grep -F "$CRON_JOB")
        echo "已存在完全相同的定时任务："
        echo "$existing"
        return 0
    fi

    # 检查是否有相同脚本但配置不同的任务
    if crontab -l 2>/dev/null | grep -q "bigdata-apps/delete-error-pod.sh"; then
        existing=$(crontab -l | grep "bigdata-apps/delete-error-pod.sh")
        echo "发现相似但不完全相同的任务："
        echo "$existing"
        echo "（建议手动确认是否需要更新）"
        return 0
    fi

    return 1
}

add_cronjob() {
    # 备份现有crontab
    backup_file="/tmp/crontab_backup"
    crontab -l > "$backup_file" 2>/dev/null
    echo "已备份当前crontab到 $backup_file"

    # 添加新任务
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

    # 验证是否添加成功
    if crontab -l | grep -Fxq "$CRON_JOB"; then
        echo "✅ 新任务添加成功："
        crontab -l | grep -F "$CRON_JOB"
    else
        echo "❌ 任务添加失败，正在恢复备份..."
        if [ -f "$backup_file" ]; then
            cat "$backup_file" | crontab -
            echo "已从备份恢复原始crontab"
        fi
        exit 1
    fi
}

# 主流程
echo "正在检查定时任务状态..."
if check_existing_cronjob; then
    echo "无需执行添加操作"
else
    echo "未发现相同配置，准备添加任务..."
    add_cronjob
fi

# 显示最终crontab内容（可选）
echo -e "\n当前用户的crontab内容:"
crontab -l