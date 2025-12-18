#!/bin/bash


# ====== 使用方式 ======
# ./kill.sh                # 默认今天
# ./kill.sh 2025-11-23     # 指定日期
# =======================

if [ -z "$1" ]; then
    DATE=$(date '+%Y-%m-%d')
else
    DATE="$1"
fi

SESSION="run_${DATE}"

tmux has-session -t "$SESSION" 2>/dev/null
if [ $? != 0 ]; then
    echo "没有找到正在运行的 session: $SESSION"
    exit 1
fi

tmux kill-session -t "$SESSION"
echo "已停止：$SESSION"
