#!/bin/bash

# ====== 使用方式 ======
# ./start.sh                # 自动用今天的日期
# ./start.sh 2025-11-23     # 指定日期
# =======================
if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi

# =========== 2. 初始化 conda（消除 CondaError） ==========
source /home/wsw/miniconda3/etc/profile.d/conda.sh

# =========== 3. 激活环境 ==========
conda activate dev   # 改成环境名，如 quant

# 1) 如果没传日期，用今天日期
if [ -z "$1" ]; then
    DATE=$(date '+%Y-%m-%d')
else
    DATE="$1"
fi


SESSION="run_${DATE}"
LOG_DIR="logs/run"
mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/run_${DATE}_$(date '+%Y-%m-%d_%H-%M-%S').log"

# 2) 防止重复启动
tmux has-session -t "$SESSION" 2>/dev/null
if [ $? == 0 ]; then
    echo "Error: session '$SESSION' already running!"
    echo "➜ 停止任务请运行： ./kill.sh $DATE"
    echo "➜ 查看任务： tmux attach -t $SESSION"
    exit 1
fi

echo "Starting job: $DATE"
echo "Log file: $LOG_FILE"

# 3) 后台运行训练任务
tmux new-session -d -s "$SESSION" \
"python -m src.cli run $DATE 2>&1 | tee -a $LOG_FILE"

echo ""
echo "=============================="
echo "  已成功启动训练任务！"
echo "=============================="
echo "日志查看： tail -f $LOG_FILE"
echo "进入任务： tmux attach -t $SESSION"
echo ""
