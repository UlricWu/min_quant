#!/bin/bash
# ==========================================
# start.sh
#   - Start Flask API as a long-running daemon
# ==========================================

if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi

# -------- 1. Conda --------
source /home/wsw/miniconda3/etc/profile.d/conda.sh
conda activate dev

# -------- 2. Env --------
export PYTHONPATH=$(pwd)

SESSION="minquant_api"
LOG_DIR="logs/api"
mkdir -p "$LOG_DIR"

tmux has-session -t "$SESSION" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "❌ Flask API already running (tmux: $SESSION)"
    exit 1
fi

LOG_FILE="$LOG_DIR/api_$(date '+%Y-%m-%d_%H-%M-%S').log"

# -------- 3. Start Flask (python -m, 最稳) --------
tmux new-session -d -s "$SESSION" \
"python -m src.api.app 2>&1 | tee -a $LOG_FILE"

echo ""
echo "========================================"
echo "✅ MinQuant Flask API started"
echo "========================================"
echo "Session : $SESSION"
echo "Log     : $LOG_FILE"
echo "Attach  : tmux attach -t $SESSION"
echo ""
