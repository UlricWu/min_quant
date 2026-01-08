#!/bin/bash
# ==========================================
# kill.sh
#
# Responsibility:
#   - Stop MinQuant Flask API daemon
#   - ONLY kills tmux session
# ==========================================

SESSION="minquant_api"

tmux has-session -t "$SESSION" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "ℹ️  Flask API is not running (tmux session '$SESSION' not found)"
    exit 0
fi

echo "Stopping Flask API (tmux session: $SESSION)..."

tmux kill-session -t "$SESSION"

if [ $? -eq 0 ]; then
    echo "✅ Flask API stopped"
else
    echo "❌ Failed to stop Flask API"
    exit 1
fi
