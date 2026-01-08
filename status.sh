#!/bin/bash
# ==========================================
# status.sh
#
# Responsibility:
#   - Check whether Flask API daemon is running
#   - Read-only (NO side effects)
# ==========================================

SESSION="minquant_api"

tmux has-session -t "$SESSION" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ Flask API is RUNNING"
    echo "   tmux session : $SESSION"
    echo "   attach       : tmux attach -t $SESSION"
    exit 0
else
    echo "❌ Flask API is NOT running"
    exit 1
fi
