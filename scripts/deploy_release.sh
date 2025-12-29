#!/usr/bin/env bash
set -Eeuo pipefail
# --------------------------------------------------
# 基本配置
# --------------------------------------------------
APP_DIR="/home/wsw/app/code"
LOG_DIR="/home/wsw/deploy"
LOG="${LOG_DIR}/release.log"

# RUN_ID：允许从上游透传（webhook → 本机 → SSH → 这里）
RUN_ID="${RUN_ID:-$(date '+%Y%m%d-%H%M%S')-$$}"


mkdir -p "$LOG_DIR"

# 所有 stdout / stderr 统一进日志
exec >>"$LOG" 2>&1

echo
echo "=================================================="
echo "RUN_ID=$RUN_ID"
echo "DEPLOY START $(date '+%Y-%m-%d %H:%M:%S')"
echo "USER=$(whoami)"
echo "HOST=$(hostname)"
echo "APP_DIR=$APP_DIR"

# --------------------------------------------------
# 进入代码目录
# --------------------------------------------------
cd "$APP_DIR"

# --------------------------------------------------
# Git 状态（安全方式，避免非交互 exit）
# --------------------------------------------------
BEFORE="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
echo "BEFORE=$BEFORE"


# --------------------------------------------------
# 拉取并强制对齐 release/auto-release
# --------------------------------------------------
git fetch origin
git checkout -B release/auto-release origin/release/auto-release

AFTER="$(git rev-parse HEAD)"
echo "AFTER=$AFTER"

# --------------------------------------------------
# 重启服务（失败不炸 webhook）
# --------------------------------------------------
#set +e
#systemctl restart your-app.service
#SERVICE_RC=$?
#set -e
#if [[ $SERVICE_RC -ne 0 ]]; then
#  echo "[RUN_ID=$RUN_ID][WARN] service restart failed (rc=$SERVICE_RC)"
#else
#  echo "[RUN_ID=$RUN_ID] service restarted successfully"
#fi

echo "DEPLOY END $(date '+%Y-%m-%d %H:%M:%S')"
echo "=================================================="

exit 0