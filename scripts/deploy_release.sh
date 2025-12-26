#!/bin/bash
set -euo pipefail

APP_DIR="/home/wsw/app/code"
LOG="/home/wsw/deploy/deploy_release.log"

{
  echo
  echo "===== DEPLOY START $(date '+%Y-%m-%d %H:%M:%S') ====="
  cd "$APP_DIR"

  echo "HOST=$(hostname)"
  echo "BRANCH=$(git branch --show-current)"
  echo "BEFORE=$(git rev-parse HEAD)"

  git fetch origin
  #git pull origin release/auto-release
  git checkout -B release/auto-release origin/release/auto-release

  echo "AFTER=$(git rev-parse HEAD)"

  #systemctl restart your-app.service

  echo "===== DEPLOY END $(date '+%Y-%m-%d %H:%M:%S') ====="
} >> "$LOG" 2>&1

