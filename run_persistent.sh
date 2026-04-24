#!/bin/bash
# Persistent runner: restarts bot and dashboard if they crash

cd /root/polymarket-bot

BOT_PID_FILE="data/bot.pid"
DASH_PID_FILE="data/dashboard.pid"

is_running() {
  local pid_file="$1"
  [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null
}

ensure_bot() {
  if ! is_running "$BOT_PID_FILE"; then
    echo "[$(date)] Starting bot..."
    nohup .venv/bin/polybot run >> data/bot.log 2>&1 &
    echo $! > "$BOT_PID_FILE"
  fi
}

ensure_dashboard() {
  if ! is_running "$DASH_PID_FILE"; then
    echo "[$(date)] Starting dashboard..."
    nohup .venv/bin/streamlit run src/polybot/dashboard/app.py --server.port 8501 --server.address 0.0.0.0 --browser.gatherUsageStats false >> data/dashboard.log 2>&1 &
    echo $! > "$DASH_PID_FILE"
  fi
}

# Initial start
ensure_bot
ensure_dashboard

# Monitor loop - check every 10 seconds
while true; do
  sleep 10
  ensure_bot
  ensure_dashboard
done
