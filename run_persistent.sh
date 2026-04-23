#!/bin/bash
# Persistent runner: restarts bot and dashboard if they crash

cd /root/polymarket-bot

# Start bot if not running
ensure_bot() {
  if ! pgrep -f '[p]olybot run' > /dev/null; then
    echo "[$(date)] Starting bot..."
    nohup polybot run > data/bot.log 2>&1 &
  fi
}

# Start dashboard if not running
ensure_dashboard() {
  if ! pgrep -f 'streamlit.*app.py' > /dev/null; then
    echo "[$(date)] Starting dashboard..."
    nohup .venv/bin/streamlit run src/polybot/dashboard/app.py --server.port 8503 --server.address 0.0.0.0 > data/dashboard.log 2>&1 &
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
