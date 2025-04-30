#!/bin/bash

# Activate virtual environment
source /srv/bill_review/venv/bin/activate

# Navigate to app directory
cd /srv/bill_review

# Kill any previous Flask or ngrok process
pkill -f "python -m portal.run"
pkill -f "ngrok start billreview"

# Start Flask on localhost:5002
nohup python -m portal.run > flask.log 2>&1 &

# Wait for Flask to start
sleep 3

# Start ngrok using named tunnel and full snap path
nohup /snap/bin/ngrok start billreview --config /root/.ngrok2/ngrok.yml > ngrok.log 2>&1 &

echo "✅ Flask and ngrok started"
echo "🌐 URL: https://cdx-billreview.ngrok.io"
echo "🔐 Login: admin / bill-review-portal2025"
