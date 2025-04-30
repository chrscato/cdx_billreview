#!/bin/bash

# Activate virtual environment
source /srv/bill_review/venv/bin/activate

# Navigate to app directory
cd /srv/bill_review

# Kill any existing Flask or ngrok processes
pkill -f "python -m portal.run"
pkill -f "ngrok"

# Start Flask app in the background, bound to localhost
nohup python -m portal.run > flask.log 2>&1 &

# Wait briefly to ensure Flask starts
sleep 2

# Start ngrok using named tunnel from ~/.ngrok2/ngrok.yml
nohup ngrok start billreview > ngrok.log 2>&1 &

echo "✅ Flask and ngrok started in background"
echo "🌐 Visit: https://cdx-billreview.ngrok.io"
echo "🔐 Username: admin"
echo "🔐 Password: bill-review-portal2025"
