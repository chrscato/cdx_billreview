#!/bin/bash

# Start Flask app in the background with nohup
nohup python -m portal.run > flask.log 2>&1 &

# Wait for Flask to start
sleep 2

# Start ngrok with basic auth and fixed domain
nohup ngrok http --auth="admin:bill-review-portal2025" --domain=cdx-billreview.ngrok.io 5002 > ngrok.log 2>&1 &

echo "Flask and ngrok started in the background. Check flask.log and ngrok.log for output."
