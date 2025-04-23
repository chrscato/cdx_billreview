#!/bin/bash

# CONFIG: Update this!
DROPLET_IP="159.223.104.254"
REMOTE_PATH="/root/bill_review"
CONTAINER_NAME="bill-review"
IMAGE_NAME="bill-review-app"

echo "🔄 Copying updated files to droplet..."
scp -r ./ root@$DROPLET_IP:$REMOTE_PATH-temp

echo "🔁 Connecting via SSH to deploy..."
ssh root@$DROPLET_IP << EOF
  echo "✅ Cleaning up old project..."
  rm -rf $REMOTE_PATH
  mv ${REMOTE_PATH}-temp $REMOTE_PATH

  echo "🔁 Shutting down old containers..."
  cd $REMOTE_PATH
  docker compose down || true

  echo "🔨 Rebuilding and starting fresh containers..."
  docker compose up --build -d

  echo "🌍 App should be live at: https://cdx-billreview.ngrok.io"
EOF
