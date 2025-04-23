#!/bin/bash

# Set your droplet IP and target path
DROPLET_IP="159.223.104.254"
REMOTE_PATH="/root/bill_review"

echo "🔄 Copying updated files to droplet..."
scp -r ./* root@$DROPLET_IP:$REMOTE_PATH-temp

echo "🔁 Connecting via SSH to deploy..."
ssh root@$DROPLET_IP << EOF
  echo "✅ Cleaning up old project..."
  rm -rf $REMOTE_PATH
  mv ${REMOTE_PATH}-temp $REMOTE_PATH

  echo "🔁 Stopping old containers with Docker Compose..."
  cd $REMOTE_PATH
  docker compose down || true

  echo "🔨 Rebuilding and starting containers..."
  docker compose up --build -d

  echo "✅ Deployment complete!"
  echo "🌍 Your app should be live at: https://cdx-billreview.ngrok.io"
EOF
