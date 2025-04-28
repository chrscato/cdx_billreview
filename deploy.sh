#!/bin/bash

# Push local changes
echo "Pushing changes to GitHub..."
git add .
git commit -m "Deploy: $(date)"
git push origin master

# SSH into server, pull, and rsync
echo "Deploying to server..."
ssh root@159.223.104.254 << 'EOF'
  cd /opt/bill_review
  git pull origin master
  rsync -av \
    --exclude '.env' \
    --exclude '*.db' \
    --exclude 'data/' \
    --exclude 'db_backups/' \
    /opt/bill_review/ /srv/bill_review/
EOF

echo "✅ Deployment complete."
