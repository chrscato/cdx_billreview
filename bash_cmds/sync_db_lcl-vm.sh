#!/bin/bash

# Sync local filemaker.db to production server
echo "Uploading local filemaker.db to server..."
scp "C:/Users/ChristopherCato/OneDrive - clarity-dx.com/code/bill_review/filemaker.db" root@159.223.104.254:/srv/bill_review/filemaker.db

echo "✅ Database updated on server."
