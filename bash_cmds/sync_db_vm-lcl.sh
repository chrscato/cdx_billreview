#!/bin/bash

# Sync remote filemaker.db from production server to local machine
echo "Downloading filemaker.db from server..."
scp root@159.223.104.254:/srv/bill_review/filemaker.db "C:/Users/ChristopherCato/OneDrive - clarity-dx.com/code/bill_review/filemaker.db"

echo "✅ Database downloaded to local machine."
