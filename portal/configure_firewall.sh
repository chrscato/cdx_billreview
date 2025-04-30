#!/bin/bash

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run this script as root or with sudo"
    exit 1
fi

# Enable UFW if not already enabled
if ! sudo ufw status | grep -q "Status: active"; then
    echo "Enabling UFW..."
    sudo ufw --force enable
fi

# Reset UFW to default deny
echo "Resetting UFW rules..."
sudo ufw --force reset

# Allow OpenSSH
echo "Allowing OpenSSH..."
sudo ufw allow OpenSSH

# Allow localhost access to port 5002
echo "Allowing localhost access to port 5002..."
sudo ufw allow from 127.0.0.1 to any port 5002

# Set default policies
echo "Setting default policies..."
sudo ufw default deny incoming
sudo ufw default allow outgoing

# Reload UFW to apply changes
echo "Reloading UFW..."
sudo ufw reload

# Display status
echo -e "\nUFW Status:"
sudo ufw status numbered

echo -e "\nFirewall configuration complete!" 