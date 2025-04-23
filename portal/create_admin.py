#!/usr/bin/env python
"""
Script to create an initial admin user for the bill review portal.
"""
import sys
import os
import getpass
from pathlib import Path

# Add the parent directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import after setting path
from portal.models import User

def create_admin_user():
    """Create an admin user with interactive prompts."""
    print("Creating admin user for Bill Review Portal")
    print("-" * 50)
    
    username = input("Enter admin username: ").strip()
    
    # Validate username
    if not username:
        print("Error: Username cannot be empty.")
        return False
    
    # Check if user already exists
    if User.get_by_username(username):
        print(f"Error: User '{username}' already exists.")
        return False
    
    # Get password with confirmation
    while True:
        password = getpass.getpass("Enter password: ")
        
        if not password:
            print("Error: Password cannot be empty.")
            continue
        
        confirm_password = getpass.getpass("Confirm password: ")
        
        if password != confirm_password:
            print("Error: Passwords do not match. Please try again.")
            continue
        
        break
    
    # Create the admin user
    success = User.add_user(username, password, is_admin=True)
    
    if success:
        print(f"Admin user '{username}' created successfully.")
    else:
        print(f"Error: Failed to create admin user '{username}'.")
    
    return success

if __name__ == "__main__":
    create_admin_user() 