#!/usr/bin/env python
"""
Command-line utility to manage users for the bill review portal.
"""
import sys
import argparse
import os
from pathlib import Path

# Add the project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Import after setting path
from portal.models import User

def create_user(username, password, is_admin=False):
    """Create a new user."""
    if User.get_by_username(username):
        print(f"Error: User '{username}' already exists.")
        return False
    
    success = User.add_user(username, password, is_admin)
    if success:
        print(f"User '{username}' created successfully.")
        if is_admin:
            print(f"User '{username}' has admin privileges.")
    else:
        print(f"Error: Failed to create user '{username}'.")
    
    return success

def delete_user(username):
    """Delete a user by username."""
    user = User.get_by_username(username)
    if not user:
        print(f"Error: User '{username}' not found.")
        return False
    
    success = User.delete_user(user.id)
    if success:
        print(f"User '{username}' deleted successfully.")
    else:
        print(f"Error: Failed to delete user '{username}'.")
    
    return success

def list_users():
    """List all users."""
    users = User.get_all_users()
    if not users:
        print("No users found.")
        return
    
    print(f"Total users: {len(users)}")
    print("ID | Username | Admin")
    print("-" * 30)
    for user in users:
        is_admin = "Yes" if user.get('is_admin', False) else "No"
        print(f"{user['id']} | {user['username']} | {is_admin}")

def change_password(username, new_password):
    """Change a user's password."""
    user = User.get_by_username(username)
    if not user:
        print(f"Error: User '{username}' not found.")
        return False
    
    success = User.update_password(user.id, new_password)
    if success:
        print(f"Password for user '{username}' updated successfully.")
    else:
        print(f"Error: Failed to update password for user '{username}'.")
    
    return success

def toggle_admin(username, make_admin):
    """Toggle admin status for a user."""
    users = User.get_all_users()
    for user in users:
        if user['username'] == username:
            user['is_admin'] = make_admin
            User.save_users(users)
            status = "granted" if make_admin else "revoked"
            print(f"Admin privileges {status} for user '{username}'.")
            return True
    
    print(f"Error: User '{username}' not found.")
    return False

def main():
    parser = argparse.ArgumentParser(description="Manage users for the bill review portal")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Create user command
    create_parser = subparsers.add_parser("create", help="Create a new user")
    create_parser.add_argument("username", help="Username for the new user")
    create_parser.add_argument("password", help="Password for the new user")
    create_parser.add_argument("--admin", action="store_true", help="Grant admin privileges")
    
    # Delete user command
    delete_parser = subparsers.add_parser("delete", help="Delete a user")
    delete_parser.add_argument("username", help="Username of the user to delete")
    
    # List users command
    list_parser = subparsers.add_parser("list", help="List all users")
    
    # Change password command
    passwd_parser = subparsers.add_parser("passwd", help="Change a user's password")
    passwd_parser.add_argument("username", help="Username of the user")
    passwd_parser.add_argument("new_password", help="New password for the user")
    
    # Grant/revoke admin privileges
    admin_parser = subparsers.add_parser("admin", help="Grant or revoke admin privileges")
    admin_parser.add_argument("username", help="Username of the user")
    admin_parser.add_argument("--revoke", action="store_true", help="Revoke admin privileges")
    
    args = parser.parse_args()
    
    if args.command == "create":
        create_user(args.username, args.password, args.admin)
    elif args.command == "delete":
        delete_user(args.username)
    elif args.command == "list":
        list_users()
    elif args.command == "passwd":
        change_password(args.username, args.new_password)
    elif args.command == "admin":
        toggle_admin(args.username, not args.revoke)
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 