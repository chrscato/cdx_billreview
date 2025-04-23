from flask_login import UserMixin
import json
import os
from pathlib import Path
from werkzeug.security import generate_password_hash, check_password_hash

# Path to the users.json file
USERS_FILE = Path(__file__).parent / "data" / "users.json"

# Ensure the data directory exists
os.makedirs(Path(__file__).parent / "data", exist_ok=True)

class User(UserMixin):
    """User model for authentication."""
    
    def __init__(self, id, username, password_hash, is_admin=False):
        self.id = id
        self.username = username
        self.password_hash = password_hash
        self.is_admin = is_admin
    
    def check_password(self, password):
        """Check if the provided password matches the stored hash."""
        return check_password_hash(self.password_hash, password)
    
    @staticmethod
    def get(user_id):
        """Get a user by ID."""
        users = User.get_all_users()
        for user in users:
            if user.get('id') == int(user_id):
                return User(
                    id=user['id'],
                    username=user['username'],
                    password_hash=user['password_hash'],
                    is_admin=user.get('is_admin', False)
                )
        return None
    
    @staticmethod
    def get_by_username(username):
        """Get a user by username."""
        users = User.get_all_users()
        for user in users:
            if user.get('username') == username:
                return User(
                    id=user['id'],
                    username=user['username'],
                    password_hash=user['password_hash'],
                    is_admin=user.get('is_admin', False)
                )
        return None
    
    @staticmethod
    def get_all_users():
        """Get all users from the users.json file."""
        if not USERS_FILE.exists():
            return []
        
        try:
            with open(USERS_FILE, 'r') as f:
                users = json.load(f)
                return users
        except (json.JSONDecodeError, FileNotFoundError):
            # Return empty list if file doesn't exist or is invalid
            return []
    
    @staticmethod
    def save_users(users):
        """Save users to the users.json file."""
        with open(USERS_FILE, 'w') as f:
            json.dump(users, f, indent=4)
        return True
    
    @staticmethod
    def add_user(username, password, is_admin=False):
        """Add a new user."""
        # Get existing users
        users = User.get_all_users()
        
        # Check if username already exists
        if any(user.get('username') == username for user in users):
            return False
        
        # Generate a new user ID
        user_id = 1
        if users:
            user_id = max(user.get('id', 0) for user in users) + 1
        
        # Create new user
        new_user = {
            'id': user_id,
            'username': username,
            'password_hash': generate_password_hash(password),
            'is_admin': is_admin
        }
        
        # Add to users list and save
        users.append(new_user)
        return User.save_users(users)
    
    @staticmethod
    def delete_user(user_id):
        """Delete a user by ID."""
        users = User.get_all_users()
        
        # Filter out the user to delete
        updated_users = [user for user in users if user.get('id') != user_id]
        
        # If no users were removed, user doesn't exist
        if len(updated_users) == len(users):
            return False
        
        return User.save_users(updated_users)
    
    @staticmethod
    def update_password(user_id, new_password):
        """Update a user's password."""
        users = User.get_all_users()
        
        for user in users:
            if user.get('id') == user_id:
                user['password_hash'] = generate_password_hash(new_password)
                return User.save_users(users)
        
        return False
    
    @staticmethod
    def toggle_admin(user_id, make_admin):
        """Toggle admin status for a user."""
        users = User.get_all_users()
        
        for user in users:
            if user.get('id') == user_id:
                user['is_admin'] = make_admin
                return User.save_users(users)
        
        return False 