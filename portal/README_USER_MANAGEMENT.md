# User Management System

This module provides user authentication and management functionality for the Bill Review Portal.

## Features

- **Authentication**: Secure login/logout with password hashing
- **User Management**: Create, edit, and delete users
- **Role-Based Access Control**: Admin users can manage other users
- **Profile Management**: Users can change their passwords

## Components

### Models

- `User` class in `models.py`: Defines the user model with authentication methods

### Views

- `auth_bp` Blueprint in `views/auth.py`: Handles authentication and user management routes

### Templates

- `auth/login.html`: Login form
- `auth/users.html`: List of users (admin only)
- `auth/create_user.html`: Create new user form (admin only)
- `auth/edit_user.html`: Edit user form (admin only)
- `auth/profile.html`: User profile page

## Usage

### Setting Up Initial Admin User

Run the following command to create an initial admin user:

```bash
python -m portal.create_admin
```

### Managing Users via CLI

The `manage_users.py` script provides command-line tools for user management:

```bash
# List all users
python manage_users.py list

# Create a new user
python manage_users.py create username password [--admin]

# Delete a user
python manage_users.py delete username

# Change a user's password
python manage_users.py passwd username new_password

# Grant/revoke admin privileges
python manage_users.py admin username [--revoke]
```

### Web Interface

- **User Login**: `/auth/login`
- **User Logout**: `/auth/logout`
- **User List (Admin)**: `/auth/users`
- **Create User (Admin)**: `/auth/users/create`
- **Edit User (Admin)**: `/auth/users/edit/<user_id>`
- **Delete User (Admin)**: `/auth/users/delete/<user_id>` (POST only)
- **User Profile**: `/auth/profile`

## Security Features

- Passwords are hashed using Werkzeug's security utilities
- Flask-Login handles session management
- Admin-required decorator restricts access to admin features
- Protection against CSRF attacks through Flask's built-in CSRF protection 