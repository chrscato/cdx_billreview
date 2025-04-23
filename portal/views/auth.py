from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash
from portal.models import User
from functools import wraps

auth_bp = Blueprint('auth', __name__)

def admin_required(f):
    """Decorator to require admin access for a route."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('You need administrator privileges to access this page.', 'danger')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Handle user login."""
    if current_user.is_authenticated:
        return redirect(url_for('home.home'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        remember = 'remember' in request.form
        
        user = User.get_by_username(username)
        
        if user and user.check_password(password):
            login_user(user, remember=remember)
            flash('Logged in successfully!', 'success')
            
            # Redirect to the page the user was trying to access
            next_page = request.args.get('next')
            if next_page and next_page.startswith('/'):
                return redirect(next_page)
            return redirect(url_for('home.home'))
        
        flash('Invalid username or password', 'danger')
    
    return render_template('auth/login.html')

@auth_bp.route('/logout')
@login_required
def logout():
    """Handle user logout."""
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('auth.login'))

@auth_bp.route('/users')
@login_required
@admin_required
def users_list():
    """Display list of users (admin only)."""
    users = User.get_all_users()
    return render_template('auth/users.html', users=users)

@auth_bp.route('/users/create', methods=['GET', 'POST'])
@login_required
@admin_required
def create_user():
    """Create a new user (admin only)."""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')
        is_admin = 'is_admin' in request.form
        
        # Validate input
        if not username or not password:
            flash('Username and password are required.', 'danger')
            return render_template('auth/create_user.html')
        
        if password != confirm_password:
            flash('Passwords do not match.', 'danger')
            return render_template('auth/create_user.html')
        
        # Check if user already exists
        if User.get_by_username(username):
            flash(f'User "{username}" already exists.', 'danger')
            return render_template('auth/create_user.html')
        
        # Create the user
        success = User.add_user(username, password, is_admin)
        
        if success:
            flash(f'User "{username}" created successfully.', 'success')
            return redirect(url_for('auth.users_list'))
        else:
            flash('Failed to create user.', 'danger')
    
    return render_template('auth/create_user.html')

@auth_bp.route('/users/edit/<int:user_id>', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_user(user_id):
    """Edit a user (admin only)."""
    user = User.get(user_id)
    
    if not user:
        flash('User not found.', 'danger')
        return redirect(url_for('auth.users_list'))
    
    if request.method == 'POST':
        is_admin = 'is_admin' in request.form
        
        # Update admin status
        User.toggle_admin(user_id, is_admin)
        
        # Update password if provided
        password = request.form.get('password')
        if password and password.strip():
            confirm_password = request.form.get('confirm_password')
            
            if password != confirm_password:
                flash('Passwords do not match.', 'danger')
                return render_template('auth/edit_user.html', user=user)
            
            User.update_password(user_id, password)
            flash('Password updated.', 'success')
        
        flash('User updated successfully.', 'success')
        return redirect(url_for('auth.users_list'))
    
    return render_template('auth/edit_user.html', user=user)

@auth_bp.route('/users/delete/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    """Delete a user (admin only)."""
    # Prevent deleting yourself
    if current_user.id == user_id:
        flash('You cannot delete your own account.', 'danger')
        return redirect(url_for('auth.users_list'))
    
    success = User.delete_user(user_id)
    
    if success:
        flash('User deleted successfully.', 'success')
    else:
        flash('Failed to delete user.', 'danger')
    
    return redirect(url_for('auth.users_list'))

@auth_bp.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    """Allow users to view and edit their profile."""
    if request.method == 'POST':
        password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')
        
        if password != confirm_password:
            flash('Passwords do not match.', 'danger')
            return render_template('auth/profile.html')
        
        success = User.update_password(current_user.id, password)
        
        if success:
            flash('Password updated successfully.', 'success')
        else:
            flash('Failed to update password.', 'danger')
    
    return render_template('auth/profile.html') 