# Bootstrap 3 Implementation

This project uses Flask-Bootstrap which is built on Bootstrap 3. The following changes were made to ensure compatibility:

## Secure Deployment

The application is designed to run securely with the following components:

### Security Configuration

1. **Firewall (UFW) Setup**
   - Run `sudo ./configure_firewall.sh` to:
     - Enable UFW
     - Allow OpenSSH access
     - Allow localhost access to port 5002
     - Block all other incoming connections
     - Allow all outgoing connections

2. **Application Binding**
   - Flask runs on `127.0.0.1:5002` (localhost only)
   - This prevents direct external access to the application
   - All external access is routed through ngrok with authentication

3. **Secure Startup**
   - Run `./start_secure.sh` to:
     - Start Flask application in the background
     - Start ngrok with basic auth (admin:bill-review-portal2025)
     - Logs are redirected to `flask.log` and `ngrok.log`

### Security Architecture

```
External Access ──┐
                 ├─► ngrok (with basic auth) ──► localhost:5002 ──► Flask App
UFW Blocked ─────┘
```

- UFW blocks all direct access to port 5002 from external IPs
- ngrok creates a secure tunnel with authentication
- Flask only accepts connections from localhost
- Basic auth credentials: admin:bill-review-portal2025

## Packages Used

- `flask-bootstrap==3.3.7.1` - This provides Bootstrap 3 integration with Flask
- `Font-Awesome` - For icons (CDN linked in base template)

## CSS Customizations

Custom CSS has been added in `static/css/styles.css` to:

1. Provide spacing utilities similar to Bootstrap 5 (mt-*, mb-*, etc.)
2. Provide enhanced styling for Bootstrap 3 components
3. Add modern utility classes similar to Bootstrap 5

## Template Structure

All templates have been updated to use Bootstrap 3 markup:

- Panels instead of Cards
- Bootstrap 3 grid system (col-md-offset-* instead of offset-md-*)
- Bootstrap 3 form styling
- Bootstrap 3 navigation bar structure

## Icons

Font Awesome is used for icons instead of Bootstrap Icons:

- `fa fa-user` instead of `bi bi-person`
- `fa fa-save` instead of `fas fa-save`
- `fa fa-sign-in` instead of `fas fa-sign-in-alt`

## JavaScript

The application uses Bootstrap 3's JavaScript components which are automatically included by Flask-Bootstrap.

## Development Guidelines

When developing new templates or updating existing ones:

1. Use Bootstrap 3 classes and components
2. Refer to the [Bootstrap 3 documentation](https://getbootstrap.com/docs/3.4/) 
3. Use the custom CSS utilities for modern spacing
4. Use Font Awesome for icons

## Troubleshooting

If you encounter issues with styles or components:

1. Verify you're using Bootstrap 3 class names
2. Check that the template extends the base template
3. Use browser dev tools to inspect elements 