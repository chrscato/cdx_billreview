# Bootstrap 3 Implementation

This project uses Flask-Bootstrap which is built on Bootstrap 3. The following changes were made to ensure compatibility:

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