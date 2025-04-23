/**
 * Common utility functions for the Bill Review Portal
 */

/**
 * Format a currency value
 * @param {number} value - The value to format
 * @param {string} currency - The currency symbol (default: '$')
 * @returns {string} - Formatted currency string
 */
function formatCurrency(value, currency = '$') {
    return `${currency}${parseFloat(value).toFixed(2)}`;
}

/**
 * Clean a TIN by removing non-digit characters
 * @param {string} tin - The TIN to clean
 * @returns {string} - Cleaned TIN (digits only)
 */
function cleanTin(tin) {
    return tin.replace(/\D/g, '');
}

/**
 * Extract a specific modifier from an array
 * Only returns 26 or TC modifiers, null otherwise
 * @param {Array} modifiers - Array of modifiers
 * @returns {string|null} - The relevant modifier or null
 */
function extractRelevantModifier(modifiers) {
    if (!Array.isArray(modifiers)) return null;
    
    const relevantModifiers = ['26', 'TC'];
    for (const mod of modifiers) {
        if (relevantModifiers.includes(mod)) {
            return mod;
        }
    }
    
    return null;
}

/**
 * Display a flash message in the flash message container
 * @param {string} message - The message to display
 * @param {string} type - Bootstrap alert type (success, danger, warning, info)
 * @param {boolean} autoFade - Whether to auto-dismiss the alert after timeout
 * @param {number} timeout - Time in ms before dismissing the alert
 */
function showFlashMessage(message, type = 'info', autoFade = true, timeout = 5000) {
    // Get the container for flash messages from the base template
    const container = document.getElementById('flash-message-container');
    if (!container) {
        console.error('Flash message container not found');
        return;
    }
    
    // Create the alert
    const alert = document.createElement('div');
    alert.className = `alert alert-${type} alert-dismissible fade show`;
    alert.role = 'alert';
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    // Add to container
    container.appendChild(alert);
    
    // Initialize via Bootstrap to enable the dismiss button
    try {
        const bsAlert = new bootstrap.Alert(alert);
    } catch (e) {
        console.warn('Bootstrap Alert not initialized:', e);
    }
    
    // Auto-dismiss after timeout if requested
    if (autoFade) {
        setTimeout(() => {
            alert.classList.remove('show');
            setTimeout(() => {
                if (container.contains(alert)) {
                    container.removeChild(alert);
                }
            }, 150); // Wait for fade out animation
        }, timeout);
    }
}

/**
 * Display an alert message (floating notification)
 * @param {string} message - The message to display
 * @param {string} type - Bootstrap alert type (success, danger, warning, info)
 * @param {number} timeout - Time in ms before dismissing the alert
 */
function showAlert(message, type = 'info', timeout = 5000) {
    // Check if an alert container exists, create one if not
    let alertContainer = document.getElementById('alert-container');
    if (!alertContainer) {
        alertContainer = document.createElement('div');
        alertContainer.id = 'alert-container';
        alertContainer.style.position = 'fixed';
        alertContainer.style.top = '20px';
        alertContainer.style.right = '20px';
        alertContainer.style.zIndex = '9999';
        document.body.appendChild(alertContainer);
    }
    
    // Create the alert
    const alert = document.createElement('div');
    alert.className = `alert alert-${type} alert-dismissible fade show`;
    alert.role = 'alert';
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    // Add to container
    alertContainer.appendChild(alert);
    
    // Auto-dismiss after specified timeout
    setTimeout(() => {
        alert.classList.remove('show');
        setTimeout(() => {
            if (alertContainer.contains(alert)) {
                alertContainer.removeChild(alert);
            }
        }, 150);
    }, timeout);
}

/**
 * Handle AJAX response that contains flash messages
 * @param {Object} response - The AJAX response object
 * @param {string} defaultSuccessMessage - Default message if no message in response
 * @param {string} defaultErrorMessage - Default error message if no error in response
 * @returns {boolean} - Whether the operation was successful
 */
function handleAjaxResponse(response, defaultSuccessMessage = null, defaultErrorMessage = 'An error occurred') {
    // Handle error response
    if (!response.success && response.status === 'error') {
        showFlashMessage(response.message || defaultErrorMessage, 'danger');
        return false;
    }
    
    // Handle successful response
    if (response.success || response.status === 'success') {
        if (response.message) {
            showFlashMessage(response.message, 'success');
        } else if (defaultSuccessMessage) {
            showFlashMessage(defaultSuccessMessage, 'success');
        }
        
        // Handle redirect if provided
        if (response.redirect) {
            // Wait a moment for the user to see the message before redirecting
            setTimeout(() => {
                window.location.href = response.redirect;
            }, 1000);
        }
        
        return true;
    }
    
    // For other cases, just show any message provided
    if (response.message) {
        const messageType = response.type || 'info';
        showFlashMessage(response.message, messageType);
    }
    
    return !!response.success;
}

/**
 * Automatically set up AJAX form submission with proper flash message handling
 * @param {string} formSelector - CSS selector for the form
 * @param {Object} options - Configuration options
 */
function setupAjaxForm(formSelector, options = {}) {
    const form = document.querySelector(formSelector);
    if (!form) {
        console.error(`Form not found: ${formSelector}`);
        return;
    }
    
    const defaults = {
        successMessage: null,
        errorMessage: 'Form submission failed',
        redirectDelay: 1000,
        beforeSubmit: null,
        afterSuccess: null,
        afterError: null
    };
    
    const config = { ...defaults, ...options };
    
    form.addEventListener('submit', function(e) {
        e.preventDefault();
        
        // Call beforeSubmit callback if provided
        if (config.beforeSubmit && typeof config.beforeSubmit === 'function') {
            const shouldContinue = config.beforeSubmit(form);
            if (shouldContinue === false) return;
        }
        
        // Show loading state
        const submitButton = form.querySelector('button[type="submit"]');
        if (submitButton) {
            const originalText = submitButton.innerHTML;
            submitButton.disabled = true;
            submitButton.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Submitting...';
        }
        
        // Get form data
        const formData = new FormData(form);
        
        // Send AJAX request
        fetch(form.action, {
            method: form.method || 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            const success = handleAjaxResponse(data, config.successMessage, config.errorMessage);
            
            if (success && typeof config.afterSuccess === 'function') {
                config.afterSuccess(data, form);
            } else if (!success && typeof config.afterError === 'function') {
                config.afterError(data, form);
            }
        })
        .catch(error => {
            console.error('AJAX form submission error:', error);
            showFlashMessage(config.errorMessage || error.message, 'danger');
            
            if (typeof config.afterError === 'function') {
                config.afterError({ error: error.message }, form);
            }
        })
        .finally(() => {
            // Restore submit button
            if (submitButton) {
                submitButton.disabled = false;
                submitButton.innerHTML = originalText;
            }
        });
    });
}

// Initialize flash messages auto-dismiss when the page loads
document.addEventListener('DOMContentLoaded', function() {
    // Auto-dismiss existing flash messages
    const flashMessages = document.querySelectorAll('#flash-message-container .alert');
    flashMessages.forEach(message => {
        setTimeout(() => {
            message.classList.remove('show');
            setTimeout(() => {
                if (message.parentNode) {
                    message.parentNode.removeChild(message);
                }
            }, 150);
        }, 5000);
    });
});