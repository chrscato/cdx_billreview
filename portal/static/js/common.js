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
 * Display an alert message
 * @param {string} message - The message to display
 * @param {string} type - Bootstrap alert type (success, danger, warning, info)
 */
function showAlert(message, type = 'info') {
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
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        alert.classList.remove('show');
        setTimeout(() => {
            alertContainer.removeChild(alert);
        }, 150);
    }, 5000);
}