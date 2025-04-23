// File: portal/static/js/processing/rates.js

document.addEventListener('DOMContentLoaded', function() {
    // Initialize rate assignment functionality
    initRateAssignment();
    
    // Initialize PDF viewer
    initPdfViewer();

    // Initialize category rates functionality
    initCategoryRates();
});

/**
 * Initialize the PDF viewer functionality
 */
function initPdfViewer() {
    const viewPdfButton = document.getElementById('view-pdf');
    if (!viewPdfButton) return;
    
    viewPdfButton.addEventListener('click', async function() {
        try {
            const filename = document.getElementById('filename').value;
            const response = await fetch(`/processing/fails/${filename}/pdf`);
            const data = await response.json();
            
            if (data.url) {
                window.open(data.url, '_blank');
            } else {
                showAlert('PDF not found or error occurred', 'danger');
            }
        } catch (error) {
            showAlert(`Error fetching PDF: ${error.message}`, 'danger');
        }
    });
}

/**
 * Initialize the category rates functionality
 */
function initCategoryRates() {
    // Handle category checkbox changes
    document.querySelectorAll('[name^="category_enabled["]').forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            handleCategoryToggle(this);
        });

        // Initialize state
        handleCategoryToggle(checkbox);
    });

    // Handle tab switching
    document.querySelectorAll('button[data-bs-toggle="tab"]').forEach(tab => {
        tab.addEventListener('shown.bs.tab', function(event) {
            handleTabSwitch(event.target.getAttribute('id'));
        });
    });

    // Handle form submission
    const form = document.getElementById('rateAssignmentForm');
    if (form) {
        form.addEventListener('submit', function(event) {
            event.preventDefault(); // Prevent default form submission
            handleRateAssignment(); // Use our AJAX handler instead
        });
    }
}

/**
 * Handle toggling of category checkboxes
 */
function handleCategoryToggle(checkbox) {
    const container = checkbox.closest('.col-md-4, .col-md-6');
    const rateInput = container.querySelector('[name^="category_rate["]');
    
    if (checkbox.checked) {
        rateInput.required = true;
        rateInput.disabled = false;
        rateInput.classList.remove('is-invalid');
    } else {
        rateInput.required = false;
        rateInput.disabled = true;
        rateInput.value = '';
        rateInput.classList.remove('is-invalid');
    }
}

/**
 * Handle switching between individual and category rate tabs
 */
function handleTabSwitch(activeTabId) {
    const individualRatesInputs = document.querySelectorAll('#individual-rates input[name="rate"]');
    const categoryRatesCheckboxes = document.querySelectorAll('[name^="category_enabled["]');
    
    if (activeTabId === 'individual-rates-tab') {
        // Enable validation for individual rates
        individualRatesInputs.forEach(input => {
            input.required = true;
        });
        // Disable validation for category rates
        categoryRatesCheckboxes.forEach(checkbox => {
            const rateInput = checkbox.closest('.col-md-4, .col-md-6').querySelector('[name^="category_rate["]');
            rateInput.required = false;
        });
    } else {
        // Disable validation for individual rates
        individualRatesInputs.forEach(input => {
            input.required = false;
        });
        // Enable validation for checked category rates
        categoryRatesCheckboxes.forEach(checkbox => {
            const rateInput = checkbox.closest('.col-md-4, .col-md-6').querySelector('[name^="category_rate["]');
            rateInput.required = checkbox.checked;
        });
    }
}

/**
 * Initialize the rate assignment functionality
 */
function initRateAssignment() {
    const assignRatesBtn = document.getElementById('assignRatesBtn');
    const createOTABtn = document.getElementById('createOTABtn');
    
    // Handle In Network rate assignment
    if (assignRatesBtn) {
        assignRatesBtn.addEventListener('click', handleRateAssignment);
    }
    
    // Handle OTA rate assignment
    if (createOTABtn) {
        createOTABtn.addEventListener('click', handleRateAssignment);
    }
}

/**
 * Validate and collect category rate data
 */
function validateCategoryRates() {
    const categoryData = [];
    let isValid = true;

    document.querySelectorAll('[name^="category_enabled["]').forEach(checkbox => {
        if (checkbox.checked) {
            const container = checkbox.closest('.col-md-4, .col-md-6');
            const rateInput = container.querySelector('[name^="category_rate["]');
            const rate = parseFloat(rateInput.value);

            if (!rateInput.value || rate <= 0) {
                rateInput.classList.add('is-invalid');
                isValid = false;
            } else {
                rateInput.classList.remove('is-invalid');
                // Extract category name from the input name (e.g., "category_rate[mri_wo]" -> "mri_wo")
                const categoryName = rateInput.name.match(/\[(.*?)\]/)[1];
                categoryData.push({
                    category: categoryName,
                    rate: rate
                });
            }
        }
    });

    return {
        isValid,
        categoryData
    };
}

/**
 * Validate and collect individual rate data
 */
function validateIndividualRates() {
    const rateData = [];
    let isValid = true;

    const cptCodeElements = document.querySelectorAll('[data-cpt-code]');
    cptCodeElements.forEach(element => {
        const cptCode = element.dataset.cptCode;
        const rateInput = document.getElementById(`rate-input-${cptCode}`);
        const modifierInput = element.querySelector('[name="modifier"]');
        
        if (rateInput.required && (!rateInput.value || parseFloat(rateInput.value) <= 0)) {
            rateInput.classList.add('is-invalid');
            isValid = false;
        } else if (rateInput.value) {
            rateInput.classList.remove('is-invalid');
            rateData.push({
                cpt_code: cptCode,
                rate: parseFloat(rateInput.value),
                modifier: modifierInput ? modifierInput.value : null
            });
        }
    });

    return {
        isValid,
        rateData
    };
}

/**
 * Format the category summary information into a readable message
 */
function formatCategorySummary(summary) {
    if (!summary || Object.keys(summary).length === 0) return '';
    
    const summaryLines = Object.entries(summary)
        .filter(([_, count]) => count > 0)
        .map(([category, count]) => {
            return `- ${category}: ${count} CPT codes updated`;
        });
    
    if (summaryLines.length === 0) return '';
    
    return '\n\nCategory Update Summary:\n' + summaryLines.join('\n');
}

/**
 * Handle the rate assignment submission
 */
async function handleRateAssignment() {
    try {
        // Get data from the form
        const form = document.getElementById('rateAssignmentForm');
        if (!form) return;
        
        // Get filename and other common data
        const filename = document.getElementById('filename').value;
        
        // Determine active tab
        const activeTab = document.querySelector('.tab-pane.active');
        const isIndividualRates = activeTab.id === 'individual-rates';

        // Validate based on active tab
        let validationResult;
        let rateType;
        let requestData = {};

        if (isIndividualRates) {
            validationResult = validateIndividualRates();
            rateType = 'individual';
            requestData.rates = validationResult.rateData;
        } else {
            validationResult = validateCategoryRates();
            if (validationResult.categoryData.length === 0) {
                showAlert('Please enable at least one category and provide its rate', 'warning');
                return;
            }
            rateType = 'category';
            
            // Build category_rates object in the format the server expects
            const categoryRates = {};
            validationResult.categoryData.forEach(item => {
                categoryRates[item.category] = item.rate;
            });
            requestData.category_rates = categoryRates;
        }

        if (!validationResult.isValid) {
            showAlert('Please enter valid rates for all selected items', 'warning');
            return;
        }

        // Add common fields
        requestData.rate_type = rateType;
        
        // Get filter parameters if they exist
        if (form.querySelector('input[name="filter_params"]')) {
            try {
                requestData.filter_params = JSON.parse(form.querySelector('input[name="filter_params"]').value);
            } catch (e) {
                console.error('Error parsing filter parameters:', e);
            }
        }
        
        // Set button to loading state
        const submitButton = isIndividualRates ? 
            document.getElementById('assignRatesBtn') : 
            document.getElementById('createOTABtn');
        
        let originalButtonText = '';
        if (submitButton) {
            originalButtonText = submitButton.innerHTML;
            submitButton.disabled = true;
            submitButton.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Processing...';
        }
        
        // Submit to server
        const response = await fetch(`/processing/fails/${filename}/assign-rates`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            },
            body: JSON.stringify(requestData)
        });
        
        if (response.ok) {
            const result = await response.json();
            if (result.status === 'success') {
                let successMessage = result.message || 'Rates assigned successfully';
                
                // Add category summary information if available
                if (rateType === 'category' && result.category_summary) {
                    successMessage += formatCategorySummary(result.category_summary);
                }
                
                // Create a more detailed alert for category assignments
                const alertElement = document.createElement('div');
                alertElement.className = 'alert alert-success alert-dismissible fade show';
                alertElement.role = 'alert';
                alertElement.style.whiteSpace = 'pre-line'; // Preserve line breaks
                alertElement.innerHTML = `
                    ${successMessage}
                    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
                `;
                
                // Replace showAlert with direct alert creation for better formatting
                let alertContainer = document.getElementById('alert-container');
                if (!alertContainer) {
                    alertContainer = document.createElement('div');
                    alertContainer.id = 'alert-container';
                    alertContainer.style.position = 'fixed';
                    alertContainer.style.top = '20px';
                    alertContainer.style.right = '20px';
                    alertContainer.style.zIndex = '9999';
                    alertContainer.style.maxWidth = '400px';
                    document.body.appendChild(alertContainer);
                }
                
                // Add to container
                alertContainer.appendChild(alertElement);
                
                // Hide modal
                const modal = bootstrap.Modal.getInstance(document.getElementById('rateAssignmentModal'));
                if (modal) {
                    modal.hide();
                }
                
                // CRITICAL FIX: Force redirect to list view instead of individual file
                setTimeout(() => {
                    window.location.href = '/processing/fails';
                }, 2000);
            } else {
                showAlert(result.message || 'Error assigning rates', 'danger');
                // Reset button state
                if (submitButton) {
                    submitButton.disabled = false;
                    submitButton.innerHTML = originalButtonText;
                }
            }
        } else {
            const errorData = await response.json();
            showAlert(errorData.message || `Server error: ${response.status}`, 'danger');
            // Reset button state
            if (submitButton) {
                submitButton.disabled = false;
                submitButton.innerHTML = originalButtonText;
            }
        }
    } catch (error) {
        showAlert(`Error assigning rate: ${error.message}`, 'danger');
        // Reset button state
        const submitButton = document.querySelector('#assignRatesBtn, #createOTABtn');
        if (submitButton) {
            submitButton.disabled = false;
            submitButton.innerHTML = submitButton.id === 'assignRatesBtn' ? 
                'Assign Rate & Save' : 'Create OTA & Save';
        }
    }
}

/**
 * Display an alert message
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
        alertContainer.style.maxWidth = '400px';
        document.body.appendChild(alertContainer);
    }
    
    // Create the alert
    const alert = document.createElement('div');
    alert.className = `alert alert-${type} alert-dismissible fade show`;
    alert.role = 'alert';
    alert.style.whiteSpace = 'pre-line'; // Preserve line breaks in the message
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
            if (alertContainer.contains(alert)) {
                alertContainer.removeChild(alert);
            }
        }, 150);
    }, 5000);
}