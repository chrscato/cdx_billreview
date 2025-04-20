// File: portal/static/js/processing/rates.js

document.addEventListener('DOMContentLoaded', function() {
    // Initialize rate assignment functionality
    initRateAssignment();
    
    // Initialize PDF viewer
    initPdfViewer();
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
        createOTABtn.addEventListener('click', function() {
            showAlert('OTA rate assignment will be implemented separately', 'info');
        });
    }
}

/**
 * Handle the rate assignment submission
 */
async function handleRateAssignment() {
    // Get data from the form
    const form = document.getElementById('rateAssignmentForm');
    if (!form) return;
    
    // Get filename
    const filename = document.getElementById('filename').value;
    
    // Get CPT code(s) from the failure reasons
    const cptCodeElements = document.querySelectorAll('[data-cpt-code]');
    const cptCodes = Array.from(cptCodeElements).map(el => el.dataset.cptCode);
    
    if (cptCodes.length === 0) {
        showAlert('No CPT codes found for rate assignment', 'danger');
        return;
    }
    
    // Validate rate inputs
    let isValid = true;
    const rateData = [];
    
    cptCodes.forEach(cptCode => {
        const rateInput = document.getElementById(`rate-input-${cptCode}`);
        const modifierInput = document.querySelector(`[data-cpt-code="${cptCode}"] [name="modifier"]`);
        
        if (!rateInput.value || parseFloat(rateInput.value) <= 0) {
            rateInput.classList.add('is-invalid');
            isValid = false;
        } else {
            rateInput.classList.remove('is-invalid');
            
            rateData.push({
                cpt_code: cptCode,
                rate: parseFloat(rateInput.value),
                modifier: modifierInput ? modifierInput.value : null
            });
        }
    });
    
    if (!isValid) {
        showAlert('Please enter valid rates for all CPT codes', 'warning');
        return;
    }
    
    // Get TIN and other data
    const cleanTin = document.getElementById('clean-tin').value;
    const notes = document.getElementById('rate-notes').value;
    const providerNetwork = document.getElementById('provider-network').value;
    
    try {
        // Submit to server
        const response = await fetch(`/processing/fails/${filename}/assign-rates`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                tin: cleanTin,
                rates: rateData,
                notes: notes,
                provider_network: providerNetwork
            })
        });
        
        if (response.ok) {
            const result = await response.json();
            if (result.success) {
                showAlert('Rate(s) assigned successfully!', 'success');
                // Redirect after a short delay
                setTimeout(() => {
                    window.location.href = '/processing/fails';
                }, 1500);
            } else {
                showAlert(`Error: ${result.error}`, 'danger');
            }
        } else {
            showAlert(`Server error: ${response.status}`, 'danger');
        }
    } catch (error) {
        showAlert(`Error assigning rate: ${error.message}`, 'danger');
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