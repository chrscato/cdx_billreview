/**
 * Failed Bills View Manager
 * Manages the display, filtering, and grouping of failed bills
 */

// Global state
let allFailedBills = [];

// Chart objects
let failureTypesChart = null;
let providersChart = null;
let ageChart = null;

// Constants for failure types and their display properties
const FAILURE_TYPES = {
    'RATE_MISSING': { label: 'Missing Rate', color: '#dc3545', icon: 'fa-dollar-sign' },
    'UNMATCHED_CPT': { label: 'Unmatched CPT', color: '#fd7e14', icon: 'fa-code' },
    'TOO_MANY_UNITS': { label: 'Too Many Units', color: '#ffc107', icon: 'fa-list-ol' }
};

// Utility function to populate dropdown filters
function populateDropdown(id, values, defaultLabel) {
    const select = document.getElementById(id);
    select.innerHTML = "";

    const defaultOption = document.createElement("option");
    defaultOption.value = defaultLabel;
    defaultOption.textContent = defaultLabel;
    select.appendChild(defaultOption);

    values.forEach(v => {
        const opt = document.createElement("option");
        opt.value = v;
        opt.textContent = v;
        select.appendChild(opt);
    });
}

/**
 * Safely get an element by ID - returns null if element doesn't exist
 */
function safeGetById(id) {
    return document.getElementById(id);
}

// Render the bill list with the filtered data
function renderBillList(bills) {
    const container = document.getElementById("bill-list-container");
    container.innerHTML = "";

    if (!bills.length) {
        container.innerHTML = "<p>No failed bills match the selected filters.</p>";
        return;
    }

    bills.forEach(bill => {
        const div = document.createElement("div");
        div.className = "bill-card";

        div.innerHTML = `
            <div class="card mb-3 p-3 border">
                <div class="d-flex justify-content-between align-items-center">
                    <strong>${bill.filename}</strong>
                    <a href="/processing/fails/${bill.filename}" class="btn btn-sm btn-outline-primary">View</a>
                </div>
                <div>
                    <span class="badge text-bg-warning me-2">${bill.provider || "Unknown Provider"}</span>
                    <span class="badge text-bg-light">${bill.dos || "Unknown DOS"}</span>
                    <span class="badge text-bg-secondary">${bill.age_days} days old</span>
                </div>
                <div class="mt-2">
                    ${bill.failure_types.map(type => `<span class="badge text-bg-danger me-1">${type}</span>`).join("")}
                </div>
            </div>
        `;

        container.appendChild(div);
    });
}

// Filter and re-render the bill list
function filterAndRender() {
    const type = document.getElementById("filter-failure-type").value;
    const provider = document.getElementById("filter-provider").value;
    const age = document.getElementById("filter-age").value;
    const search = document.getElementById("search-filename").value.toLowerCase();

    let filtered = [...allFailedBills];

    if (type !== "All Types") {
        filtered = filtered.filter(b => b.failure_types.includes(type));
    }

    if (provider !== "All Providers") {
        filtered = filtered.filter(b => b.provider === provider);
    }

    if (age !== "All Dates") {
        filtered = filtered.filter(b => {
            const d = b.age_days || 0;
            if (age === "0–30 days") return d <= 30;
            if (age === "31–60 days") return d > 30 && d <= 60;
            return d > 60;
        });
    }

    if (search) {
        filtered = filtered.filter(b => b.filename.toLowerCase().includes(search));
    }

    renderBillList(filtered);
    
    // Update count badge
    const countBadge = document.querySelector('.badge.bg-danger');
    if (countBadge) {
        countBadge.textContent = filtered.length;
    }
}

// Format age display
function formatAge(ageDays) {
    if (!ageDays) return 'Unknown';
    if (ageDays < 1) return 'Today';
    if (ageDays === 1) return 'Yesterday';
    if (ageDays < 7) return `${ageDays} days ago`;
    if (ageDays < 30) return `${Math.floor(ageDays/7)} weeks ago`;
    return `${Math.floor(ageDays/30)} months ago`;
}

// Get failure type badges HTML
function getFailureTypeBadges(types) {
    if (!Array.isArray(types)) return '';
    
    const badgeClasses = {
        'RATE_MISSING': 'bg-warning',
        'UNMATCHED_CPT': 'bg-danger',
        'TOO_MANY_UNITS': 'bg-info',
        'READ_ERROR': 'bg-secondary'
    };
    
    return types.map(type => {
        const badgeClass = badgeClasses[type] || 'bg-primary';
        return `<span class="badge ${badgeClass} me-1">${type}</span>`;
    }).join('');
}

/**
 * Initialize the dashboard
 */
function initDashboard() {
    const bills = window.BILLS_DATA;
    if (!Array.isArray(bills)) {
        console.error("❌ BILLS_DATA is not an array.");
        return;
    }

    allFailedBills = bills;
    console.log("✅ Loaded failed bills:", allFailedBills.length);

    const failureTypes = [...new Set(bills.flatMap(b => Array.isArray(b.failure_types) ? b.failure_types : []))].sort();
    const providers = [...new Set(bills.map(b => b.provider).filter(Boolean))].sort();
    const ageBuckets = ["0–30 days", "31–60 days", "60+ days"];

    populateDropdown("filter-failure-type", failureTypes, "All Types");
    populateDropdown("filter-provider", providers, "All Providers");
    populateDropdown("filter-age", ageBuckets, "All Dates");

    // Set up event listeners
    document.getElementById("filter-failure-type").addEventListener("change", filterAndRender);
    document.getElementById("filter-provider").addEventListener("change", filterAndRender);
    document.getElementById("filter-age").addEventListener("change", filterAndRender);
    document.getElementById("search-filename").addEventListener("input", filterAndRender);

    filterAndRender();  // render after populating filters
}

/**
 * Load failed bills data
 */
function loadFailedBills() {
    console.log('Loading failed bills data...');
    
    // Get the bills data from the data attribute in the HTML
    const billsDataElement = safeGetById('failed-bills-data');
    
    if (!billsDataElement) {
        console.error('Failed bills data element not found');
        return;
    }
    
    try {
        allFailedBills = JSON.parse(billsDataElement.dataset.bills || '[]');
        console.log(`Loaded ${allFailedBills.length} failed bills`);
        
        // Initialize the dashboard with all bills
        filteredBills = [...allFailedBills];
        
        // Populate filter options
        populateFilterOptions();
        
        // Initialize charts
        initCharts();
        
        // Display bills in default view (list)
        displayBills('list', 'type');
        
    } catch (error) {
        console.error('Error parsing bills data:', error);
        showError('Failed to load bill data. Please refresh the page and try again.');
    }
}

/**
 * Show error message
 */
function showError(message) {
    const listContainer = safeGetById('listViewContainer');
    if (listContainer) {
        listContainer.innerHTML = `
            <div class="alert alert-danger m-3">
                <i class="fas fa-exclamation-triangle me-2"></i>
                ${message}
            </div>
        `;
    }
}

/**
 * Populate filter dropdown options
 */
function populateFilterOptions() {
    console.log('Populating filter options...');
    
    // Get unique providers
    const providers = [...new Set(allFailedBills.map(bill => 
        bill.filemaker?.provider?.['DBA Name Billing Name'] || 'Unknown Provider'
    ))];
    
    // Get unique failure types
    const failureTypes = new Set();
    allFailedBills.forEach(bill => {
        if (bill.validation_info?.failure_reasons) {
            bill.validation_info.failure_reasons.forEach(reason => {
                const type = reason.split(':')[0].trim();
                failureTypes.add(type);
            });
        }
    });
    
    // Populate provider filter dropdown
    const providerFilter = safeGetById('providerFilter');
    if (providerFilter) {
        providers.forEach(provider => {
            const option = document.createElement('option');
            option.value = provider;
            option.textContent = provider;
            providerFilter.appendChild(option);
        });
    }
    
    // Populate failure type filter
    const failureTypeFilter = safeGetById('failureTypeFilter');
    if (failureTypeFilter) {
        // Clear existing options except "All Types"
        while (failureTypeFilter.options.length > 1) {
            failureTypeFilter.remove(1);
        }
        
        // Add options for each failure type
        failureTypes.forEach(type => {
            const option = document.createElement('option');
            option.value = type;
            option.textContent = FAILURE_TYPES[type]?.label || type;
            failureTypeFilter.appendChild(option);
        });
    }
}

/**
 * Initialize dashboard charts
 */
function initCharts() {
    console.log('Initializing charts...');
    
    // Extract stats for charts
    const stats = calculateStats(filteredBills);
    
    // Failure Types Chart
    const failureTypesCtx = safeGetById('failureTypesChart')?.getContext('2d');
    if (failureTypesCtx) {
        failureTypesChart = new Chart(failureTypesCtx, {
            type: 'bar',
            data: {
                labels: Object.keys(stats.failureTypes).map(type => FAILURE_TYPES[type]?.label || type),
                datasets: [{
                    label: 'Number of Failures',
                    data: Object.values(stats.failureTypes),
                    backgroundColor: Object.keys(stats.failureTypes).map(type => FAILURE_TYPES[type]?.color || '#6c757d')
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        display: false
                    },
                    title: {
                        display: true,
                        text: 'Failures by Type'
                    }
                }
            }
        });
    }
    
    // Providers Chart
    const providersCtx = safeGetById('providersChart')?.getContext('2d');
    if (providersCtx) {
        providersChart = new Chart(providersCtx, {
            type: 'pie',
            data: {
                labels: Object.keys(stats.providers).slice(0, 5),
                datasets: [{
                    data: Object.values(stats.providers).slice(0, 5),
                    backgroundColor: [
                        '#007bff', '#6610f2', '#6f42c1', '#e83e8c', '#20c997'
                    ]
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        position: 'right'
                    },
                    title: {
                        display: true,
                        text: 'Top 5 Providers with Issues'
                    }
                }
            }
        });
    }
    
    // Age Chart
    const ageCtx = safeGetById('ageChart')?.getContext('2d');
    if (ageCtx) {
        ageChart = new Chart(ageCtx, {
            type: 'line',
            data: {
                labels: Object.keys(stats.ages),
                datasets: [{
                    label: 'Files by Age',
                    data: Object.values(stats.ages),
                    borderColor: '#ffc107',
                    backgroundColor: 'rgba(255, 193, 7, 0.2)',
                    tension: 0.4,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: 'Files by Age (days)'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }
}

/**
 * Calculate statistics for charts and displays
 */
function calculateStats(bills) {
    const stats = {
        failureTypes: {},
        providers: {},
        ages: {
            '0-30': 0,
            '31-60': 0,
            '61-90': 0,
            '91+': 0
        }
    };
    
    bills.forEach(bill => {
        // Count failure types
        if (bill.validation_info?.failure_reasons) {
            bill.validation_info.failure_reasons.forEach(reason => {
                const type = reason.split(':')[0].trim();
                stats.failureTypes[type] = (stats.failureTypes[type] || 0) + 1;
            });
        }
        
        // Count providers
        const provider = bill.filemaker?.provider?.['DBA Name Billing Name'] || 'Unknown Provider';
        stats.providers[provider] = (stats.providers[provider] || 0) + 1;
        
        // Calculate age from DOS
        let oldestDOS = null;
        if (bill.filemaker?.line_items && bill.filemaker.line_items.length > 0) {
            bill.filemaker.line_items.forEach(item => {
                if (item.DOS) {
                    const dosDate = new Date(item.DOS);
                    if (!oldestDOS || dosDate < oldestDOS) {
                        oldestDOS = dosDate;
                    }
                }
            });
            
            if (oldestDOS) {
                const ageInDays = Math.floor((new Date() - oldestDOS) / (1000 * 60 * 60 * 24));
                
                if (ageInDays <= 30) {
                    stats.ages['0-30']++;
                } else if (ageInDays <= 60) {
                    stats.ages['31-60']++;
                } else if (ageInDays <= 90) {
                    stats.ages['61-90']++;
                } else {
                    stats.ages['91+']++;
                }
            }
        }
    });
    
    return stats;
}

/**
 * Apply filters to the bills data
 */
function applyFilters() {
    console.log('Applying filters...');
    
    const failureTypeFilter = safeGetById('failureTypeFilter');
    const providerFilter = safeGetById('providerFilter');
    const ageFilter = safeGetById('ageFilter');
    
    const failureType = failureTypeFilter ? failureTypeFilter.value : 'ALL';
    const provider = providerFilter ? providerFilter.value : 'ALL';
    const ageRange = ageFilter ? ageFilter.value : 'ALL';
    
    filteredBills = allFailedBills.filter(bill => {
        // Filter by failure type
        if (failureType !== 'ALL') {
            if (!bill.validation_info?.failure_reasons) return false;
            const hasFailureType = bill.validation_info.failure_reasons.some(reason => 
                reason.startsWith(failureType)
            );
            if (!hasFailureType) return false;
        }
        
        // Filter by provider
        if (provider !== 'ALL') {
            const billProvider = bill.filemaker?.provider?.['DBA Name Billing Name'] || 'Unknown Provider';
            if (billProvider !== provider) return false;
        }
        
        // Filter by age
        if (ageRange !== 'ALL') {
            let oldestDOS = null;
            if (bill.filemaker?.line_items && bill.filemaker.line_items.length > 0) {
                bill.filemaker.line_items.forEach(item => {
                    if (item.DOS) {
                        const dosDate = new Date(item.DOS);
                        if (!oldestDOS || dosDate < oldestDOS) {
                            oldestDOS = dosDate;
                        }
                    }
                });
                
                if (oldestDOS) {
                    const ageInDays = Math.floor((new Date() - oldestDOS) / (1000 * 60 * 60 * 24));
                    
                    if (ageRange === 'RECENT' && ageInDays > 30) return false;
                    if (ageRange === '30-60' && (ageInDays <= 30 || ageInDays > 60)) return false;
                    if (ageRange === '60-90' && (ageInDays <= 60 || ageInDays > 90)) return false;
                    if (ageRange === '90+' && ageInDays <= 90) return false;
                }
            }
        }
        
        return true;
    });
    
    // Update the view
    updateDashboard();
}

/**
 * Clear all filters
 */
function clearFilters() {
    const failureTypeFilter = safeGetById('failureTypeFilter');
    const providerFilter = safeGetById('providerFilter');
    const ageFilter = safeGetById('ageFilter');
    
    if (failureTypeFilter) failureTypeFilter.value = 'ALL';
    if (providerFilter) providerFilter.value = 'ALL';
    if (ageFilter) ageFilter.value = 'ALL';
    
    filteredBills = [...allFailedBills];
    updateDashboard();
}

/**
 * Update the dashboard with filtered data
 */
function updateDashboard() {
    console.log('Updating dashboard...');
    
    // Get current view type and grouping
    const activeViewBtn = document.querySelector('.view-toggle-btn.active');
    const activeGroupBtn = document.querySelector('.group-toggle-btn.active');
    
    const viewType = activeViewBtn ? activeViewBtn.dataset.view : 'list';
    const groupBy = activeGroupBtn ? activeGroupBtn.dataset.group : 'type';
    
    // Update counter
    const failCount = safeGetById('failCount');
    if (failCount) failCount.textContent = filteredBills.length;
    
    // Update charts
    updateCharts();
    
    // Update bills display
    displayBills(viewType, groupBy);
}

/**
 * Update charts with filtered data
 */
function updateCharts() {
    const stats = calculateStats(filteredBills);
    
    // Update Failure Types Chart
    if (failureTypesChart) {
        failureTypesChart.data.labels = Object.keys(stats.failureTypes).map(type => FAILURE_TYPES[type]?.label || type);
        failureTypesChart.data.datasets[0].data = Object.values(stats.failureTypes);
        failureTypesChart.data.datasets[0].backgroundColor = Object.keys(stats.failureTypes).map(type => FAILURE_TYPES[type]?.color || '#6c757d');
        failureTypesChart.update();
    }
    
    // Update Providers Chart
    if (providersChart) {
        const providerLabels = Object.keys(stats.providers).slice(0, 5);
        const providerData = Object.values(stats.providers).slice(0, 5);
        providersChart.data.labels = providerLabels;
        providersChart.data.datasets[0].data = providerData;
        providersChart.update();
    }
    
    // Update Age Chart
    if (ageChart) {
        ageChart.data.datasets[0].data = Object.values(stats.ages);
        ageChart.update();
    }
}

/**
 * Display bills in the specified view and grouping
 */
function displayBills(viewType, groupBy) {
    console.log(`Displaying bills in ${viewType} view, grouped by ${groupBy}`);
    
    const listViewContainer = safeGetById('listViewContainer');
    const cardViewContainer = safeGetById('cardViewContainer');
    const groupViewContainer = safeGetById('groupViewContainer');
    
    if (!listViewContainer && !cardViewContainer && !groupViewContainer) {
        console.error('No view containers found');
        return;
    }
    
    // Show the appropriate view container
    if (listViewContainer) listViewContainer.style.display = viewType === 'list' ? 'block' : 'none';
    if (cardViewContainer) cardViewContainer.style.display = viewType === 'card' ? 'block' : 'none';
    if (groupViewContainer) groupViewContainer.style.display = viewType === 'group' ? 'block' : 'none';
    
    // Update active buttons
    document.querySelectorAll('.view-toggle-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.view === viewType);
    });
    document.querySelectorAll('.group-toggle-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.group === groupBy);
    });
    
    // Get container for current view
    let currentContainer;
    if (viewType === 'list') currentContainer = listViewContainer;
    else if (viewType === 'card') currentContainer = cardViewContainer;
    else if (viewType === 'group') currentContainer = groupViewContainer;
    
    if (!currentContainer) {
        console.error(`Container for view type "${viewType}" not found`);
        return;
    }
    
    currentContainer.innerHTML = '';
    
    // No bills to display
    if (filteredBills.length === 0) {
        currentContainer.innerHTML = `
            <div class="alert alert-info m-3">
                <i class="fas fa-info-circle me-2"></i>
                No failed bills match your current filter criteria.
            </div>
        `;
        return;
    }
    
    // Group the bills
    const groupedBills = groupBills(filteredBills, groupBy);
    
    // Render the appropriate view
    switch(viewType) {
        case 'list':
            renderListView(groupedBills, groupBy);
            break;
        case 'card':
            renderCardView(groupedBills, groupBy);
            break;
        case 'group':
            renderGroupView(groupedBills, groupBy);
            break;
        default:
            renderListView(groupedBills, groupBy);
    }
}

/**
 * Group bills by the specified criteria
 */
function groupBills(bills, groupBy) {
    const grouped = {};
    
    bills.forEach(bill => {
        let groupKey = 'Unknown';
        
        switch(groupBy) {
            case 'type':
                if (bill.validation_info?.failure_reasons && bill.validation_info.failure_reasons.length > 0) {
                    groupKey = bill.validation_info.failure_reasons[0].split(':')[0].trim();
                }
                break;
            case 'provider':
                groupKey = bill.filemaker?.provider?.['DBA Name Billing Name'] || 'Unknown Provider';
                break;
            case 'age':
                let oldestDOS = null;
                if (bill.filemaker?.line_items && bill.filemaker.line_items.length > 0) {
                    bill.filemaker.line_items.forEach(item => {
                        if (item.DOS) {
                            const dosDate = new Date(item.DOS);
                            if (!oldestDOS || dosDate < oldestDOS) {
                                oldestDOS = dosDate;
                            }
                        }
                    });
                    
                    if (oldestDOS) {
                        const ageInDays = Math.floor((new Date() - oldestDOS) / (1000 * 60 * 60 * 24));
                        
                        if (ageInDays <= 30) {
                            groupKey = '0-30 days';
                        } else if (ageInDays <= 60) {
                            groupKey = '31-60 days';
                        } else if (ageInDays <= 90) {
                            groupKey = '61-90 days';
                        } else {
                            groupKey = '91+ days';
                        }
                    }
                }
                break;
            default:
                if (bill.validation_info?.failure_reasons && bill.validation_info.failure_reasons.length > 0) {
                    groupKey = bill.validation_info.failure_reasons[0].split(':')[0].trim();
                }
        }
        
        if (!grouped[groupKey]) {
            grouped[groupKey] = [];
        }
        
        grouped[groupKey].push(bill);
    });
    
    return grouped;
}

/**
 * Render the list view
 */
function renderListView(groupedBills, groupBy) {
    const container = safeGetById('listViewContainer');
    if (!container) return;
    
    container.innerHTML = '';
    
    Object.keys(groupedBills).forEach(groupKey => {
        const bills = groupedBills[groupKey];
        
        // Create group header
        const groupHeader = document.createElement('div');
        groupHeader.className = 'list-group-item list-group-item-secondary d-flex justify-content-between align-items-center';
        
        let groupLabel = groupKey;
        let groupIcon = 'fa-folder';
        let groupClass = '';
        
        // Customize group header based on group type
        if (groupBy === 'type') {
            groupLabel = FAILURE_TYPES[groupKey]?.label || groupKey;
            groupIcon = FAILURE_TYPES[groupKey]?.icon || 'fa-exclamation-triangle';
            groupClass = `text-${groupKey}`;
        }
        
        groupHeader.innerHTML = `
            <div>
                <i class="fas ${groupIcon} me-2 ${groupClass}"></i>
                <strong>${groupLabel}</strong>
            </div>
            <span class="badge bg-secondary">${bills.length}</span>
        `;
        
        container.appendChild(groupHeader);
        
        // Create list items for each bill
        bills.forEach(bill => {
            const listItem = document.createElement('a');
            listItem.href = `/processing/fails/${bill.filename}`;
            listItem.className = 'list-group-item list-group-item-action d-flex justify-content-between align-items-center';
            
            // Get failure type for styling
            let failureType = 'Unknown';
            if (bill.validation_info?.failure_reasons && bill.validation_info.failure_reasons.length > 0) {
                failureType = bill.validation_info.failure_reasons[0].split(':')[0].trim();
            }
            
            // Get provider name
            const provider = bill.filemaker?.provider?.['DBA Name Billing Name'] || 'Unknown Provider';
            
            // Get service date
            let serviceDate = 'Unknown Date';
            if (bill.filemaker?.line_items && bill.filemaker.line_items.length > 0) {
                const item = bill.filemaker.line_items[0];
                if (item.DOS) {
                    serviceDate = new Date(item.DOS).toLocaleDateString();
                }
            }
            
            // Get CPT codes from failure reasons
            let cptCodes = [];
            if (bill.validation_info?.failure_reasons) {
                bill.validation_info.failure_reasons.forEach(reason => {
                    const parts = reason.split(':');
                    if (parts.length > 1) {
                        cptCodes.push(parts[1].trim());
                    }
                });
            }
            
            const cptBadges = cptCodes.map(cpt => 
                `<span class="badge bg-info me-1">${cpt}</span>`
            ).join('');
            
            listItem.innerHTML = `
                <div>
                    <div class="d-flex align-items-center">
                        <span class="me-2" style="width: 5px; height: 24px; background-color: ${FAILURE_TYPES[failureType]?.color || '#6c757d'}"></span>
                        <strong>${bill.filename}</strong>
                    </div>
                    <div class="small text-muted mt-1">
                        ${provider} • ${serviceDate} • ${cptBadges}
                    </div>
                </div>
                <div>
                    <span class="badge bg-primary rounded-pill">View</span>
                </div>
            `;
            
            container.appendChild(listItem);
        });
    });
}

/**
 * Render the card view
 */
function renderCardView(groupedBills, groupBy) {
    const container = safeGetById('cardViewContainer');
    if (!container) return;
    
    container.innerHTML = '';
    
    Object.keys(groupedBills).forEach(groupKey => {
        const bills = groupedBills[groupKey];
        
        // Create group header
        const groupHeader = document.createElement('h5');
        groupHeader.className = 'mb-3 mt-4';
        
        let groupLabel = groupKey;
        let groupIcon = 'fa-folder';
        
        // Customize group header based on group type
        if (groupBy === 'type') {
            groupLabel = FAILURE_TYPES[groupKey]?.label || groupKey;
            groupIcon = FAILURE_TYPES[groupKey]?.icon || 'fa-exclamation-triangle';
        }
        
        groupHeader.innerHTML = `
            <i class="fas ${groupIcon} me-2"></i>
            ${groupLabel} <span class="badge bg-secondary">${bills.length}</span>
        `;
        
        container.appendChild(groupHeader);
        
        // Create card row
        const cardRow = document.createElement('div');
        cardRow.className = 'row g-3';
        
        // Create cards for each bill
        bills.forEach(bill => {
            const cardCol = document.createElement('div');
            cardCol.className = 'col-md-4 col-lg-3';
            
            // Get failure type for styling
            let failureType = 'Unknown';
            let failureDetails = '';
            if (bill.validation_info?.failure_reasons && bill.validation_info.failure_reasons.length > 0) {
                failureType = bill.validation_info.failure_reasons[0].split(':')[0].trim();
                failureDetails = bill.validation_info.failure_reasons.join('<br>');
            }
            
            // Get provider name
            const provider = bill.filemaker?.provider?.['DBA Name Billing Name'] || 'Unknown Provider';
            
            // Get patient name
            const patientName = bill.patient_info?.patient_name || 'Unknown Patient';
            
            // Get service date
            let serviceDate = 'Unknown Date';
            if (bill.filemaker?.line_items && bill.filemaker.line_items.length > 0) {
                const item = bill.filemaker.line_items[0];
                if (item.DOS) {
                    serviceDate = new Date(item.DOS).toLocaleDateString();
                }
            }
            
            cardCol.innerHTML = `
                <div class="card h-100" style="border-top: 3px solid ${FAILURE_TYPES[failureType]?.color || '#6c757d'}">
                    <div class="card-header d-flex justify-content-between align-items-center">
                        <span class="badge" style="background-color: ${FAILURE_TYPES[failureType]?.color || '#6c757d'}">
                            ${FAILURE_TYPES[failureType]?.label || failureType}
                        </span>
                        <small class="text-muted">${serviceDate}</small>
                    </div>
                    <div class="card-body">
                        <h6 class="card-title text-truncate" title="${bill.filename}">${bill.filename}</h6>
                        <div class="card-text small">
                            <p class="mb-1"><strong>Provider:</strong> ${provider}</p>
                            <p class="mb-1"><strong>Patient:</strong> ${patientName}</p>
                            <p class="mb-0"><strong>Failures:</strong><br>${failureDetails}</p>
                        </div>
                    </div>
                    <div class="card-footer bg-transparent">
                        <a href="/processing/fails/${bill.filename}" class="btn btn-sm btn-primary w-100">View Details</a>
                    </div>
                </div>
            `;
            
            cardRow.appendChild(cardCol);
        });
        
        container.appendChild(cardRow);
    });
}

/**
 * Render the group view (accordion style)
 */
function renderGroupView(groupedBills, groupBy) {
    const container = safeGetById('groupViewContainer');
    if (!container) return;
    
    container.innerHTML = '';
    
    // Create accordion
    const accordion = document.createElement('div');
    accordion.className = 'accordion';
    accordion.id = 'failedBillsAccordion';
    
    // Sort group keys (customized by group type)
    let sortedKeys = Object.keys(groupedBills);
    if (groupBy === 'age') {
        const ageOrder = {
            '0-30 days': 0,
            '31-60 days': 1,
            '61-90 days': 2,
            '91+ days': 3
        };
        sortedKeys.sort((a, b) => (ageOrder[a] || 99) - (ageOrder[b] || 99));
    }
    
    // Create accordion items for each group
    sortedKeys.forEach((groupKey, index) => {
        const bills = groupedBills[groupKey];
        const accordionId = `collapse-${groupKey.replace(/[^a-z0-9]/gi, '')}`;
        
        // Create group header
        let groupLabel = groupKey;
        let groupIcon = 'fa-folder';
        let groupBadgeColor = 'bg-secondary';
        
        // Customize group header based on group type
        if (groupBy === 'type') {
            groupLabel = FAILURE_TYPES[groupKey]?.label || groupKey;
            groupIcon = FAILURE_TYPES[groupKey]?.icon || 'fa-exclamation-triangle';
            groupBadgeColor = `bg-${groupKey.toLowerCase()}`;
        }
        
        const accordionItem = document.createElement('div');
        accordionItem.className = 'accordion-item';
        
        accordionItem.innerHTML = `
            <h2 class="accordion-header" id="heading-${accordionId}">
                <button class="accordion-button ${index > 0 ? 'collapsed' : ''}" type="button" 
                       data-bs-toggle="collapse" data-bs-target="#${accordionId}" 
                       aria-expanded="${index === 0 ? 'true' : 'false'}" aria-controls="${accordionId}">
                    <i class="fas ${groupIcon} me-2"></i>
                    <strong>${groupLabel}</strong>
                    <span class="badge ${groupBadgeColor} ms-2">${bills.length}</span>
                </button>
            </h2>
            <div id="${accordionId}" class="accordion-collapse collapse ${index === 0 ? 'show' : ''}" 
                 aria-labelledby="heading-${accordionId}" data-bs-parent="#failedBillsAccordion">
                <div class="accordion-body p-0">
                    <div class="list-group list-group-flush">
                    </div>
                </div>
            </div>
        `;
        
        accordion.appendChild(accordionItem);
        
        // Add bills to group
        const listGroup = accordionItem.querySelector('.list-group');
        
        bills.forEach(bill => {
            const listItem = document.createElement('a');
            listItem.href = `/processing/fails/${bill.filename}`;
            listItem.className = 'list-group-item list-group-item-action d-flex justify-content-between align-items-center';
            
            // Get failure type for styling
            let failureType = 'Unknown';
            if (bill.validation_info?.failure_reasons && bill.validation_info.failure_reasons.length > 0) {
                failureType = bill.validation_info.failure_reasons[0].split(':')[0].trim();
            }
            
            // Get provider name
            const provider = bill.filemaker?.provider?.['DBA Name Billing Name'] || 'Unknown Provider';
            
            // Get service date
            let serviceDate = 'Unknown Date';
            if (bill.filemaker?.line_items && bill.filemaker.line_items.length > 0) {
                const item = bill.filemaker.line_items[0];
                if (item.DOS) {
                    serviceDate = new Date(item.DOS).toLocaleDateString();
                }
            }
            
            // Get main failure
            let mainFailure = 'Unknown Failure';
            if (bill.validation_info?.failure_reasons && bill.validation_info.failure_reasons.length > 0) {
                mainFailure = bill.validation_info.failure_reasons[0];
            }
            
            listItem.innerHTML = `
                <div>
                    <div class="d-flex align-items-center">
                        <span class="me-2" style="width: 4px; height: 20px; background-color: ${FAILURE_TYPES[failureType]?.color || '#6c757d'}"></span>
                        <strong>${bill.filename}</strong>
                    </div>
                    <div class="small text-muted mt-1">
                        ${provider} • ${serviceDate} • ${mainFailure}
                    </div>
                </div>
                <div>
                    <span class="badge bg-primary rounded-pill">View</span>
                </div>
            `;
            
            listGroup.appendChild(listItem);
        });
    });
    
    container.appendChild(accordion);
}

/**
 * Switch between view types
 */
function switchView(viewType) {
    const activeGroupBtn = document.querySelector('.group-toggle-btn.active');
    const groupBy = activeGroupBtn ? activeGroupBtn.dataset.group : 'type';
    displayBills(viewType, groupBy);
}

/**
 * Switch between grouping types
 */
function switchGrouping(groupBy) {
    const activeViewBtn = document.querySelector('.view-toggle-btn.active');
    const viewType = activeViewBtn ? activeViewBtn.dataset.view : 'list';
    displayBills(viewType, groupBy);
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', initDashboard);

// Export the initialization function
window.initFailedBillsDashboard = initDashboard;