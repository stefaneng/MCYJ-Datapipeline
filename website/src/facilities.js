// Facilities page - displays facility counts by grouping
const BASE_URL = import.meta.env.BASE_URL || '/';

// Active license statuses (facilities with these statuses are considered "active")
const ACTIVE_LICENSE_STATUSES = ['Regular', 'Original', '1st Provisional', '2nd Provisional', 'Inspected'];

let allFacilities = [];
let currentGrouping = 'LicenseStatus';

// Load and display data
async function init() {
    try {
        // Fetch the facility data
        const response = await fetch(`${BASE_URL}data/facilities_data.json`);
        if (!response.ok) {
            throw new Error(`Failed to load data: ${response.statusText}`);
        }
        
        allFacilities = await response.json();
        
        // Filter to only active facilities
        allFacilities = allFacilities.filter(f => 
            ACTIVE_LICENSE_STATUSES.includes(f.LicenseStatus)
        );
        
        hideLoading();
        updateStatsSummary();
        renderBarChart();
        setupGroupingSelector();
        
    } catch (error) {
        console.error('Error loading data:', error);
        showError(`Failed to load data: ${error.message}`);
        hideLoading();
    }
}

function setupGroupingSelector() {
    const selector = document.getElementById('groupingSelect');
    if (!selector) return;
    
    selector.addEventListener('change', (e) => {
        currentGrouping = e.target.value;
        updateChartTitle();
        renderBarChart();
    });
}

function updateChartTitle() {
    const titleEl = document.getElementById('chartTitle');
    if (!titleEl) return;
    
    const titles = {
        'LicenseStatus': 'Facilities by License Status',
        'AgencyType': 'Facilities by Agency Type',
        'County': 'Facilities by County'
    };
    
    titleEl.textContent = titles[currentGrouping] || 'Facilities by ' + currentGrouping;
}

function updateStatsSummary() {
    const summaryEl = document.getElementById('statsSummary');
    if (!summaryEl) return;
    
    const totalFacilities = allFacilities.length;
    const uniqueCounties = new Set(allFacilities.map(f => f.County)).size;
    const uniqueTypes = new Set(allFacilities.map(f => f.AgencyType)).size;
    
    summaryEl.innerHTML = `
        <strong>📊 Summary:</strong> 
        ${totalFacilities} active facilities across 
        ${uniqueCounties} counties and 
        ${uniqueTypes} agency types.
    `;
}

function groupFacilities(groupBy) {
    const groups = {};
    
    allFacilities.forEach(facility => {
        const key = facility[groupBy] || 'Unknown';
        if (!groups[key]) {
            groups[key] = [];
        }
        groups[key].push(facility);
    });
    
    // Convert to array and sort by count (descending)
    return Object.entries(groups)
        .map(([key, facilities]) => ({ key, count: facilities.length, facilities }))
        .sort((a, b) => b.count - a.count);
}

function renderBarChart() {
    const container = document.getElementById('barChartContainer');
    const chartDiv = document.getElementById('barChart');
    
    if (!container || !chartDiv) return;
    
    const grouped = groupFacilities(currentGrouping);
    
    if (grouped.length === 0) {
        container.innerHTML = '<div style="color: #666; font-size: 0.9em; font-style: italic; padding: 20px; text-align: center;">No facility data available</div>';
        chartDiv.style.display = 'block';
        return;
    }
    
    // Find max count for scaling
    const maxCount = Math.max(...grouped.map(g => g.count));
    
    // Build bar chart HTML
    const barsHtml = grouped.map(item => {
        const percentage = maxCount > 0 ? (item.count / maxCount) * 100 : 0;
        const encodedValue = encodeURIComponent(item.key);
        const filterParam = currentGrouping.toLowerCase();
        
        return `
            <div class="bar-chart-row">
                <a href="${BASE_URL}?${filterParam}=${encodedValue}" class="bar-chart-label" title="View agencies with ${currentGrouping}: ${escapeHtml(item.key)}">${escapeHtml(item.key)}</a>
                <div class="bar-chart-bar-container">
                    <div class="bar-chart-bar" style="width: ${percentage}%"></div>
                </div>
                <div class="bar-chart-count">${item.count}</div>
            </div>
        `;
    }).join('');
    
    container.innerHTML = barsHtml;
    chartDiv.style.display = 'block';
}

function hideLoading() {
    const loadingEl = document.getElementById('loading');
    if (loadingEl) {
        loadingEl.style.display = 'none';
    }
}

function showError(message) {
    const loadingEl = document.getElementById('loading');
    if (loadingEl) {
        loadingEl.textContent = `Error: ${message}`;
        loadingEl.style.color = '#e74c3c';
    }
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Initialize the page
init();
