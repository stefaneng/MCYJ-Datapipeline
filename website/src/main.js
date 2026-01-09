// Main application logic
import { queryDeepSeek } from './apiService.js';
import { Trie } from './trie.js';

// Constants
const DOM_READY_DELAY = 100; // Delay in ms to ensure DOM is ready for operations
const BASE_URL = import.meta.env.BASE_URL || '/';

// Active license statuses (facilities with these statuses are considered "active")
const ACTIVE_LICENSE_STATUSES = ['Regular', 'Original', '1st Provisional', '2nd Provisional', 'Inspected'];

let allAgencies = [];
let filteredAgencies = [];
let currentOpenAgencyId = null;
let currentDocumentData = null; // Store current document for queries
let apiKey = null; // Store decrypted API key

// Filter state
let filters = {
    sirOnly: true, // Enable SIR-only by default
    keywords: [], // Multiple selected keywords (OR semantics)
    agency: null, // Single selected agency (agencyId)
    activeLicenseOnly: true, // Only show agencies with active license by default
    licenseStatus: null, // Filter by specific license status
    agencyType: null, // Filter by agency type
    county: null // Filter by county
};

let keywordTrie = new Trie();
let agencyTrie = new Trie();
let allKeywords = new Set();
let agencyIdMap = new Map(); // Maps lowercase agency text to original agencyId

// Unique values for facility filters
let uniqueLicenseStatuses = [];
let uniqueAgencyTypes = [];
let uniqueCounties = [];

// Load and display data
async function init() {
    try {
        // Fetch the agency data
        const response = await fetch(`${BASE_URL}data/agencies_data.json`);
        if (!response.ok) {
            throw new Error(`Failed to load data: ${response.statusText}`);
        }
        
        allAgencies = await response.json();
        filteredAgencies = allAgencies;
        
        // Build keyword trie from all documents
        buildKeywordTrie();
        
        // Build agency trie from all agencies
        buildAgencyTrie();
        
        // Build unique values for facility filters
        buildFacilityFilterOptions();

        hideLoading();
        displayStats();
        displayAgencies(allAgencies);
        setupFilters();
        setupKeywordFilter();
        setupAgencyFilter();
        setupFacilityFilters();
        
        // Handle URL query string before applying filters to capture facility filter params
        handleUrlQueryString();
        
        // Apply filters to respect default settings (SIR-only and active license by default)
        applyFilters();
        
        handleQueryStringDocument();
        
        // Set commit hash
        setCommitHash();
        
    } catch (error) {
        console.error('Error loading data:', error);
        showError(`Failed to load data: ${error.message}`);
        hideLoading();
    }
}

function hideLoading() {
    document.getElementById('loading').style.display = 'none';
}

function showError(message) {
    const errorEl = document.getElementById('error');
    errorEl.textContent = message;
    errorEl.style.display = 'block';
}

function displayStats() {
    const statsEl = document.getElementById('statsCount');
    
    // Use filtered agencies for stats
    const agencies = filteredAgencies;
    const totalAgencies = agencies.length;
    const totalReports = agencies.reduce((sum, a) => sum + a.total_reports, 0);
    
    if (statsEl) {
        statsEl.textContent = `Showing ${totalAgencies} ${totalAgencies === 1 ? 'agency' : 'agencies'} with ${totalReports} ${totalReports === 1 ? 'document' : 'documents'}`;
    }
}

function applyFilters() {
    // Start with all agencies
    let agencies = JSON.parse(JSON.stringify(allAgencies)); // Deep clone
    
    // Store the selected agency ID for auto-open (before filtering)
    let selectedAgencyId = null;
    
    // Filter by selected agency first
    if (filters.agency) {
        agencies = agencies.filter(agency => agency.agencyId === filters.agency);
        
        // Store the agency ID if there's exactly one agency selected
        if (agencies.length === 1) {
            selectedAgencyId = agencies[0].agencyId;
        }
    }
    
    // Apply facility-level filters (filter agencies based on their facility info)
    agencies = agencies.filter(agency => {
        const facility = agency.facility;
        
        // Filter by active license only
        if (filters.activeLicenseOnly) {
            if (!facility || !ACTIVE_LICENSE_STATUSES.includes(facility.LicenseStatus)) {
                return false;
            }
        }
        
        // Filter by specific license status
        if (filters.licenseStatus) {
            if (!facility || facility.LicenseStatus !== filters.licenseStatus) {
                return false;
            }
        }
        
        // Filter by agency type
        if (filters.agencyType) {
            if (!facility || facility.AgencyType !== filters.agencyType) {
                return false;
            }
        }
        
        // Filter by county
        if (filters.county) {
            if (!facility || facility.County !== filters.county) {
                return false;
            }
        }
        
        return true;
    });
    
    // Apply filters to each agency's documents
    agencies = agencies.map(agency => {
        if (!agency.documents || !Array.isArray(agency.documents)) {
            return agency;
        }
        
        const originalReportCount = agency.documents.length;
        
        let filteredDocuments = agency.documents.filter(d => {
            // Filter by SIR only
            if (filters.sirOnly && !d.is_special_investigation) {
                return false;
            }
            
            // Filter by keywords (OR semantics - match if document contains ANY selected keyword)
            if (filters.keywords.length > 0) {
                const docKeywords = d.sir_violation_level?.keywords || [];
                const docKeywordsLower = docKeywords.map(k => k.toLowerCase());
                const hasAnyKeyword = filters.keywords.some(filterKeyword => 
                    docKeywordsLower.includes(filterKeyword.toLowerCase())
                );
                if (!hasAnyKeyword) {
                    return false;
                }
            }
            
            return true;
        });
        
        const filteredReportCount = filteredDocuments.length;
        const filteredOutCount = originalReportCount - filteredReportCount;
        
        // Update agency stats based on filtered documents
        return {
            ...agency,
            documents: filteredDocuments,
            total_reports: filteredReportCount,
            original_total_reports: originalReportCount,
            filtered_out_count: filteredOutCount
        };
    });
    
    // If a specific agency is selected, keep it even if it has no reports after filtering
    // Otherwise, remove agencies with no reports after filtering
    if (!filters.agency) {
        agencies = agencies.filter(agency => agency.total_reports > 0);
    }
    
    filteredAgencies = agencies;
    displayStats();
    displayAgencies(filteredAgencies);
    
    // Auto-open the agency card if a specific agency is selected
    if (selectedAgencyId) {
        // Use setTimeout to ensure DOM is ready
        setTimeout(() => {
            openAgencyCard(selectedAgencyId);
        }, DOM_READY_DELAY);
    }
}

function setupFilters() {
    // SIR only filter
    const sirOnlyCheckbox = document.getElementById('filterSirOnly');
    sirOnlyCheckbox.addEventListener('change', (e) => {
        filters.sirOnly = e.target.checked;
        applyFilters();
    });
    
    // Active license only filter
    const activeLicenseCheckbox = document.getElementById('filterActiveLicenseOnly');
    if (activeLicenseCheckbox) {
        activeLicenseCheckbox.addEventListener('change', (e) => {
            filters.activeLicenseOnly = e.target.checked;
            applyFilters();
        });
    }
}

function buildFacilityFilterOptions() {
    // Build unique values for facility filters from agencies
    const licenseStatuses = new Set();
    const agencyTypes = new Set();
    const counties = new Set();
    
    allAgencies.forEach(agency => {
        if (agency.facility) {
            if (agency.facility.LicenseStatus) {
                licenseStatuses.add(agency.facility.LicenseStatus);
            }
            if (agency.facility.AgencyType) {
                agencyTypes.add(agency.facility.AgencyType);
            }
            if (agency.facility.County) {
                counties.add(agency.facility.County);
            }
        }
    });
    
    uniqueLicenseStatuses = Array.from(licenseStatuses).sort();
    uniqueAgencyTypes = Array.from(agencyTypes).sort();
    uniqueCounties = Array.from(counties).sort();
}

function setupFacilityFilters() {
    // Setup License Status filter
    const licenseStatusSelect = document.getElementById('filterLicenseStatus');
    if (licenseStatusSelect) {
        // Populate options
        licenseStatusSelect.innerHTML = '<option value="">All Statuses</option>' +
            uniqueLicenseStatuses.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('');
        
        licenseStatusSelect.addEventListener('change', (e) => {
            filters.licenseStatus = e.target.value || null;
            updateUrlWithFacilityFilters();
            applyFilters();
        });
    }
    
    // Setup Agency Type filter
    const agencyTypeSelect = document.getElementById('filterAgencyType');
    if (agencyTypeSelect) {
        // Populate options
        agencyTypeSelect.innerHTML = '<option value="">All Types</option>' +
            uniqueAgencyTypes.map(t => `<option value="${escapeHtml(t)}">${escapeHtml(t)}</option>`).join('');
        
        agencyTypeSelect.addEventListener('change', (e) => {
            filters.agencyType = e.target.value || null;
            updateUrlWithFacilityFilters();
            applyFilters();
        });
    }
    
    // Setup County filter
    const countySelect = document.getElementById('filterCounty');
    if (countySelect) {
        // Populate options
        countySelect.innerHTML = '<option value="">All Counties</option>' +
            uniqueCounties.map(c => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join('');
        
        countySelect.addEventListener('change', (e) => {
            filters.county = e.target.value || null;
            updateUrlWithFacilityFilters();
            applyFilters();
        });
    }
}

function updateUrlWithFacilityFilters() {
    const url = new URL(window.location);
    
    // Update License Status
    if (filters.licenseStatus) {
        url.searchParams.set('licensestatus', filters.licenseStatus);
    } else {
        url.searchParams.delete('licensestatus');
    }
    
    // Update Agency Type
    if (filters.agencyType) {
        url.searchParams.set('agencytype', filters.agencyType);
    } else {
        url.searchParams.delete('agencytype');
    }
    
    // Update County
    if (filters.county) {
        url.searchParams.set('county', filters.county);
    } else {
        url.searchParams.delete('county');
    }
    
    window.history.pushState({}, '', url);
}

function buildKeywordTrie() {
    // Collect all keywords from all documents
    // Split each keyword by whitespace to allow partial word matching
    allAgencies.forEach(agency => {
        if (agency.documents && Array.isArray(agency.documents)) {
            agency.documents.forEach(doc => {
                if (doc.sir_violation_level && doc.sir_violation_level.keywords && Array.isArray(doc.sir_violation_level.keywords)) {
                    doc.sir_violation_level.keywords.forEach(keyword => {
                        // Insert the full keyword phrase and mark it as a full keyword
                        keywordTrie.insert(keyword, true, keyword);
                        allKeywords.add(keyword.toLowerCase());
                        
                        // Also insert individual words from the keyword for search purposes
                        // These link back to the full keyword so typing "inj" can find "serious injury"
                        const words = keyword.trim().split(/\s+/);
                        words.forEach(word => {
                            if (word.length > 0) {
                                keywordTrie.insert(word, false, keyword);
                            }
                        });
                    });
                }
            });
        }
    });
    console.log(`Built keyword trie with ${allKeywords.size} unique keywords`);
}

function buildAgencyTrie() {
    allAgencies.forEach(agency => {
        if (agency.AgencyName && agency.agencyId) {
            // Insert agency name and ID into the trie
            // Store agencyId as the lookup value
            const searchText = `${agency.AgencyName} (${agency.agencyId})`;
            agencyTrie.insert(searchText, true, searchText);
            
            // Map lowercase searchText to original agencyId for case-insensitive lookup
            agencyIdMap.set(searchText.toLowerCase(), agency.agencyId);
            
            // Also insert individual words from agency name for search
            const words = agency.AgencyName.trim().split(/\s+/);
            words.forEach(word => {
                if (word.length > 0) {
                    agencyTrie.insert(word, false, searchText);
                }
            });
            
            // Insert agency ID for direct ID search
            agencyTrie.insert(agency.agencyId, false, searchText);
        }
    });
    console.log(`Built agency trie with ${allAgencies.length} agencies`);
}

function setupKeywordFilter() {
    const keywordInput = document.getElementById('keywordFilterInput');
    const keywordSuggestions = document.getElementById('keywordSuggestions');
    const selectedKeywordContainer = document.getElementById('selectedKeyword');
    
    if (!keywordInput) return;
    
    // Handle input for autocomplete
    keywordInput.addEventListener('input', (e) => {
        const query = e.target.value.trim();
        
        if (query.length < 2) {
            keywordSuggestions.style.display = 'none';
            return;
        }
        
        const suggestions = keywordTrie.search(query);
        
        if (suggestions.length === 0) {
            keywordSuggestions.style.display = 'none';
            return;
        }
        
        keywordSuggestions.innerHTML = suggestions.map(s => `
            <div class="keyword-suggestion" data-keyword="${escapeHtml(s.keyword)}">
                <span>${escapeHtml(s.keyword)}</span>
                <span style="color: #666; font-size: 0.85em;">(${s.count})</span>
            </div>
        `).join('');
        keywordSuggestions.style.display = 'block';
        
        // Add click handlers to suggestions
        keywordSuggestions.querySelectorAll('.keyword-suggestion').forEach(div => {
            div.addEventListener('click', () => {
                const keyword = div.dataset.keyword;
                addKeywordFilter(keyword);
                keywordInput.value = '';
                keywordSuggestions.style.display = 'none';
            });
        });
    });
    
    // Hide suggestions when clicking outside
    document.addEventListener('click', (e) => {
        if (!e.target.closest('#keywordFilterInput') && !e.target.closest('#keywordSuggestions')) {
            keywordSuggestions.style.display = 'none';
        }
    });
    
    // Allow pressing Enter to add the first suggestion
    keywordInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            const firstSuggestion = keywordSuggestions.querySelector('.keyword-suggestion');
            if (firstSuggestion) {
                const keyword = firstSuggestion.dataset.keyword;
                addKeywordFilter(keyword);
                keywordInput.value = '';
                keywordSuggestions.style.display = 'none';
            }
        }
    });
}

function addKeywordFilter(keyword) {
    const keywordLower = keyword.toLowerCase();
    
    // Check if keyword is already in the list
    if (!filters.keywords.some(k => k.toLowerCase() === keywordLower)) {
        filters.keywords.push(keyword);
        renderSelectedKeywords();
        updateUrlWithKeywords();
        applyFilters();
    }
}

function removeKeywordFilter(keyword) {
    const keywordLower = keyword.toLowerCase();
    filters.keywords = filters.keywords.filter(k => k.toLowerCase() !== keywordLower);
    renderSelectedKeywords();
    updateUrlWithKeywords();
    applyFilters();
}

function clearAllKeywords() {
    filters.keywords = [];
    renderSelectedKeywords();
    updateUrlWithKeywords();
    applyFilters();
}

function updateUrlWithKeywords() {
    const url = new URL(window.location);
    url.searchParams.delete('keyword');
    url.searchParams.delete('keywords');
    
    if (filters.keywords.length > 0) {
        // Use comma-separated keywords in a single parameter
        url.searchParams.set('keywords', filters.keywords.join(','));
    }
    
    window.history.pushState({}, '', url);
}

function renderSelectedKeywords() {
    const container = document.getElementById('selectedKeyword');
    const input = document.getElementById('keywordFilterInput');

    if (!container || !input) return;

    if (filters.keywords.length === 0) {
        container.innerHTML = '<div style="color: #666; font-size: 0.9em; font-style: italic;">No keywords selected</div>';
    } else {
        // Show "OR" label prominently when multiple keywords are selected
        const orLabel = filters.keywords.length > 1 
            ? '<div style="color: #e67e22; font-weight: 600; font-size: 0.85em; margin-bottom: 6px;">🔍 Showing documents matching ANY of these keywords (OR):</div>'
            : '';
        
        const badges = filters.keywords.map(kw => `
            <span class="selected-keyword-badge">
                ${escapeHtml(kw)}
                <button class="remove-keyword-btn" data-keyword="${escapeHtml(kw)}" title="Remove keyword">✕</button>
            </span>
        `).join('');
        
        const clearAllBtn = filters.keywords.length > 1 
            ? '<button id="clearAllKeywordsBtn" style="background: #e74c3c; color: white; border: none; padding: 6px 12px; border-radius: 16px; font-size: 0.85em; cursor: pointer; margin-left: 6px;">Clear All</button>'
            : '';
        
        container.innerHTML = orLabel + '<div style="display: flex; flex-wrap: wrap; gap: 6px; align-items: center;">' + badges + clearAllBtn + '</div>';
        
        // Attach event listeners after rendering
        container.querySelectorAll('.remove-keyword-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const keyword = btn.dataset.keyword;
                removeKeywordFilter(keyword);
            });
        });
        
        const clearAllButton = document.getElementById('clearAllKeywordsBtn');
        if (clearAllButton) {
            clearAllButton.addEventListener('click', (e) => {
                e.stopPropagation();
                clearAllKeywords();
            });
        }
    }
}

function setupAgencyFilter() {
    const agencyInput = document.getElementById('agencyFilterInput');
    const agencySuggestions = document.getElementById('agencySuggestions');
    const selectedAgencyContainer = document.getElementById('selectedAgency');
    
    if (!agencyInput) return;
    
    // Handle input for autocomplete
    agencyInput.addEventListener('input', (e) => {
        const query = e.target.value.trim();
        
        // If an agency is already selected, hide input and show selected
        if (filters.agency) {
            return;
        }
        
        if (query.length < 2) {
            agencySuggestions.style.display = 'none';
            return;
        }
        
        const suggestions = agencyTrie.search(query);
        
        if (suggestions.length === 0) {
            agencySuggestions.style.display = 'none';
            return;
        }
        
        agencySuggestions.innerHTML = suggestions.map(s => {
            // Get the original agencyId from the map (case-sensitive)
            const agencyId = agencyIdMap.get(s.keyword.toLowerCase()) || '';
            return `
                <div class="agency-suggestion" data-agency-text="${escapeHtml(s.keyword)}" data-agency-id="${escapeHtml(agencyId)}">
                    <span>${escapeHtml(s.keyword)}</span>
                </div>
            `;
        }).join('');
        agencySuggestions.style.display = 'block';
        
        // Add click handlers to suggestions
        agencySuggestions.querySelectorAll('.agency-suggestion').forEach(div => {
            div.addEventListener('click', () => {
                const agencyText = div.dataset.agencyText;
                const agencyId = div.dataset.agencyId;
                setAgencyFilter(agencyText, agencyId);
                agencyInput.value = '';
                agencySuggestions.style.display = 'none';
            });
        });
    });
    
    // Hide suggestions when clicking outside
    document.addEventListener('click', (e) => {
        if (!e.target.closest('#agencyFilterInput') && !e.target.closest('#agencySuggestions')) {
            agencySuggestions.style.display = 'none';
        }
    });
    
    // Allow pressing Enter to add the first suggestion
    agencyInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            const firstSuggestion = agencySuggestions.querySelector('.agency-suggestion');
            if (firstSuggestion) {
                const agencyText = firstSuggestion.dataset.agencyText;
                const agencyId = firstSuggestion.dataset.agencyId;
                setAgencyFilter(agencyText, agencyId);
                agencyInput.value = '';
                agencySuggestions.style.display = 'none';
            }
        }
    });
}

function setAgencyFilter(agencyText, agencyId, skipUrlUpdate = false) {
    filters.agency = agencyId;
    renderSelectedAgency(agencyText);
    
    // Update URL query string unless we're restoring from URL
    if (!skipUrlUpdate) {
        const url = new URL(window.location);
        url.searchParams.set('agency', agencyId);
        window.history.pushState({}, '', url);
    }
    
    applyFilters();
}

function removeAgencyFilter() {
    filters.agency = null;
    currentOpenAgencyId = null; // Reset the currently open agency so it can be reopened
    renderSelectedAgency(null);
    
    // Remove agency from URL query string
    const url = new URL(window.location);
    url.searchParams.delete('agency');
    window.history.pushState({}, '', url);
    
    applyFilters();
}

function renderSelectedAgency(agencyText) {
    const container = document.getElementById('selectedAgency');
    const input = document.getElementById('agencyFilterInput');

    if (!container || !input) return;

    if (!filters.agency) {
        container.innerHTML = '<div style="color: #666; font-size: 0.9em; font-style: italic;">No agency selected</div>';
        input.style.display = 'block';
    } else {
        container.innerHTML = `
            <span class="selected-keyword-badge">
                ${escapeHtml(agencyText || filters.agency)}
                <button class="remove-keyword-btn" onclick="window.removeAgencyFilter()" title="Remove agency">✕</button>
            </span>
        `;
        input.style.display = 'none';
    }
}

// Export functions to window for inline onclick handlers
window.removeAgencyFilter = removeAgencyFilter;

function displayAgencies(agencies) {
    const agenciesEl = document.getElementById('agencies');
    const noResultsEl = document.getElementById('noResults');
    
    if (agencies.length === 0) {
        agenciesEl.innerHTML = '';
        noResultsEl.style.display = 'block';
        return;
    }
    
    noResultsEl.style.display = 'none';
    
    agenciesEl.innerHTML = agencies.map(agency => {
        return `
            <div class="agency-card" id="agency-${agency.agencyId}" data-agency-id="${agency.agencyId}">
                <div class="agency-header">
                    <div>
                        <div class="agency-name">
                            ${escapeHtml(agency.AgencyName || 'Unknown Agency')}
                            <button class="copy-link-btn" onclick="copyAgencyLink('${agency.agencyId}', event)" title="Copy link to this agency">
                                🔗
                            </button>
                        </div>
                        <div style="color: #666; font-size: 0.9em; margin-top: 4px;">ID: ${escapeHtml(agency.agencyId)}</div>
                    </div>
                </div>
                
                <div class="agency-stats">
                    <span class="stat-badge reports-badge">
                        📋 ${agency.total_reports} ${agency.total_reports === 1 ? 'Report' : 'Reports'}${agency.filtered_out_count > 0 ? ` <span style="color: #e67e22;">(${agency.filtered_out_count} filtered out)</span>` : ''}
                    </span>
                </div>
                
                <div class="agency-details" id="details-${agency.agencyId}">
                    ${renderDocuments(agency)}
                </div>
            </div>
        `;
    }).join('');
    
    // Add click handlers to expand/collapse details
    document.querySelectorAll('.agency-card').forEach(card => {
        card.addEventListener('click', (e) => {
            // Don't toggle if clicking on the copy link button
            if (e.target.closest('.copy-link-btn')) {
                return;
            }
            
            const agencyId = card.dataset.agencyId;
            openAgencyCard(agencyId);
        });
    });
}

function renderDocuments(agency) {
    const documents = agency.documents || [];
    const filteredOutCount = agency.filtered_out_count || 0;
    
    if (documents.length === 0) {
        // Show different message depending on whether documents were filtered out
        const noDocsMessage = filteredOutCount > 0 
            ? `<p style="color: #e67e22; background: #fff3cd; padding: 12px; border-radius: 4px; border-left: 3px solid #f39c12;">
                   <strong>All ${filteredOutCount} ${filteredOutCount === 1 ? 'report' : 'reports'} have been filtered out.</strong><br>
                   <span style="font-size: 0.9em;">Try adjusting the filters above to see more reports.</span>
               </p>`
            : `<p style="color: #666;">No reports available.</p>`;
        
        return `
            <div class="documents-list">
                <div class="section-title">Documents & Reports</div>
                ${noDocsMessage}
            </div>
        `;
    }
    
    // Sort by date (most recent first)
    const sortedDocuments = [...documents].sort((a, b) => {
        return new Date(b.date_processed) - new Date(a.date_processed);
    });
    
    const documentItems = sortedDocuments.map(d => {
        // Use document title if available, otherwise fall back to agency name
        const displayTitle = d.document_title || d.agency_name || 'Untitled Document';
        const isSir = d.is_special_investigation;
        const hasSummary = d.sir_summary && d.sir_summary.summary;
        const hasViolationLevel = d.sir_violation_level && d.sir_violation_level.level;
        
        // Determine violation level badge
        let violationLevelBadge = '';
        if (hasViolationLevel) {
            const level = d.sir_violation_level.level.toLowerCase();
            let levelColor = '#95a5a6';
            let levelEmoji = '⚪';
            
            if (level === 'low') {
                levelColor = '#f39c12';
                levelEmoji = '🟡';
            } else if (level === 'moderate') {
                levelColor = '#e67e22';
                levelEmoji = '🟠';
            } else if (level === 'severe') {
                levelColor = '#e74c3c';
                levelEmoji = '🔴';
            }
            
            violationLevelBadge = `<span style="color: ${levelColor}; font-size: 0.85em; margin-left: 6px;">${levelEmoji} ${level.charAt(0).toUpperCase() + level.slice(1)}</span>`;
        }
        
        return `
            <div class="document-item ${isSir ? 'is-sir' : ''}">
                <div style="font-weight: 600; margin-bottom: 4px;">
                    ${escapeHtml(displayTitle)}
                    ${isSir ? ' <span style="color: #e74c3c; font-size: 0.85em;">🔍 SIR</span>' : ''}
                    ${d.sha256 ? `
                        <button class="copy-link-btn" onclick="copyDocumentLink('${d.sha256}', event)" title="Copy link to this document">
                            🔗
                        </button>
                    ` : ''}
                </div>
                <div class="date">${escapeHtml(d.date || 'Date not specified')}</div>
                ${hasSummary ? `
                    <div style="margin-top: 10px; padding: 10px; background: #fff9e6; border-left: 3px solid #f39c12; border-radius: 4px;">
                        <div style="font-weight: 600; color: #e67e22; margin-bottom: 6px; font-size: 0.9em;">
                            📋 Summary (AI-generated by DeepSeek v3.2)
                            ${d.sir_summary.violation === 'y' ? `<span style="color: #e74c3c; margin-left: 6px;">⚠️ Violation Substantiated${violationLevelBadge}</span>` : ''}
                            ${d.sir_summary.violation === 'n' ? '<span style="color: #27ae60; margin-left: 6px;">✓ No Violation</span>' : ''}
                        </div>
                        <div style="font-size: 0.9em; line-height: 1.5; color: #555;">${escapeHtml(d.sir_summary.summary)}</div>
                        ${hasViolationLevel && d.sir_violation_level.keywords && d.sir_violation_level.keywords.length > 0 ? `
                            <div style="margin-top: 8px; display: flex; flex-wrap: wrap; gap: 4px;">
                                <span style="font-size: 0.8em; color: #666; margin-right: 4px;">🏷️</span>
                                ${d.sir_violation_level.keywords.slice(0, 5).map(kw => 
                                    `<span style="background: #e8f4f8; color: #2980b9; padding: 2px 8px; border-radius: 10px; font-size: 0.75em; border: 1px solid #3498db;">${escapeHtml(kw)}</span>`
                                ).join('')}
                                ${d.sir_violation_level.keywords.length > 5 ? `<span style="font-size: 0.75em; color: #666;">+${d.sir_violation_level.keywords.length - 5} more</span>` : ''}
                            </div>
                        ` : ''}
                    </div>
                ` : ''}
                ${d.sha256 ? `
                    <div style="margin-top: 8px;">
                        <a href="${BASE_URL}document.html?sha=${d.sha256}" target="_blank" class="view-document-btn" style="text-decoration: none; display: inline-block;">
                            📄 View Full Document
                        </a>
                    </div>
                ` : ''}
            </div>
        `;
    }).join('');
    
    return `
        <div class="documents-list">
            <div class="section-title">Documents & Reports (${documents.length})</div>
            ${documentItems}
        </div>
    `;
}

async function viewDocument(sha256, event) {
    if (event) {
        event.stopPropagation();
    }
    
    try {
        const response = await fetch(`${BASE_URL}documents/${sha256}.json`);
        if (!response.ok) {
            throw new Error(`Failed to load document: ${response.statusText}`);
        }
        
        const docData = await response.json();
        
        // Find document metadata from agencies data
        let docMetadata = null;
        for (const agency of allAgencies) {
            if (agency.documents && Array.isArray(agency.documents)) {
                const document = agency.documents.find(d => d.sha256 === sha256);
                if (document) {
                    docMetadata = {
                        title: document.document_title || document.agency_name || 'Untitled Document',
                        is_special_investigation: document.is_special_investigation || false
                    };
                    break;
                }
            }
        }
        
        showDocumentModal(docData, docMetadata);
    } catch (error) {
        console.error('Error loading document:', error);
        alert(`Failed to load document: ${error.message}`);
    }
}

function highlightText(text, highlightRanges) {
    // highlightRanges is an array of {start, end, className} objects
    if (!highlightRanges || highlightRanges.length === 0) {
        return escapeHtml(text);
    }
    
    // Sort ranges by start position
    const sortedRanges = [...highlightRanges].sort((a, b) => a.start - b.start);
    
    let result = '';
    let lastIndex = 0;
    
    for (const range of sortedRanges) {
        // Add text before highlight
        if (range.start > lastIndex) {
            result += escapeHtml(text.substring(lastIndex, range.start));
        }
        
        // Add highlighted text
        const highlightedText = escapeHtml(text.substring(range.start, range.end));
        result += `<mark class="${range.className}">${highlightedText}</mark>`;
        
        lastIndex = range.end;
    }
    
    // Add remaining text
    if (lastIndex < text.length) {
        result += escapeHtml(text.substring(lastIndex));
    }
    
    return result;
}

function findTextPositions(text, pattern, flags = 'gi') {
    const positions = [];
    const regex = new RegExp(pattern, flags);
    let match;
    
    while ((match = regex.exec(text)) !== null) {
        positions.push({
            start: match.index,
            end: match.index + match[0].length
        });
    }
    
    return positions;
}

function showDocumentModal(docData, docMetadata) {
    const modal = document.getElementById('documentModal') || createDocumentModal();
    const modalContent = modal.querySelector('.modal-document-content');
    
    // Store current document data for queries
    currentDocumentData = docData;
    
    // Validate document data
    if (!docData.pages || !Array.isArray(docData.pages)) {
        console.error('Invalid document data: pages array missing or invalid');
        return;
    }
    
    // Format the document pages
    const totalPages = docData.pages.length;
    const pagesHtml = docData.pages.map((page, pageIndex) => {
        const pageContent = escapeHtml(page);
        
        return `
            <div class="document-page">
                <div class="page-number">Page ${pageIndex + 1} of ${totalPages}</div>
                <pre class="page-text">${pageContent}</pre>
            </div>
        `;
    }).join('');
    
    modalContent.innerHTML = `
        <div class="document-header">
            <h2>Document Details</h2>
            <button class="close-modal" onclick="closeDocumentModal()">✕</button>
        </div>
        <div class="document-info">
            ${docMetadata ? `
                <div><strong>Title:</strong> ${escapeHtml(docMetadata.title)}</div>
                ${docMetadata.is_special_investigation ? `
                    <div style="color: #e74c3c;"><strong>Type:</strong> 🔍 Special Investigation Report</div>
                ` : ''}
            ` : ''}
            <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
                <button class="copy-link-btn" onclick="copyDocumentLink('${docData.sha256}', event)" title="Copy link to this document" style="padding: 8px 16px; background: #3498db; color: white; opacity: 1; font-size: 0.9em;">
                    🔗 Copy URL to this document
                </button>
            </div>
            <div><strong>Date Processed:</strong> ${escapeHtml(docData.dateprocessed)}</div>
            <div><strong>Total Pages:</strong> ${totalPages}</div>
        </div>
        
        ${docData.sir_summary && docData.sir_summary.summary ? `
            <!-- SIR Summary Section -->
            <div style="padding: 20px; background: #fffbf0; border-bottom: 2px solid #f39c12;">
                <div style="margin-bottom: 15px;">
                    <h3 style="margin: 0 0 10px 0; color: #e67e22; font-size: 1.1em;">
                        📋 Special Investigation Report Summary (AI-generated by DeepSeek v3.2)
                        ${docData.sir_summary.violation === 'y' ? `<span style="color: #e74c3c; margin-left: 8px; font-size: 0.9em;">⚠️ Violation Substantiated</span>` : ''}
                        ${docData.sir_summary.violation === 'n' ? '<span style="color: #27ae60; margin-left: 8px; font-size: 0.9em;">✓ No Violation</span>' : ''}
                        ${docData.sir_violation_level && docData.sir_violation_level.level ? (() => {
                            const level = docData.sir_violation_level.level.toLowerCase();
                            let levelColor = '#95a5a6';
                            let levelEmoji = '⚪';
                            
                            if (level === 'low') {
                                levelColor = '#f39c12';
                                levelEmoji = '🟡';
                            } else if (level === 'moderate') {
                                levelColor = '#e67e22';
                                levelEmoji = '🟠';
                            } else if (level === 'severe') {
                                levelColor = '#e74c3c';
                                levelEmoji = '🔴';
                            }
                            
                            return `<span style="color: ${levelColor}; margin-left: 8px; font-size: 0.9em;">${levelEmoji} ${level.charAt(0).toUpperCase() + level.slice(1)} Severity</span>`;
                        })() : ''}
                    </h3>
                </div>
                <div style="background: white; padding: 15px; border-radius: 6px; border-left: 4px solid #f39c12; line-height: 1.6; color: #333;">
                    <div style="margin-bottom: ${docData.sir_violation_level && (docData.sir_violation_level.justification || (docData.sir_violation_level.keywords && docData.sir_violation_level.keywords.length > 0)) ? '15px' : '0'};">
                        <strong style="color: #2c3e50;">Summary:</strong>
                        <div style="margin-top: 8px;">${escapeHtml(docData.sir_summary.summary)}</div>
                    </div>
                    ${docData.sir_violation_level && docData.sir_violation_level.keywords && docData.sir_violation_level.keywords.length > 0 ? `
                        <div style="padding-top: 15px; border-top: 1px solid #ecf0f1; margin-bottom: ${docData.sir_violation_level.justification ? '15px' : '0'};">
                            <strong style="color: #2c3e50;">Keywords:</strong>
                            <div style="margin-top: 8px; display: flex; flex-wrap: wrap; gap: 6px;">
                                ${docData.sir_violation_level.keywords.map(kw => 
                                    `<span style="background: #e8f4f8; color: #2980b9; padding: 4px 10px; border-radius: 12px; font-size: 0.85em; border: 1px solid #3498db;">${escapeHtml(kw)}</span>`
                                ).join('')}
                            </div>
                        </div>
                    ` : ''}
                    ${docData.sir_violation_level && docData.sir_violation_level.justification ? `
                        <div style="padding-top: 15px; border-top: 1px solid #ecf0f1;">
                            <strong style="color: #2c3e50;">Severity Justification:</strong>
                            <div style="margin-top: 8px;">${escapeHtml(docData.sir_violation_level.justification)}</div>
                        </div>
                    ` : ''}
                </div>
            </div>
        ` : ''}
        
        <!-- AI Query Section -->
        <div id="aiQuerySection" style="padding: 20px; background: #f8f9fa; border-bottom: 1px solid #ecf0f1;">
            <div style="margin-bottom: 15px;">
                <h3 style="margin: 0 0 10px 0; color: #2c3e50; font-size: 1.1em;">🤖 Ask AI About This Document</h3>
                <p style="margin: 0; color: #666; font-size: 0.9em;">Query DeepSeek v3.2 about this document. <strong>Note:</strong> Results are displayed but not stored. Your API key is used directly and never saved by this application.</p>
            </div>
            
            <div id="apiKeyPrompt" style="margin-bottom: 15px;">
                <div style="display: flex; gap: 10px; align-items: flex-start;">
                    <input 
                        type="password" 
                        id="apiKeyInput" 
                        placeholder="Enter your OpenRouter API key..."
                        style="flex: 1; padding: 10px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px;"
                        autocomplete="off"
                    />
                    <button 
                        id="setApiKeyBtn"
                        onclick="setApiKey()"
                        style="padding: 10px 20px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 14px; white-space: nowrap;"
                    >
                        ✓ Set Key
                    </button>
                </div>
                <div style="font-size: 0.85em; color: #666; margin-top: 8px;">
                    Get your API key from <a href="https://openrouter.ai/keys" target="_blank" style="color: #3498db;">OpenRouter</a>. Your browser may offer to save this for you.
                </div>
                <div id="apiKeyError" style="color: #e74c3c; font-size: 0.9em; margin-top: 8px; display: none;"></div>
            </div>
            
            <div id="queryInterface" style="display: none;">
                <div style="margin-bottom: 15px;">
                    <textarea 
                        id="aiQueryInput" 
                        placeholder="Ask a question about this document..."
                        style="width: 100%; min-height: 80px; padding: 10px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px; resize: vertical; font-family: inherit;"
                    ></textarea>
                </div>
                <div style="display: flex; gap: 10px; align-items: center;">
                    <button 
                        id="submitQueryBtn"
                        onclick="submitAiQuery()"
                        style="padding: 10px 20px; background: #27ae60; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 14px;"
                    >
                        🚀 Submit Query
                    </button>
                    <div id="querySpinner" style="display: none;">
                        <span style="font-size: 24px; animation: spin 1s linear infinite; display: inline-block;">⏳</span>
                        <span style="margin-left: 10px; color: #666;">Processing...</span>
                    </div>
                    <div id="queryStatus" style="color: #666; font-size: 0.9em;"></div>
                </div>
            </div>
            
            <!-- Current Query Result -->
            <div id="queryResult" style="margin-top: 20px; display: none;">
                <h4 style="margin: 0 0 10px 0; color: #2c3e50; font-size: 1em;">Latest Query Result</h4>
                <div id="queryResultContent"></div>
            </div>
        </div>
        
        <div class="document-pages">
            ${pagesHtml}
        </div>
    `;
    
    modal.style.display = 'flex';
    
    // Prevent body scroll when modal is open
    document.body.style.overflow = 'hidden';
    
    // Check if API key is already set
    if (apiKey) {
        showQueryInterface();
    }
}

function createDocumentModal() {
    const modal = document.createElement('div');
    modal.id = 'documentModal';
    modal.className = 'modal';
    modal.innerHTML = '<div class="modal-document-content"></div>';
    document.body.appendChild(modal);
    
    // Close modal when clicking outside
    modal.addEventListener('click', (e) => {
        if (e.target === modal) {
            closeDocumentModal();
        }
    });
    
    return modal;
}

function closeDocumentModal() {
    const modal = document.getElementById('documentModal');
    if (modal) {
        modal.style.display = 'none';
        
        // Re-enable body scroll when modal is closed
        document.body.style.overflow = '';
    }
}

// Make viewDocument available globally
window.viewDocument = viewDocument;
window.closeDocumentModal = closeDocumentModal;

function openAgencyCard(agencyId) {
    // If this card is already open, do nothing
    if (currentOpenAgencyId === agencyId) {
        return;
    }
    
    // Close currently open card if different from the one being opened
    if (currentOpenAgencyId && currentOpenAgencyId !== agencyId) {
        const currentDetails = document.getElementById(`details-${currentOpenAgencyId}`);
        if (currentDetails) {
            currentDetails.classList.remove('visible');
        }
    }
    
    // Open the selected card
    const details = document.getElementById(`details-${agencyId}`);
    if (details) {
        details.classList.add('visible');
        currentOpenAgencyId = agencyId;
        
        // Scroll to the card - position top of card at top of viewport
        const card = document.getElementById(`agency-${agencyId}`);
        if (card) {
            card.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
    }
}

function handleUrlQueryString() {
    const urlParams = new URLSearchParams(window.location.search);
    const agencyId = urlParams.get('agency');
    const keywordsParam = urlParams.get('keywords');
    const legacyKeyword = urlParams.get('keyword'); // Support old single keyword param
    
    // Handle facility filter params
    const licenseStatusParam = urlParams.get('licensestatus');
    const agencyTypeParam = urlParams.get('agencytype');
    const countyParam = urlParams.get('county');
    
    if (agencyId) {
        // Find the agency
        const agency = allAgencies.find(a => a.agencyId === agencyId);
        if (agency) {
            // Set the agency filter using the same function, but skip URL update to avoid circular loop
            const searchText = `${agency.AgencyName} (${agency.agencyId})`;
            setAgencyFilter(searchText, agencyId, true);
        }
    }
    
    // Handle facility filters from URL
    if (licenseStatusParam) {
        filters.licenseStatus = licenseStatusParam;
        // Disable active license filter when a specific license status is selected
        // This is because the user explicitly chose a status
        filters.activeLicenseOnly = false;
        const activeLicenseCheckbox = document.getElementById('filterActiveLicenseOnly');
        if (activeLicenseCheckbox) {
            activeLicenseCheckbox.checked = false;
        }
        const licenseStatusSelect = document.getElementById('filterLicenseStatus');
        if (licenseStatusSelect) {
            licenseStatusSelect.value = licenseStatusParam;
        }
    }
    
    if (agencyTypeParam) {
        filters.agencyType = agencyTypeParam;
        const agencyTypeSelect = document.getElementById('filterAgencyType');
        if (agencyTypeSelect) {
            agencyTypeSelect.value = agencyTypeParam;
        }
    }
    
    if (countyParam) {
        filters.county = countyParam;
        const countySelect = document.getElementById('filterCounty');
        if (countySelect) {
            countySelect.value = countyParam;
        }
    }
    
    // Handle multiple keywords (new format)
    // URLSearchParams automatically decodes URL-encoded values
    if (keywordsParam) {
        const keywords = keywordsParam.split(',').map(k => k.trim()).filter(k => k.length > 0);
        filters.keywords = keywords;
        renderSelectedKeywords();
    }
    // Handle legacy single keyword (old format)
    else if (legacyKeyword) {
        filters.keywords = [legacyKeyword];
        renderSelectedKeywords();
    }
}

async function handleQueryStringDocument() {
    // Parse query string for sha parameter
    const urlParams = new URLSearchParams(window.location.search);
    const sha = urlParams.get('sha');
    
    if (!sha) {
        return;
    }
    
    // Redirect to document viewer page
    window.location.href = `${BASE_URL}document.html?sha=${sha}`;
}

function copyAgencyLink(agencyId, event) {
    if (event) {
        event.stopPropagation();
    }
    
    const url = `${window.location.origin}${window.location.pathname}?agency=${agencyId}`;
    
    // Copy to clipboard
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(url).then(() => {
            // Show feedback
            const btn = event.target;
            const originalText = btn.textContent;
            btn.textContent = '✓';
            setTimeout(() => {
                btn.textContent = originalText;
            }, 1000);
        }).catch(err => {
            console.error('Failed to copy link:', err);
            alert('Failed to copy link to clipboard');
        });
    } else {
        // Fallback for browsers without Clipboard API
        // Create a temporary textarea element
        const textarea = document.createElement('textarea');
        textarea.value = url;
        textarea.style.position = 'fixed';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.select();
        try {
            document.execCommand('copy');
            // Show feedback
            const btn = event.target;
            const originalText = btn.textContent;
            btn.textContent = '✓';
            setTimeout(() => {
                btn.textContent = originalText;
            }, 1000);
        } catch (err) {
            console.error('Failed to copy link:', err);
            alert('Failed to copy link to clipboard');
        } finally {
            document.body.removeChild(textarea);
        }
    }
}

function copyDocumentLink(sha256, event) {
    if (event) {
        event.stopPropagation();
    }
    
    const url = `${window.location.origin}${BASE_URL}document.html?sha=${sha256}`;
    
    // Helper function to show feedback on button
    const showCopyFeedback = (btn) => {
        const originalText = btn.textContent;
        btn.textContent = '✓';
        setTimeout(() => {
            btn.textContent = originalText;
        }, 1500);
    };
    
    // Copy to clipboard
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(url).then(() => {
            showCopyFeedback(event.target);
        }).catch(err => {
            console.error('Failed to copy link:', err);
            alert('Failed to copy link to clipboard');
        });
    } else {
        // Fallback for browsers without Clipboard API
        const textarea = document.createElement('textarea');
        textarea.value = url;
        textarea.style.position = 'fixed';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.select();
        try {
            document.execCommand('copy');
            showCopyFeedback(event.target);
        } catch (err) {
            console.error('Failed to copy link:', err);
            alert('Failed to copy link to clipboard');
        } finally {
            document.body.removeChild(textarea);
        }
    }
}

// Make functions available globally
window.copyAgencyLink = copyAgencyLink;
window.copyDocumentLink = copyDocumentLink;

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Set the API key from user input
 */
function setApiKey() {
    const apiKeyInput = document.getElementById('apiKeyInput');
    const apiKeyError = document.getElementById('apiKeyError');
    const setKeyBtn = document.getElementById('setApiKeyBtn');
    
    const key = apiKeyInput.value.trim();
    
    if (!key) {
        apiKeyError.textContent = 'Please enter your OpenRouter API key';
        apiKeyError.style.display = 'block';
        return;
    }
    
    // Basic validation - OpenRouter keys typically start with "sk-"
    if (!key.startsWith('sk-')) {
        apiKeyError.textContent = 'API key should start with "sk-". Please check your key.';
        apiKeyError.style.display = 'block';
        return;
    }
    
    apiKeyError.style.display = 'none';
    
    // Store the API key in memory (not persisted)
    apiKey = key;
    
    // Success - show query interface
    showQueryInterface();
    
    // Clear the input for security
    apiKeyInput.value = '';
}

/**
 * Show the query interface after API key is set
 */
function showQueryInterface() {
    const apiKeyPrompt = document.getElementById('apiKeyPrompt');
    const queryInterface = document.getElementById('queryInterface');
    
    if (apiKeyPrompt) {
        apiKeyPrompt.style.display = 'none';
    }
    if (queryInterface) {
        queryInterface.style.display = 'block';
    }
}

/**
 * Submit an AI query about the current document
 */
async function submitAiQuery() {
    const queryInput = document.getElementById('aiQueryInput');
    const submitBtn = document.getElementById('submitQueryBtn');
    const spinner = document.getElementById('querySpinner');
    const statusDiv = document.getElementById('queryStatus');
    
    if (!currentDocumentData) {
        statusDiv.innerHTML = '<div style="color: #e74c3c; padding: 10px; background: #fee; border-radius: 4px; margin-top: 10px;">Document not available</div>';
        return;
    }
    
    if (!apiKey) {
        statusDiv.innerHTML = '<div style="color: #e74c3c; padding: 10px; background: #fee; border-radius: 4px; margin-top: 10px;">Please set your API key first</div>';
        return;
    }
    
    const query = queryInput.value.trim();
    
    if (!query) {
        statusDiv.innerHTML = '<div style="color: #e74c3c; padding: 10px; background: #fee; border-radius: 4px; margin-top: 10px;">Please enter a query</div>';
        setTimeout(() => statusDiv.innerHTML = '', 3000);
        return;
    }
    
    // Disable input and button
    queryInput.disabled = true;
    submitBtn.disabled = true;
    spinner.style.display = 'flex';
    spinner.style.alignItems = 'center';
    statusDiv.textContent = '';
    
    try {
        // Concatenate all pages
        const documentText = currentDocumentData.pages.join('\n\n');
        
        // Submit query to API
        const result = await queryDeepSeek(apiKey, query, documentText);
        
        // Display the result (not stored)
        displayQueryResult({
            query,
            response: result.response,
            inputTokens: result.inputTokens,
            outputTokens: result.outputTokens,
            durationMs: result.durationMs,
            cost: result.cost,
            cacheDiscount: result.cacheDiscount,
            timestamp: Date.now()
        });
        
        // Clear input
        queryInput.value = '';
        
        statusDiv.textContent = 'Query completed successfully!';
        statusDiv.style.color = '#27ae60';
        setTimeout(() => {
            statusDiv.textContent = '';
        }, 3000);
        
    } catch (error) {
        console.error('Error submitting query:', error);
        statusDiv.innerHTML = `<div style="color: #e74c3c; padding: 10px; background: #fee; border-radius: 4px; margin-top: 10px;">Error: ${escapeHtml(error.message)}</div>`;
    } finally {
        // Re-enable input and button
        queryInput.disabled = false;
        submitBtn.disabled = false;
        spinner.style.display = 'none';
    }
}

/**
 * Display a query result
 */
function displayQueryResult(queryData) {
    // Calculate estimated cost: $0.25 per million input tokens, $0.38 per million output tokens
    const inputCost = (queryData.inputTokens / 1000000) * 0.25;
    const outputCost = (queryData.outputTokens / 1000000) * 0.38;
    const estimatedCost = inputCost + outputCost;
    
    // Build cache discount info if available
    let cacheInfo = '';
    if (queryData.cacheDiscount !== null && queryData.cacheDiscount !== undefined) {
        cacheInfo = `<span style="color: #27ae60; font-weight: 600;">💾 Cache Discount: $${queryData.cacheDiscount.toFixed(6)}</span>`;
    }
    
    // Create a result display element
    const resultHtml = `
        <div style="background: white; padding: 20px; border: 2px solid #3498db; border-radius: 8px;">
            <div style="margin-bottom: 15px;">
                <strong style="color: #2c3e50;">Query:</strong>
                <div style="background: #f8f9fa; padding: 10px; margin-top: 5px; border-radius: 4px; white-space: pre-wrap;">${escapeHtml(queryData.query)}</div>
            </div>
            <div style="margin-bottom: 15px;">
                <strong style="color: #2c3e50;">Response:</strong>
                <div style="background: #e8f4f8; padding: 15px; margin-top: 5px; border-radius: 4px; white-space: pre-wrap; line-height: 1.6;">${escapeHtml(queryData.response)}</div>
            </div>
            <div style="display: flex; gap: 20px; flex-wrap: wrap; font-size: 0.85em; color: #666;">
                <span>📊 Input tokens: ${queryData.inputTokens}</span>
                <span>📊 Output tokens: ${queryData.outputTokens}</span>
                <span>⏱️ Duration: ${(queryData.durationMs / 1000).toFixed(2)}s</span>
                <span>💰 Estimated Cost: ~$${estimatedCost.toFixed(6)}</span>
                ${cacheInfo}
                <span>🕐 ${new Date(queryData.timestamp).toLocaleString()}</span>
            </div>
        </div>
    `;
    
    // Display in the query result section
    const resultContent = document.getElementById('queryResultContent');
    const resultSection = document.getElementById('queryResult');
    
    if (resultContent && resultSection) {
        resultContent.innerHTML = resultHtml;
        resultSection.style.display = 'block';
    }
}

// Make AI query functions available globally
window.setApiKey = setApiKey;
window.submitAiQuery = submitAiQuery;

/**
 * Set the commit hash at the bottom of the page
 */
function setCommitHash() {
    const commitHashEl = document.getElementById('commitHash');
    if (commitHashEl) {
        // The commit hash will be injected during build
        // For now, we'll use a placeholder that can be replaced during deployment
        const commitHash = '__COMMIT_HASH__';
        commitHashEl.textContent = `Version: ${commitHash}`;
    }
}

// Initialize the application
init();

