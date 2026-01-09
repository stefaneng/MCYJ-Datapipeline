import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Header, FilterPanel, AgencyList, Loading, Error } from '../components/index.js';
import { Trie } from '../trie.js';
import { getBaseUrl, ACTIVE_LICENSE_STATUSES, copyToClipboard } from '../utils/helpers.js';

const BASE_URL = getBaseUrl();
const DOM_READY_DELAY = 100;

/**
 * Main App component for the dashboard
 */
export function App() {
    // State
    const [allAgencies, setAllAgencies] = useState([]);
    const [filteredAgencies, setFilteredAgencies] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [openAgencyId, setOpenAgencyId] = useState(null);
    const [selectedAgencyText, setSelectedAgencyText] = useState('');
    
    // Filter state
    const [filters, setFilters] = useState({
        sirOnly: true,
        keywords: [],
        agency: null,
        activeLicenseOnly: true,
        licenseStatus: null,
        agencyType: null,
        county: null
    });
    
    // Unique values for dropdowns
    const [uniqueLicenseStatuses, setUniqueLicenseStatuses] = useState([]);
    const [uniqueAgencyTypes, setUniqueAgencyTypes] = useState([]);
    const [uniqueCounties, setUniqueCounties] = useState([]);
    
    // Tries for autocomplete
    const keywordTrieRef = useRef(new Trie());
    const agencyTrieRef = useRef(new Trie());
    const agencyIdMapRef = useRef(new Map());

    // Load data on mount
    useEffect(() => {
        loadData();
    }, []);

    // Apply filters when filters or allAgencies change
    useEffect(() => {
        if (allAgencies.length > 0) {
            applyFilters();
        }
    }, [filters, allAgencies]);

    // Handle URL query string on mount
    useEffect(() => {
        if (allAgencies.length > 0) {
            handleUrlQueryString();
        }
    }, [allAgencies]);

    const loadData = async () => {
        try {
            const response = await fetch(`${BASE_URL}data/agencies_data.json`);
            if (!response.ok) {
                throw new Error(`Failed to load data: ${response.statusText}`);
            }
            
            const data = await response.json();
            setAllAgencies(data);
            
            // Build tries and filter options
            buildKeywordTrie(data);
            buildAgencyTrie(data);
            buildFacilityFilterOptions(data);
            
            setLoading(false);
        } catch (err) {
            console.error('Error loading data:', err);
            setError(`Failed to load data: ${err.message}`);
            setLoading(false);
        }
    };

    const buildKeywordTrie = (agencies) => {
        const trie = new Trie();
        agencies.forEach(agency => {
            if (agency.documents && Array.isArray(agency.documents)) {
                agency.documents.forEach(doc => {
                    if (doc.sir_violation_level?.keywords) {
                        doc.sir_violation_level.keywords.forEach(keyword => {
                            trie.insert(keyword, true, keyword);
                            const words = keyword.trim().split(/\s+/);
                            words.forEach(word => {
                                if (word.length > 0) {
                                    trie.insert(word, false, keyword);
                                }
                            });
                        });
                    }
                });
            }
        });
        keywordTrieRef.current = trie;
    };

    const buildAgencyTrie = (agencies) => {
        const trie = new Trie();
        const idMap = new Map();
        
        agencies.forEach(agency => {
            if (agency.AgencyName && agency.agencyId) {
                const searchText = `${agency.AgencyName} (${agency.agencyId})`;
                trie.insert(searchText, true, searchText);
                idMap.set(searchText.toLowerCase(), agency.agencyId);
                
                const words = agency.AgencyName.trim().split(/\s+/);
                words.forEach(word => {
                    if (word.length > 0) {
                        trie.insert(word, false, searchText);
                    }
                });
                trie.insert(agency.agencyId, false, searchText);
            }
        });
        
        agencyTrieRef.current = trie;
        agencyIdMapRef.current = idMap;
    };

    const buildFacilityFilterOptions = (agencies) => {
        const licenseStatuses = new Set();
        const agencyTypes = new Set();
        const counties = new Set();
        
        agencies.forEach(agency => {
            if (agency.facility) {
                if (agency.facility.LicenseStatus) licenseStatuses.add(agency.facility.LicenseStatus);
                if (agency.facility.AgencyType) agencyTypes.add(agency.facility.AgencyType);
                if (agency.facility.County) counties.add(agency.facility.County);
            }
        });
        
        setUniqueLicenseStatuses(Array.from(licenseStatuses).sort());
        setUniqueAgencyTypes(Array.from(agencyTypes).sort());
        setUniqueCounties(Array.from(counties).sort());
    };

    const handleUrlQueryString = () => {
        const urlParams = new URLSearchParams(window.location.search);
        const agencyId = urlParams.get('agency');
        const keywordsParam = urlParams.get('keywords');
        const legacyKeyword = urlParams.get('keyword');
        const licenseStatusParam = urlParams.get('licensestatus');
        const agencyTypeParam = urlParams.get('agencytype');
        const countyParam = urlParams.get('county');
        
        const newFilters = { ...filters };
        
        if (agencyId) {
            const agency = allAgencies.find(a => a.agencyId === agencyId);
            if (agency) {
                const searchText = `${agency.AgencyName} (${agency.agencyId})`;
                newFilters.agency = agencyId;
                setSelectedAgencyText(searchText);
            }
        }
        
        if (licenseStatusParam) {
            newFilters.licenseStatus = licenseStatusParam;
            newFilters.activeLicenseOnly = false;
        }
        
        if (agencyTypeParam) {
            newFilters.agencyType = agencyTypeParam;
        }
        
        if (countyParam) {
            newFilters.county = countyParam;
        }
        
        if (keywordsParam) {
            const keywords = keywordsParam.split(',').map(k => k.trim()).filter(k => k.length > 0);
            newFilters.keywords = keywords;
        } else if (legacyKeyword) {
            newFilters.keywords = [legacyKeyword];
        }
        
        setFilters(newFilters);
    };

    const applyFilters = useCallback(() => {
        let agencies = JSON.parse(JSON.stringify(allAgencies));
        let selectedAgencyIdForAutoOpen = null;
        
        // Filter by selected agency
        if (filters.agency) {
            agencies = agencies.filter(agency => agency.agencyId === filters.agency);
            if (agencies.length === 1) {
                selectedAgencyIdForAutoOpen = agencies[0].agencyId;
            }
        }
        
        // Apply facility-level filters
        agencies = agencies.filter(agency => {
            const facility = agency.facility;
            
            if (filters.activeLicenseOnly) {
                if (!facility || !ACTIVE_LICENSE_STATUSES.includes(facility.LicenseStatus)) {
                    return false;
                }
            }
            
            if (filters.licenseStatus) {
                if (!facility || facility.LicenseStatus !== filters.licenseStatus) {
                    return false;
                }
            }
            
            if (filters.agencyType) {
                if (!facility || facility.AgencyType !== filters.agencyType) {
                    return false;
                }
            }
            
            if (filters.county) {
                if (!facility || facility.County !== filters.county) {
                    return false;
                }
            }
            
            return true;
        });
        
        // Apply document-level filters
        agencies = agencies.map(agency => {
            if (!agency.documents || !Array.isArray(agency.documents)) {
                return agency;
            }
            
            const originalReportCount = agency.documents.length;
            
            let filteredDocuments = agency.documents.filter(d => {
                if (filters.sirOnly && !d.is_special_investigation) {
                    return false;
                }
                
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
            
            return {
                ...agency,
                documents: filteredDocuments,
                total_reports: filteredDocuments.length,
                original_total_reports: originalReportCount,
                filtered_out_count: originalReportCount - filteredDocuments.length
            };
        });
        
        // Remove agencies with no reports (unless specific agency is selected)
        if (!filters.agency) {
            agencies = agencies.filter(agency => agency.total_reports > 0);
        }
        
        setFilteredAgencies(agencies);
        
        // Auto-open agency card if specific agency is selected
        if (selectedAgencyIdForAutoOpen) {
            setTimeout(() => {
                setOpenAgencyId(selectedAgencyIdForAutoOpen);
                const card = document.getElementById(`agency-${selectedAgencyIdForAutoOpen}`);
                if (card) {
                    card.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
            }, DOM_READY_DELAY);
        }
    }, [allAgencies, filters]);

    // Filter change handlers
    const handleFilterChange = (key, value) => {
        setFilters(prev => ({ ...prev, [key]: value }));
        updateUrlWithFilters({ ...filters, [key]: value });
    };

    const handleKeywordSearch = (query) => {
        return keywordTrieRef.current.search(query);
    };

    const handleKeywordSelect = (suggestion) => {
        const keyword = suggestion.keyword;
        if (!filters.keywords.some(k => k.toLowerCase() === keyword.toLowerCase())) {
            const newKeywords = [...filters.keywords, keyword];
            setFilters(prev => ({ ...prev, keywords: newKeywords }));
            updateUrlWithFilters({ ...filters, keywords: newKeywords });
        }
    };

    const handleKeywordRemove = (keyword) => {
        const newKeywords = filters.keywords.filter(k => k.toLowerCase() !== keyword.toLowerCase());
        setFilters(prev => ({ ...prev, keywords: newKeywords }));
        updateUrlWithFilters({ ...filters, keywords: newKeywords });
    };

    const handleClearAllKeywords = () => {
        setFilters(prev => ({ ...prev, keywords: [] }));
        updateUrlWithFilters({ ...filters, keywords: [] });
    };

    const handleAgencySearch = (query) => {
        return agencyTrieRef.current.search(query);
    };

    const handleAgencySelect = (suggestion) => {
        const agencyId = agencyIdMapRef.current.get(suggestion.keyword.toLowerCase()) || '';
        setFilters(prev => ({ ...prev, agency: agencyId }));
        setSelectedAgencyText(suggestion.keyword);
        updateUrlWithFilters({ ...filters, agency: agencyId });
    };

    const handleAgencyRemove = () => {
        setFilters(prev => ({ ...prev, agency: null }));
        setSelectedAgencyText('');
        setOpenAgencyId(null);
        updateUrlWithFilters({ ...filters, agency: null });
    };

    const handleToggleAgency = (agencyId) => {
        // Only open a new agency card, don't close an already open one by clicking it
        // Cards are closed only by opening a different card or changing filters
        if (openAgencyId !== agencyId) {
            setOpenAgencyId(agencyId);
        }
    };

    const handleCopyDocumentLink = (sha256, event) => {
        if (event) event.stopPropagation();
        const url = `${window.location.origin}${BASE_URL}document.html?sha=${sha256}`;
        
        copyToClipboard(
            url,
            () => {
                const btn = event?.target;
                if (btn) {
                    const originalText = btn.textContent;
                    btn.textContent = '✓';
                    setTimeout(() => { btn.textContent = originalText; }, 1500);
                }
            },
            (err) => {
                console.error('Failed to copy link:', err);
                alert('Failed to copy link to clipboard');
            }
        );
    };

    const updateUrlWithFilters = (newFilters) => {
        const url = new URL(window.location);
        
        // Update keywords
        url.searchParams.delete('keyword');
        url.searchParams.delete('keywords');
        if (newFilters.keywords.length > 0) {
            url.searchParams.set('keywords', newFilters.keywords.join(','));
        }
        
        // Update agency
        if (newFilters.agency) {
            url.searchParams.set('agency', newFilters.agency);
        } else {
            url.searchParams.delete('agency');
        }
        
        // Update facility filters
        if (newFilters.licenseStatus) {
            url.searchParams.set('licensestatus', newFilters.licenseStatus);
        } else {
            url.searchParams.delete('licensestatus');
        }
        
        if (newFilters.agencyType) {
            url.searchParams.set('agencytype', newFilters.agencyType);
        } else {
            url.searchParams.delete('agencytype');
        }
        
        if (newFilters.county) {
            url.searchParams.set('county', newFilters.county);
        } else {
            url.searchParams.delete('county');
        }
        
        window.history.pushState({}, '', url);
    };

    // Calculate stats
    const totalAgencies = filteredAgencies.length;
    const totalReports = filteredAgencies.reduce((sum, a) => sum + a.total_reports, 0);

    if (loading) {
        return (
            <>
                <Header 
                    title="Michigan Child Welfare Licensing Dashboard" 
                    subtitle="Agency Documents and Reports" 
                />
                <div className="container">
                    <Loading message="Loading data..." />
                </div>
            </>
        );
    }

    if (error) {
        return (
            <>
                <Header 
                    title="Michigan Child Welfare Licensing Dashboard" 
                    subtitle="Agency Documents and Reports" 
                />
                <div className="container">
                    <Error message={error} />
                </div>
            </>
        );
    }

    return (
        <>
            <Header 
                title="Michigan Child Welfare Licensing Dashboard" 
                subtitle="Agency Documents and Reports" 
            />
            <div className="container">
                <FilterPanel
                    filters={filters}
                    onFilterChange={handleFilterChange}
                    onKeywordSearch={handleKeywordSearch}
                    onKeywordSelect={handleKeywordSelect}
                    onKeywordRemove={handleKeywordRemove}
                    onClearAllKeywords={handleClearAllKeywords}
                    onAgencySearch={handleAgencySearch}
                    onAgencySelect={handleAgencySelect}
                    onAgencyRemove={handleAgencyRemove}
                    uniqueLicenseStatuses={uniqueLicenseStatuses}
                    uniqueAgencyTypes={uniqueAgencyTypes}
                    uniqueCounties={uniqueCounties}
                    selectedAgencyText={selectedAgencyText}
                    totalAgencies={totalAgencies}
                    totalReports={totalReports}
                />
                
                <AgencyList
                    agencies={filteredAgencies}
                    openAgencyId={openAgencyId}
                    onToggleAgency={handleToggleAgency}
                    onCopyDocumentLink={handleCopyDocumentLink}
                />
            </div>
            
            <div id="commitHash" style={{ textAlign: 'center', padding: '20px', color: '#999', fontSize: '0.8em', fontFamily: 'monospace' }}>
                Version: {__COMMIT_HASH__}
            </div>
        </>
    );
}

export default App;
