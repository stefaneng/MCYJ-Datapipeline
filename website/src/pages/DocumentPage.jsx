import React, { useState, useEffect } from 'react';
import { Header, Loading, Error } from '../components/index.js';
import { KeywordBadgeList } from '../components/KeywordBadge.jsx';
import { getBaseUrl, copyToClipboard, escapeHtml } from '../utils/helpers.js';

const BASE_URL = getBaseUrl();

/**
 * DocumentPage component for viewing individual documents
 */
export function DocumentPage() {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [documentData, setDocumentData] = useState(null);
    const [docMetadata, setDocMetadata] = useState(null);
    const [showSearch, setShowSearch] = useState(false);
    const [searchSha, setSearchSha] = useState('');
    const [copyFeedback, setCopyFeedback] = useState({ link: false, text: false });

    useEffect(() => {
        const urlParams = new URLSearchParams(window.location.search);
        const sha = urlParams.get('sha');
        
        if (sha) {
            loadDocument(sha);
        } else {
            setShowSearch(true);
        }
    }, []);

    const loadDocument = async (sha256) => {
        setShowSearch(false);
        setLoading(true);
        setError(null);

        try {
            const response = await fetch(`${BASE_URL}documents/${sha256}.json`);
            if (!response.ok) {
                throw new Error(`Failed to load document: ${response.statusText}`);
            }

            const docData = await response.json();
            setDocumentData(docData);

            // Try to fetch metadata from agencies data
            try {
                const agenciesResponse = await fetch(`${BASE_URL}data/agencies_data.json`);
                if (agenciesResponse.ok) {
                    const agencies = await agenciesResponse.json();
                    
                    for (const agency of agencies) {
                        if (agency.documents && Array.isArray(agency.documents)) {
                            const doc = agency.documents.find(d => d.sha256 === sha256);
                            if (doc) {
                                setDocMetadata({
                                    title: doc.document_title || doc.agency_name || 'Untitled Document',
                                    is_special_investigation: doc.is_special_investigation || false,
                                    agencyName: agency.AgencyName,
                                    agencyId: agency.agencyId
                                });
                                break;
                            }
                        }
                    }
                }
            } catch (err) {
                console.warn('Could not load agency metadata:', err);
            }

            setLoading(false);
        } catch (err) {
            console.error('Error loading document:', err);
            setError(`Failed to load document: ${err.message}`);
            setLoading(false);
            setShowSearch(true);
        }
    };

    const handleCopyLink = () => {
        copyToClipboard(
            window.location.href,
            () => {
                setCopyFeedback(prev => ({ ...prev, link: true }));
                setTimeout(() => setCopyFeedback(prev => ({ ...prev, link: false })), 1500);
            },
            (err) => console.error('Failed to copy link:', err)
        );
    };

    const handleCopyText = () => {
        if (!documentData?.pages) return;
        
        const fullText = documentData.pages.join('\n\n');
        copyToClipboard(
            fullText,
            () => {
                setCopyFeedback(prev => ({ ...prev, text: true }));
                setTimeout(() => setCopyFeedback(prev => ({ ...prev, text: false })), 2000);
            },
            (err) => console.error('Failed to copy text:', err)
        );
    };

    const handleSearchSubmit = () => {
        const sha = searchSha.trim();
        
        if (!sha) {
            alert('Please enter a document SHA-256 hash');
            return;
        }
        
        if (!/^[a-fA-F0-9]{64}$/.test(sha)) {
            alert('Invalid SHA-256 format. Please enter a 64-character hexadecimal hash.');
            return;
        }
        
        const url = new URL(window.location);
        url.searchParams.set('sha', sha);
        window.history.pushState({}, '', url);
        
        loadDocument(sha);
    };

    const renderSirSummary = () => {
        if (!documentData?.sir_summary?.summary) return null;

        const level = documentData.sir_violation_level?.level?.toLowerCase();
        let levelColor = '#95a5a6';
        let levelEmoji = '⚪';
        
        if (level === 'low') { levelColor = '#f39c12'; levelEmoji = '🟡'; }
        else if (level === 'moderate') { levelColor = '#e67e22'; levelEmoji = '🟠'; }
        else if (level === 'severe') { levelColor = '#e74c3c'; levelEmoji = '🔴'; }

        return (
            <div className="sir-summary">
                <h3>
                    📋 Special Investigation Report Summary (AI-generated)
                    {documentData.sir_summary.violation === 'y' && (
                        <span className="violation-badge violation-yes">⚠️ Violation Substantiated</span>
                    )}
                    {documentData.sir_summary.violation === 'n' && (
                        <span className="violation-badge violation-no">✓ No Violation</span>
                    )}
                    {level && (
                        <span style={{ color: levelColor, marginLeft: '8px', fontSize: '0.9em' }}>
                            {levelEmoji} {level.charAt(0).toUpperCase() + level.slice(1)} Severity
                        </span>
                    )}
                </h3>
                <div style={{ marginBottom: documentData.sir_violation_level?.justification || documentData.sir_violation_level?.keywords?.length > 0 ? '15px' : '0' }}>
                    <strong style={{ color: '#2c3e50' }}>Summary:</strong>
                    <div style={{ marginTop: '8px' }}>{documentData.sir_summary.summary}</div>
                </div>
                {documentData.sir_violation_level?.keywords?.length > 0 && (
                    <div style={{ paddingTop: '15px', borderTop: '1px solid #ecf0f1', marginBottom: documentData.sir_violation_level?.justification ? '15px' : '0' }}>
                        <strong style={{ color: '#2c3e50' }}>Keywords:</strong>
                        <div style={{ marginTop: '8px' }}>
                            <KeywordBadgeList 
                                keywords={documentData.sir_violation_level.keywords}
                                maxDisplay={null}
                                small
                            />
                        </div>
                    </div>
                )}
                {documentData.sir_violation_level?.justification && (
                    <div style={{ paddingTop: '15px', borderTop: '1px solid #ecf0f1' }}>
                        <strong style={{ color: '#2c3e50' }}>Severity Justification:</strong>
                        <div style={{ marginTop: '8px' }}>{documentData.sir_violation_level.justification}</div>
                    </div>
                )}
            </div>
        );
    };

    if (loading) {
        return (
            <>
                <Header 
                    title="Document Viewer" 
                    subtitle="Michigan Child Welfare Licensing Dashboard" 
                />
                <div className="container">
                    <a href={`${BASE_URL}`} className="back-link">← Back to Dashboard</a>
                    <Loading message="Loading document..." />
                </div>
            </>
        );
    }

    return (
        <>
            <Header 
                title="Document Viewer" 
                subtitle="Michigan Child Welfare Licensing Dashboard" 
            />
            <div className="container">
                <a href={`${BASE_URL}`} className="back-link">← Back to Dashboard</a>
                
                {error && <Error message={error} />}
                
                {showSearch && (
                    <div className="search-box">
                        <h2>Search for a Document</h2>
                        <p>
                            Enter a document SHA-256 hash to view the document. The SHA-256 hash is a 64-character hexadecimal identifier
                            that uniquely identifies each document (e.g., <code>a1b2c3d4...</code>).
                        </p>
                        <input
                            type="text"
                            value={searchSha}
                            onChange={(e) => setSearchSha(e.target.value)}
                            placeholder="Enter document SHA-256 hash (64 hex characters)"
                            maxLength={64}
                        />
                        <div className="example">
                            Example: <code>1a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p7q8r9s0t1u2v3w4x5y6z7a8b9c0d1e2f3g</code>
                        </div>
                        <button onClick={handleSearchSubmit}>View Document</button>
                    </div>
                )}
                
                {documentData && (
                    <div className="document-container">
                        <div className="document-header">
                            <h2>Document Details</h2>
                        </div>
                        
                        <div className="document-info">
                            {docMetadata && (
                                <>
                                    <div><strong>Title:</strong> {docMetadata.title}</div>
                                    {docMetadata.agencyName && (
                                        <div><strong>Agency:</strong> {docMetadata.agencyName} (ID: {docMetadata.agencyId})</div>
                                    )}
                                    {docMetadata.is_special_investigation && (
                                        <div style={{ color: '#e74c3c' }}><strong>Type:</strong> 🔍 Special Investigation Report</div>
                                    )}
                                </>
                            )}
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap', marginTop: '10px' }}>
                                <button className="copy-link-btn" onClick={handleCopyLink} style={{ padding: '8px 16px', background: '#3498db', color: 'white', opacity: 1, fontSize: '0.9em' }}>
                                    {copyFeedback.link ? '✓ Copied!' : '🔗 Copy URL to this document'}
                                </button>
                                <button className="copy-link-btn" onClick={handleCopyText} style={{ padding: '8px 16px', background: '#3498db', color: 'white', opacity: 1, fontSize: '0.9em' }}>
                                    {copyFeedback.text ? '✓ Copied!' : '📋 Copy Document Text'}
                                </button>
                                {docMetadata?.agencyId && (
                                    <a href={`${BASE_URL}?agency=${encodeURIComponent(docMetadata.agencyId)}`} className="copy-link-btn" style={{ padding: '8px 16px', background: '#3498db', color: 'white', opacity: 1, fontSize: '0.9em', textDecoration: 'none' }}>
                                        🏢 View Agency
                                    </a>
                                )}
                            </div>
                            <div><strong>Document ID (SHA-256):</strong> <code style={{ wordBreak: 'break-all' }}>{documentData.sha256}</code></div>
                            <div><strong>Date Processed:</strong> {documentData.dateprocessed}</div>
                            <div><strong>Total Pages:</strong> {documentData.pages?.length || 0}</div>
                        </div>
                        
                        {renderSirSummary()}
                        
                        <div className="document-pages">
                            {documentData.pages?.map((page, index) => (
                                <div key={index} className="document-page">
                                    <div className="page-number">Page {index + 1} of {documentData.pages.length}</div>
                                    <pre className="page-text">{page}</pre>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
            </div>
            
            <div id="commitHash" style={{ textAlign: 'center', padding: '20px', color: '#999', fontSize: '0.8em', fontFamily: 'monospace' }}>
                Version: {__COMMIT_HASH__}
            </div>
        </>
    );
}

export default DocumentPage;
