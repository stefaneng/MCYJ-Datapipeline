#!/usr/bin/env python3
"""
Extract basic document information from parquet files containing PDF text extracts.

This script reads concatenated parquet files and extracts:
- Agency ID (License #)
- Date (inspection/report date)
- Agency name
- Document title (extracted from document content)
- Special investigation indicator (whether document is a Special Investigation Report)

Output is saved as a CSV file with one row per document.
"""

import argparse
import ast
import csv
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# Set up logger
logger = logging.getLogger(__name__)


def extract_license_number(text: str) -> Optional[str]:
    """Extract license number from text."""
    # Look for patterns like "License #: CB250296641" or "License#: CA110200973"
    patterns = [
        r'License\s*#?\s*:\s*([A-Z0-9]+)',
        r'License\s*Number\s*:\s*([A-Z0-9]+)',
        r'Re:\s*License\s*#?\s*:\s*([A-Z0-9]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return None


def extract_agency_name(text: str) -> Optional[str]:
    """Extract agency name from text."""
    # Look for patterns like "Agency Name: SAMARITAS - BAY" or "Name of Agency:"
    patterns = [
        r'Agency Name:\s*([^\n]+)',
        r'Name of Agency:\s*([^\n]+)',
        r'Licensee Name:\s*([^\n]+)',
        r'Name of Facility:\s*([^\n]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            name = match.group(1).strip()
            # Clean up the name
            name = re.sub(r'\s+', ' ', name)
            return name
    
    return None


def extract_document_title(text: str) -> Optional[str]:
    """Extract document title from text.
    
    Common document types in Michigan child welfare licensing:
    - Special Investigation Reports (with Investigation # if present)
    - Licensing Studies
    - Renewal Reports
    - Complaint Investigation Reports
    - Inspection Reports
    - Interim Monitoring Reports
    """
    # Try to find document titles in the first 3000 characters (extended for SIR detection)
    header_text = text[:3000]
    
    # Check for "Attached is the Special Investigation Report" pattern first
    # This is common in cover letters for SIR documents
    if re.search(r'Attached is the Special Investigation Report', header_text, re.IGNORECASE):
        title = "Special Investigation Report"
        sir_number = extract_investigation_number(header_text)
        if sir_number:
            title = f"{title} #{sir_number}"
        return title
    
    # Look for common document type patterns in the header
    title_patterns = [
        # Special Investigation patterns - prioritize "SPECIAL INVESTIGATION REPORT" first
        r'(?:BUREAU OF CHILDREN AND ADULT LICENSING\s+)?SPECIAL INVESTIGATION REPORT',
        # Licensing Study patterns  
        r'(?:BUREAU OF CHILDREN AND ADULT LICENSING\s+)?LICENSING STUDY',
        r'LICENSING STUDY REPORT',
        # Renewal patterns
        r'(?:BUREAU OF CHILDREN AND ADULT LICENSING\s+)?RENEWAL INSPECTION REPORT',
        r'RENEWAL REPORT',
        r'RENEWAL INSPECTION',
        # Complaint patterns
        r'COMPLAINT INVESTIGATION REPORT',
        r'COMPLAINT INVESTIGATION',
        # Inspection patterns
        r'(?:BUREAU OF CHILDREN AND ADULT LICENSING\s+)?INSPECTION REPORT',
        r'ON-SITE INSPECTION REPORT',
        r'INTERIM MONITORING REPORT',
        r'MONITORING REPORT',
        # General inspection
        r'INSPECTION CHECKLIST',
        # Other report types
        r'CORRECTIVE ACTION PLAN',
        r'PROVISIONAL LICENSE REPORT',
    ]
    
    for pattern in title_patterns:
        match = re.search(pattern, header_text, re.IGNORECASE)
        if match:
            title = match.group(0).strip()
            # Normalize spacing
            title = ' '.join(title.split())
            # Apply smart title casing - only if text is all uppercase
            if title.isupper():
                title = title.title()
            
            # For Special Investigation Reports, try to append the Investigation number
            if 'SPECIAL INVESTIGATION' in title.upper():
                sir_number = extract_investigation_number(header_text)
                if sir_number:
                    title = f"{title} #{sir_number}"
            
            return title
    
    # If no specific title found, try to extract from first few lines
    lines = header_text.split('\n')
    for line in lines[:10]:
        line = line.strip()
        # Look for lines that end with "REPORT" or "STUDY"
        if line and re.search(r'(REPORT|STUDY|INSPECTION|INVESTIGATION)$', line, re.IGNORECASE):
            if len(line) < 100:  # Reasonable title length
                title = ' '.join(line.split())
                # Apply smart title casing - only if text is all uppercase
                if title.isupper():
                    title = title.title()
                return title
    
    return None


def extract_investigation_number(text: str) -> Optional[str]:
    """Extract Investigation/SIR number from Special Investigation Reports."""
    # Look for Investigation # pattern (e.g., "Investigation #: 2019C0114036")
    patterns = [
        r'Investigation\s*#\s*:\s*([A-Z0-9]+)',
        r'SIR\s*#\s*:\s*([A-Z0-9]+)',
        r'Report\s*#\s*:\s*([A-Z0-9]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return None


def extract_inspection_date(text: str) -> Optional[str]:
    """Extract inspection or report date from text."""
    # Look for various date patterns
    patterns = [
        r'Date\(s\) of On-site Inspection:\s*([^\n]+)',
        r'Date of On-site Inspection\(s\):\s*([^\n]+)',
        r'Special Investigation Intake Date:\s*([^\n]+)',
        r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}',
        r'\d{1,2}/\d{1,2}/\d{4}',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            # Use capturing group if available, otherwise use full match
            date_str = match.group(1) if match.lastindex and match.lastindex >= 1 else match.group(0)
            date_str = date_str.strip()
            # Clean up date string
            date_str = re.sub(r'\s+', ' ', date_str)
            return date_str
    
    return None


def is_special_investigation(text: str) -> bool:
    """Determine if the document is a special investigation report.

    Returns True if the document appears to be a Special Investigation Report (SIR).
    """
    # Check for "Attached is the Special Investigation Report" pattern
    # This is common in cover letters for SIR documents
    if re.search(r'Attached is the Special Investigation Report', text[:3000], re.IGNORECASE):
        return True

    # Check for "SPECIAL INVESTIGATION REPORT" pattern in header
    if re.search(r'(?:BUREAU OF CHILDREN AND ADULT LICENSING\s+)?SPECIAL INVESTIGATION REPORT',
                 text[:3000], re.IGNORECASE):
        return True

    # Check for Investigation # which is specific to SIRs
    if extract_investigation_number(text[:3000]) is not None:
        return True

    return False


def parse_document(text_pages: List[str]) -> Dict[str, Any]:
    """Parse a document (list of pages) and extract relevant information."""
    # Combine all pages into a single text for parsing
    full_text = '\n'.join(text_pages)

    # Extract information
    license_number = extract_license_number(full_text)
    agency_name = extract_agency_name(full_text)
    document_title = extract_document_title(full_text)
    inspection_date = extract_inspection_date(full_text)
    is_sir = is_special_investigation(full_text)

    return {
        'agency_id': license_number,
        'date': inspection_date,
        'agency_name': agency_name,
        'document_title': document_title,
        'is_special_investigation': is_sir,
    }


def process_parquet_files(parquet_dir: str, output_csv: str) -> None:
    """Process all parquet files in directory and output to CSV."""
    parquet_path = Path(parquet_dir)
    
    if not parquet_path.exists():
        logger.error(f"Directory '{parquet_dir}' does not exist")
        sys.exit(1)
    
    # Find all parquet files
    parquet_files = list(parquet_path.glob("*.parquet"))
    
    if not parquet_files:
        logger.error(f"No parquet files found in '{parquet_dir}'")
        sys.exit(1)
    
    logger.info(f"Found {len(parquet_files)} parquet files")
    
    # Collect all records
    all_records = []
    total_documents = 0
    
    for parquet_file in sorted(parquet_files):
        logger.info(f"Processing: {parquet_file.name}")
        
        try:
            df = pd.read_parquet(parquet_file)
            logger.info(f"  Found {len(df)} documents in file")
            
            for idx, row in df.iterrows():
                total_documents += 1
                
                # Parse text (stored as string representation of list)
                try:
                    text_pages = ast.literal_eval(row['text']) if isinstance(row['text'], str) else row['text']
                except (ValueError, SyntaxError):
                    logger.error(f"Failed to parse text for document {row['sha256']}")
                    continue
                
                # Parse document
                parsed = parse_document(text_pages)
                
                # Add metadata from parquet
                parsed['sha256'] = row['sha256']
                parsed['date_processed'] = row['dateprocessed']
                
                all_records.append(parsed)
                
        except Exception as e:
            logger.error(f"Error processing {parquet_file.name}: {e}")
            continue
    
    logger.info(f"\nProcessed {total_documents} documents total")
    
    # Write to CSV
    if all_records:
        output_path = Path(output_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'agency_id', 'date', 'agency_name', 'document_title', 
                'is_special_investigation', 'sha256', 'date_processed'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for record in all_records:
                writer.writerow({
                    'agency_id': record['agency_id'] or '',
                    'date': record['date'] or '',
                    'agency_name': record['agency_name'] or '',
                    'document_title': record['document_title'] or '',
                    'is_special_investigation': record['is_special_investigation'],
                    'sha256': record['sha256'],
                    'date_processed': record['date_processed']
                })
        
        logger.info(f"\nWrote {len(all_records)} records to {output_csv}")
    else:
        logger.warning("No records to write")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract basic document information from parquet files to CSV"
    )
    parser.add_argument(
        "--parquet-dir",
        default="pdf_parsing/parquet_files",
        help="Directory containing parquet files (default: pdf_parsing/parquet_files)"
    )
    parser.add_argument(
        "-o", "--output",
        default="document_info.csv",
        help="Output CSV file path (default: document_info.csv)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug output"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    process_parquet_files(args.parquet_dir, args.output)


if __name__ == "__main__":
    main()
