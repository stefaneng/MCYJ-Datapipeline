#!/usr/bin/env python3
"""
Generate JSON data files for the website from CSV files.

This script processes the document info CSV to create
JSON files that can be consumed by the web frontend.
"""

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional

from keyword_reduction import load_keyword_reduction_map, apply_keyword_reduction


def load_sir_summaries_csv(csv_path):
    """Load SIR summaries CSV and create a lookup by SHA256."""
    summaries_by_sha = {}
    
    if not os.path.exists(csv_path):
        print(f"Warning: SIR summaries file not found: {csv_path}")
        return summaries_by_sha
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sha256 = row.get('sha256', '').strip()
            if not sha256:
                continue
            
            summaries_by_sha[sha256] = {
                'summary': row.get('response', ''),  # 'response' column contains the AI-generated summary text
                'violation': row.get('violation', '')
            }
    
    return summaries_by_sha


def load_sir_violation_levels_csv(csv_path, keyword_map: Optional[Dict[str, str]] = None):
    """Load SIR violation levels CSV and create a lookup by SHA256."""
    levels_by_sha = {}
    
    if not os.path.exists(csv_path):
        print(f"Warning: SIR violation levels file not found: {csv_path}")
        return levels_by_sha
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sha256 = row.get('sha256', '').strip()
            if not sha256:
                continue
            
            # Parse keywords from JSON string if present
            keywords_str = row.get('keywords', '')
            keywords = []
            if keywords_str:
                try:
                    keywords = json.loads(keywords_str)
                except (json.JSONDecodeError, ValueError):
                    print(f"Warning: Failed to parse keywords for {sha256}")
                    keywords = []
            
            # Apply keyword reduction if map is provided
            if keyword_map:
                keywords = apply_keyword_reduction(keywords, keyword_map)
            
            levels_by_sha[sha256] = {
                'level': row.get('level', ''),
                'justification': row.get('justification', ''),
                'keywords': keywords
            }
    
    return levels_by_sha


def load_document_info_csv(csv_path, sir_summaries=None, sir_violation_levels=None):
    """Load document info CSV and group by agency."""
    documents_by_agency = defaultdict(list)
    agency_names = {}  # Map agency_id to agency_name
    
    if sir_summaries is None:
        sir_summaries = {}
    if sir_violation_levels is None:
        sir_violation_levels = {}
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            agency_id = row.get('agency_id', '').strip()
            agency_name = row.get('agency_name', '').strip()
            
            if not agency_id:
                continue
            
            # Track agency name for this ID
            if agency_id and agency_name:
                agency_names[agency_id] = agency_name
            
            sha256 = row.get('sha256', '')
            document = {
                'date': row.get('date', ''),
                'agency_name': agency_name,
                'document_title': row.get('document_title', ''),
                'is_special_investigation': row.get('is_special_investigation', 'False').lower() in ('true', '1', 'yes'),
                'sha256': sha256,
                'date_processed': row.get('date_processed', '')
            }
            
            # Add SIR summary if available
            if sha256 in sir_summaries:
                document['sir_summary'] = sir_summaries[sha256]
            
            # Add SIR violation level if available
            if sha256 in sir_violation_levels:
                document['sir_violation_level'] = sir_violation_levels[sha256]
            
            documents_by_agency[agency_id].append(document)
    
    return documents_by_agency, agency_names


def load_facility_information_csv(csv_path):
    """Load facility information CSV and create a lookup by LicenseNumber.
    
    The LicenseNumber field in facility_information.csv matches the agency_id
    field in document_info.csv, so we key by LicenseNumber for proper joining.
    """
    facilities_by_license = {}
    
    if not os.path.exists(csv_path):
        print(f"Warning: Facility information file not found: {csv_path}")
        return facilities_by_license
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            license_number = row.get('LicenseNumber', '').strip()
            if not license_number:
                continue
            
            facilities_by_license[license_number] = {
                'LicenseNumber': license_number,
                'Address': row.get('Address', ''),
                'AgencyType': row.get('AgencyType', ''),
                'City': row.get('City', ''),
                'County': row.get('County', ''),
                'LicenseEffectiveDate': row.get('LicenseEffectiveDate', ''),
                'LicenseeGroupOrganizationName': row.get('LicenseeGroupOrganizationName', ''),
                'LicenseExpirationDate': row.get('LicenseExpirationDate', ''),
                'LicenseStatus': row.get('LicenseStatus', ''),
                'Phone': row.get('Phone', ''),
                'ZipCode': row.get('ZipCode', '')
            }
    
    return facilities_by_license


def generate_json_files(document_csv, output_dir, sir_summaries_csv=None, sir_violation_levels_csv=None, keyword_reduction_csv=None, facility_info_csv=None):
    """Generate JSON files for the website."""
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Load keyword reduction map if provided
    keyword_map = {}
    if keyword_reduction_csv:
        print("Loading keyword reduction mappings...")
        keyword_map = load_keyword_reduction_map(keyword_reduction_csv)
    
    # Load SIR summaries if provided
    sir_summaries = {}
    if sir_summaries_csv:
        print("Loading SIR summaries data...")
        sir_summaries = load_sir_summaries_csv(sir_summaries_csv)
        print(f"Loaded {len(sir_summaries)} SIR summaries")
    
    # Load SIR violation levels if provided
    sir_violation_levels = {}
    if sir_violation_levels_csv:
        print("Loading SIR violation levels data...")
        sir_violation_levels = load_sir_violation_levels_csv(sir_violation_levels_csv, keyword_map)
        print(f"Loaded {len(sir_violation_levels)} SIR violation levels")
    
    # Load facility information if provided
    facility_info = {}
    if facility_info_csv:
        print("Loading facility information data...")
        facility_info = load_facility_information_csv(facility_info_csv)
        print(f"Loaded {len(facility_info)} facility records")
    
    # Load document info data
    print("Loading document info data...")
    documents_by_agency, agency_names = load_document_info_csv(document_csv, sir_summaries, sir_violation_levels)
    
    # Build agency list from document data
    print("Building agency list from documents...")
    agency_data = []
    
    for agency_id, documents in documents_by_agency.items():
        # Get agency name from the documents data
        agency_name = agency_names.get(agency_id, 'Unknown Agency')
        
        agency_info = {
            'agencyId': agency_id,
            'AgencyName': agency_name,
            'documents': documents,
            'total_reports': len(documents)
        }
        
        # Add facility information if available
        if agency_id in facility_info:
            agency_info['facility'] = facility_info[agency_id]
        
        agency_data.append(agency_info)
    
    # Sort by agency name
    agency_data.sort(key=lambda x: x.get('AgencyName', ''))
    
    # Write full data file
    output_file = os.path.join(output_dir, 'agencies_data.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(agency_data, f, indent=2)
    print(f"Wrote full data to {output_file}")
    
    # Write summary file (without full documents list for faster loading)
    summary_data = []
    for agency in agency_data:
        summary = {
            'agencyId': agency['agencyId'],
            'AgencyName': agency['AgencyName'],
            'total_reports': agency['total_reports']
        }
        # Include facility info in summary as well
        if 'facility' in agency:
            summary['facility'] = agency['facility']
        summary_data.append(summary)
    
    summary_file = os.path.join(output_dir, 'agencies_summary.json')
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2)
    print(f"Wrote summary to {summary_file}")
    
    # Write facility information summary file for facilities page
    # Note: facility_info is keyed by LicenseNumber which matches agency_id in documents
    if facility_info:
        facility_data = []
        for license_number, fac in facility_info.items():
            facility_entry = {
                'agencyId': license_number,  # Use LicenseNumber as agencyId for consistency
                'AgencyName': fac.get('LicenseeGroupOrganizationName', '') or agency_names.get(license_number, 'Unknown Agency'),
                **fac
            }
            facility_data.append(facility_entry)
        
        facility_data.sort(key=lambda x: x.get('AgencyName', ''))
        
        facility_file = os.path.join(output_dir, 'facilities_data.json')
        with open(facility_file, 'w', encoding='utf-8') as f:
            json.dump(facility_data, f, indent=2)
        print(f"Wrote facility data to {facility_file}")
    
    print(f"\nProcessed {len(agency_data)} agencies")
    print(f"Total reports: {sum(a['total_reports'] for a in agency_data)}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate JSON data files for the website from document info CSV only"
    )
    parser.add_argument(
        "--document-csv",
        required=True,
        help="Path to document info CSV file"
    )
    parser.add_argument(
        "--sir-summaries-csv",
        help="Path to SIR summaries CSV file (optional)"
    )
    parser.add_argument(
        "--sir-violation-levels-csv",
        help="Path to SIR violation levels CSV file (optional)"
    )
    parser.add_argument(
        "--keyword-reduction-csv",
        help="Path to keyword reduction CSV file (optional)"
    )
    parser.add_argument(
        "--facility-info-csv",
        help="Path to facility information CSV file (optional)"
    )
    parser.add_argument(
        "--output-dir",
        default="public/data",
        help="Output directory for JSON files"
    )
    
    args = parser.parse_args()
    
    # Validate input files
    if not os.path.exists(args.document_csv):
        print(f"Error: Document info CSV not found: {args.document_csv}")
        sys.exit(1)
    
    generate_json_files(
        args.document_csv,
        args.output_dir,
        args.sir_summaries_csv,
        args.sir_violation_levels_csv,
        args.keyword_reduction_csv,
        args.facility_info_csv
    )


if __name__ == "__main__":
    main()
