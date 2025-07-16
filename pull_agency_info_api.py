import csv
import requests
import json
import urllib.parse
import urllib3
import argparse
import os
from datetime import datetime

def get_all_agency_info():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://michildwelfarepubliclicensingsearch.michigan.gov/licagencysrch/webruntime/api/apex/execute"

    params = {
        "cacheable": "true",
        "classname": "@udd/01p8z0000009E4V",
        "isContinuation": "false",
        "method": "getAgenciesDetail",
        "namespace": "",
        "params": json.dumps({"recordId": None}),
        "language": "en-US",
        "asGuest": "true",
        "htmlEncode": "false"
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://michildwelfarepubliclicensingsearch.michigan.gov/licagencysrch/'
    }

    try:
        print("GET request with recordId=null")
        response = requests.get(base_url, params=params, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"GET request with recordId=null failed: {e}")
        return None

def get_agency_details(record_id):
    """
    GET request with URL parameters directly to the API endpoint
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://michildwelfarepubliclicensingsearch.michigan.gov/licagencysrch/webruntime/api/apex/execute"

    # Build the exact URL from your example
    params = {
        "cacheable": "true",
        "classname": "@udd/01p8z0000009E4V",
        "isContinuation": "false",
        "method": "getAgenciesDetail",
        "namespace": "",
        "params": json.dumps({"recordId": record_id}),
        "language": "en-US",
        "asGuest": "true",
        "htmlEncode": "false"
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://michildwelfarepubliclicensingsearch.michigan.gov/licagencysrch/'
    }

    try:
        print("Method 1: GET request with URL parameters")
        response = requests.get(base_url, params=params, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Method 1 failed: {e}")
        return None

# Get the files
def get_content_details_method(record_id):
    """
    POST with JSON payload directly to the API endpoint
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://michildwelfarepubliclicensingsearch.michigan.gov/licagencysrch/webruntime/api/apex/execute"

    # JSON payload
    payload = {
        "namespace": "",
        "classname": "@udd/01p8z0000009E4V",
        "method": "getContentDetails",
        "isContinuation": False,
        "params": {
            "recordId": record_id
        },
        "cacheable": False
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://michildwelfarepubliclicensingsearch.michigan.gov',
        'Referer': 'https://michildwelfarepubliclicensingsearch.michigan.gov/licagencysrch/'
    }

    try:
        print("POST with JSON payload directly to the API endpoint")
        print(f"Payload: {json.dumps(payload, indent=2)}")

        response = requests.post(
            base_url,
            json=payload,
            headers=headers,
            verify=False,
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"POST with JSON payload directly to the API endpoint failed: {e}")
        if 'response' in locals():
            print(f"Response content: {response.text}")
        return None

def merge_agency_info(agency_csv, output_dir = ".", remove_files=False):
    """
    Merges the agency details into the all agency info dictionary.
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    agency_ids = []
    with open(agency_csv, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            agency_id = row.get('agencyId')
            if agency_id:
                agency_ids.append(agency_id)

    # Merge PDF content details for each agency

    combined_rows = []
    header = []
    for agency_id in agency_ids:
        pdf_csv = os.path.join(output_dir, f"{agency_id}_pdf_content_details.csv")
        if os.path.exists(pdf_csv):
            with open(pdf_csv, mode='r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                missing_agency_id = 'agency_id' not in header
                # Only add agency_id if not already present in header
                if missing_agency_id:
                    header = ['agency_id'] + header
                for row in reader:
                    combined_rows.append([agency_id] + row if missing_agency_id else row)
        else:
            print(f"Warning: PDF content details CSV not found for agency ID {agency_id}, skipping...")
            continue

    # Write out the combined CSV
    combined_csv = os.path.join(output_dir, f"{date_str}_combined_pdf_content_details.csv")
    with open(combined_csv, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        # Write header: agency_id + original header
        writer.writerow(['agency_id'] + header)
        writer.writerows(combined_rows)

    print(f"Combined PDF content details written to {combined_csv}")
    # If remove files then remove each file
    if remove_files:
        for agency_id in agency_ids:
            pdf_csv = os.path.join(output_dir, f"{agency_id}_pdf_content_details.csv")
            json_path = os.path.join(output_dir, f"{agency_id}_pdf_content_details.json")
            if os.path.exists(pdf_csv):
                os.remove(pdf_csv)
                print(f"Removed file: {pdf_csv}")
            if os.path.exists(json_path):
                os.remove(json_path)
                print(f"Removed file: {json_path}")
    return combined_csv

# Test the functions
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Child Welfare Licensing agency PDFs from Michigan's public licensing search.")
    parser.add_argument("--output-dir", dest="output_dir", help="Directory to save the CSV and JSON files", default="./")
    parser.add_argument("--overwrite", dest="overwrite", help="Overwrite existing files", default=True)
    parser.add_argument("--remove-files", dest="remove_files", help="Remove individual agency files after merging", default=True)
    parser.add_argument("--verbose", dest="verbose", help="Enable verbose output", default=False, action='store_true')
    args = parser.parse_args()
    output_dir = args.output_dir

    # # Patch all print statements in functions
    # builtins.print = lambda *a, **kw: log_print(' '.join(str(x) for x in a), logging.INFO)

    os.makedirs(output_dir, exist_ok=True)

    all_agency_info = get_all_agency_info()
    print(json.dumps(all_agency_info, indent=2))
    date_str = datetime.now().strftime("%Y-%m-%d")
    agency_file = os.path.join(output_dir, f"{date_str}_all_agency_info.json")

    with open(agency_file, "w", encoding="utf-8") as f:
        json.dump(all_agency_info, f, indent=2, ensure_ascii=False)

    print("All agency information saved to all_agency_info.json")

    # Extract the list from all_agency_info
    agency_list = (
        all_agency_info.get('returnValue', {})
        .get('objectData', {})
        .get('responseResult', [])
    )

    # Define the columns to keep
    keep_cols = [
        "Address",
        "agencyId",
        "AgencyName",
        "AgencyType",
        "City",
        "County",
        "LicenseEffectiveDate",
        "LicenseeGroupOrganizationName",
        "LicenseExpirationDate",
        "LicenseNumber",
        "LicenseStatus",
        "Phone",
        "ZipCode"
    ]

    # Update CSV filename to include date
    agency_csv_file = os.path.join(output_dir, f"{date_str}_agency_info.csv")
    with open(agency_csv_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keep_cols, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for agency in agency_list:
            row = {col: agency.get(col, "") for col in keep_cols}
            writer.writerow(row)
    print(f"Agency info written to {agency_csv_file}")

    keep_cols = ['FileExtension', 'CreatedDate', 'Title', 'ContentBodyId', 'Id', 'ContentDocumentId']

    # Run for each agency id
    for agency in agency_list:
        record_id = agency.get('agencyId')
        csv_file = os.path.join(output_dir, f"{record_id}_pdf_content_details.csv")
        if not record_id:
            print(f"Skipping agency ID {record_id} as it is empty.")
            continue
        if args.overwrite and os.path.exists(csv_file):
            print(f"File {csv_file} already exists and overwrite is enabled, skipping agency ID {record_id}.")
            continue

        print(f"Processing agency ID: {record_id}")
        pdf_results = get_content_details_method(record_id)

        if pdf_results:
            print(f"PDF Content Details for {record_id}:")
            # print(json.dumps(pdf_results, indent=2))
            # Save full JSON response to file
            json_file = os.path.join(output_dir, f"{record_id}_pdf_content_details.json")
            with open(json_file, "w", encoding="utf-8") as jf:
                json.dump(pdf_results, jf, indent=2, ensure_ascii=False)
            print(f"Full JSON results written to {json_file}")

            # Write top-level keys/values to CSV
            csv_file = os.path.join(output_dir, f"{record_id}_pdf_content_details.csv")
            with open(csv_file, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                # Write the header
                writer.writerow(keep_cols)
                for p in pdf_results.get('returnValue', {}).get('contentVersionRes', []):
                    row_data = [record_id] + [p.get(k, "") for k in keep_cols]
                    writer.writerow(row_data)

            print(f"Top-level JSON results written to {csv_file}")
        else:
            print(f"Failed to retrieve PDF content details for agency ID: {record_id}")

    merge_agency_info(agency_csv_file, output_dir, remove_files=args.remove_files)