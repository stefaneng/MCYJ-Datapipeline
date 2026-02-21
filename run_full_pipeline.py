#!/usr/bin/env python3
"""
Run the full MCYJ download pipeline in one command.

Pipeline steps:
1) Preflight SHA check: if existing metadata rows are missing sha256, run backfill first
2) Fetch agency list from API
3) Iterate agency content details and download immediately when a new file is found
4) Stop after --limit newly downloaded files (limit counts new downloads only)
5) Parse newly downloaded PDFs to parquet text (pdf_parsing)
6) Write cumulative metadata CSV and run-specific metadata output CSV
"""

import argparse
import csv
import hashlib
import os
import shutil
import tempfile
from datetime import datetime, timezone
from typing import Dict, List, Optional

from download_pdf import download_michigan_pdf
from pdf_parsing.extract_pdf_text import process_directory as process_pdf_directory
from pull_agency_info_api import get_all_agency_info, get_content_details_method


DEFAULT_METADATA_OUTPUT_DIR = "metadata_output"
DEFAULT_DOWNLOAD_DIR = "Downloads"
DEFAULT_METADATA_FILENAME = "facility_information_metadata.csv"
DEFAULT_RUN_OUTPUT_FILENAME = "latest_downloaded_metadata.csv"
DEFAULT_DOWNLOAD_DB_FILENAME = "downloaded_files_database.csv"
DEFAULT_PARQUET_DIR = "pdf_parsing/parquet_files"


def load_csv_rows(csv_path: str) -> List[Dict[str, str]]:
    if not os.path.exists(csv_path):
        return []
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return [row for row in reader]


def build_metadata_index(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    by_id = {}
    for row in rows:
        content_document_id = (row.get("ContentDocumentId") or "").strip()
        if content_document_id:
            by_id[content_document_id] = row
    return by_id


def compute_sha256(file_path: str, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(file_path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def resolve_local_file_path(row: Dict[str, str], download_dir: str) -> Optional[str]:
    downloaded_path = (row.get("downloaded_path") or "").strip()
    downloaded_filename = (row.get("downloaded_filename") or "").strip()
    generated_filename = (row.get("generated_filename") or "").strip()

    candidates = []
    if downloaded_path:
        if os.path.isabs(downloaded_path):
            candidates.append(downloaded_path)
        else:
            candidates.append(os.path.join(download_dir, downloaded_path))
    if downloaded_filename:
        candidates.append(os.path.join(download_dir, downloaded_filename))
    if generated_filename:
        candidates.append(os.path.join(download_dir, generated_filename))

    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def preflight_backfill_missing_sha(
    metadata_rows: List[Dict[str, str]],
    metadata_by_id: Dict[str, Dict[str, str]],
    download_dir: str,
) -> int:
    """Fill missing sha256 for rows that already have a local file path."""
    updated = 0
    for row in metadata_rows:
        content_document_id = (row.get("ContentDocumentId") or "").strip()
        if not content_document_id:
            continue

        sha256 = (row.get("sha256") or "").strip()
        if sha256:
            continue

        local_path = resolve_local_file_path(row, download_dir)
        if not local_path:
            continue

        row["downloaded_path"] = local_path
        row["downloaded_filename"] = os.path.basename(local_path)
        row["generated_filename"] = row.get("generated_filename") or os.path.basename(local_path)
        row["sha256"] = compute_sha256(local_path)
        row["download_status"] = row.get("download_status") or "backfilled_preflight"
        row["id_match_checked"] = "true"
        metadata_by_id[content_document_id] = row
        updated += 1

    return updated


def parse_created_date_to_iso(created_date: str) -> Optional[str]:
    if not created_date:
        return None
    created_date = created_date.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(created_date, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def build_row(record: Dict[str, str], agency_name: str, agency_id: str, out_path: str, sha256: str) -> Dict[str, str]:
    return {
        "generated_filename": os.path.basename(out_path),
        "agency_name": agency_name,
        "agency_id": agency_id,
        "FileExtension": record.get("FileExtension", ""),
        "CreatedDate": record.get("CreatedDate", ""),
        "Title": record.get("Title", ""),
        "ContentBodyId": record.get("ContentBodyId", ""),
        "Id": record.get("Id", ""),
        "ContentDocumentId": record.get("ContentDocumentId", ""),
        "downloaded_filename": os.path.basename(out_path),
        "downloaded_path": out_path,
        "sha256": sha256,
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "download_status": "downloaded",
        "id_match_checked": "true",
    }


def write_csv_rows(csv_path: str, rows: List[Dict[str, str]]) -> None:
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)

    fieldnames: List[str] = []
    default_fields = [
        "generated_filename", "agency_name", "agency_id", "FileExtension", "CreatedDate", "Title",
        "ContentBodyId", "Id", "ContentDocumentId", "downloaded_filename", "downloaded_path", "sha256",
        "downloaded_at_utc", "download_status", "id_match_checked",
    ]
    for row in rows:
        for key in row.keys():
            if key and key not in fieldnames:
                fieldnames.append(key)
    for field in default_fields:
        if field not in fieldnames:
            fieldnames.append(field)

    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def parse_new_downloads_to_parquet(new_rows: List[Dict[str, str]], parquet_dir: str) -> None:
    """Parse newly downloaded PDFs into parquet by staging only this run's files."""
    if not new_rows:
        print("No new downloads in this run; skipping PDF parsing step.")
        return

    with tempfile.TemporaryDirectory(prefix="mcyj_new_downloads_") as staging_dir:
        staged_count = 0
        for row in new_rows:
            file_path = (row.get("downloaded_path") or "").strip()
            if not file_path or not os.path.exists(file_path):
                continue

            target_path = os.path.join(staging_dir, os.path.basename(file_path))
            try:
                os.symlink(file_path, target_path)
            except OSError:
                shutil.copy2(file_path, target_path)
            staged_count += 1

        if staged_count == 0:
            print("No valid downloaded files available for parsing; skipping PDF parsing step.")
            return

        print(f"Running PDF parsing on {staged_count} newly downloaded files...")
        process_pdf_directory(staging_dir, parquet_dir, limit=None)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run full MCYJ metadata/download pipeline. "
            "Downloads files incrementally when a new ContentDocumentId is discovered. "
            "--limit counts only newly downloaded files."
        )
    )
    parser.add_argument(
        "--metadata-output-dir",
        default=DEFAULT_METADATA_OUTPUT_DIR,
        help="Directory for API metadata outputs (default: metadata_output)",
    )
    parser.add_argument(
        "--download-dir",
        default=DEFAULT_DOWNLOAD_DIR,
        help="Directory containing downloaded PDFs (default: Downloads)",
    )
    parser.add_argument(
        "--metadata-csv",
        default=None,
        help=(
            "Downloaded-file metadata CSV (legacy alias). "
            "If provided, this path is used as the download database CSV."
        ),
    )
    parser.add_argument(
        "--download-db-csv",
        default=None,
        help=(
            "Persistent download database CSV used as source of truth for downloaded files "
            "(default: <metadata-output-dir>/downloaded_files_database.csv)"
        ),
    )
    parser.add_argument(
        "--run-output-csv",
        default=None,
        help=(
            "Run-specific metadata output CSV for newly downloaded files only "
            "(default: <metadata-output-dir>/latest_downloaded_metadata.csv)"
        ),
    )
    parser.add_argument(
        "--sleep",
        dest="sleep_seconds",
        type=float,
        default=0.0,
        help="Seconds to sleep between downloads",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of newly downloaded files",
    )
    parser.add_argument(
        "--parquet-dir",
        default=DEFAULT_PARQUET_DIR,
        help="Output directory for parsed PDF text parquet files (default: pdf_parsing/parquet_files)",
    )
    parser.add_argument(
        "--skip-pdf-parsing",
        action="store_true",
        help="Skip PDF text extraction step after downloads",
    )
    args = parser.parse_args()

    root_dir = os.path.dirname(os.path.abspath(__file__))
    metadata_output_dir = os.path.join(root_dir, args.metadata_output_dir)
    download_dir = os.path.join(root_dir, args.download_dir)
    parquet_dir = os.path.join(root_dir, args.parquet_dir)
    metadata_csv = (
        args.download_db_csv
        or args.metadata_csv
        or os.path.join(metadata_output_dir, DEFAULT_DOWNLOAD_DB_FILENAME)
    )
    legacy_metadata_csv = os.path.join(download_dir, DEFAULT_METADATA_FILENAME)
    run_output_csv = args.run_output_csv or os.path.join(metadata_output_dir, DEFAULT_RUN_OUTPUT_FILENAME)

    os.makedirs(metadata_output_dir, exist_ok=True)
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(parquet_dir, exist_ok=True)

    metadata_rows = load_csv_rows(metadata_csv)
    if not metadata_rows and os.path.exists(legacy_metadata_csv) and legacy_metadata_csv != metadata_csv:
        legacy_rows = load_csv_rows(legacy_metadata_csv)
        if legacy_rows:
            metadata_rows = legacy_rows
            print(
                "Seeded download database from legacy metadata file: "
                f"{legacy_metadata_csv} ({len(legacy_rows)} rows)"
            )
    elif os.path.exists(legacy_metadata_csv) and legacy_metadata_csv != metadata_csv:
        legacy_rows = load_csv_rows(legacy_metadata_csv)
        if legacy_rows:
            current_ids = {
                (row.get("ContentDocumentId") or "").strip()
                for row in metadata_rows
                if (row.get("ContentDocumentId") or "").strip()
            }
            added = 0
            for row in legacy_rows:
                content_document_id = (row.get("ContentDocumentId") or "").strip()
                if content_document_id and content_document_id not in current_ids:
                    metadata_rows.append(row)
                    current_ids.add(content_document_id)
                    added += 1
            if added:
                print(f"Merged {added} legacy records from {legacy_metadata_csv} into download database in memory.")

    metadata_by_id = build_metadata_index(metadata_rows)
    print(f"Loaded {len(metadata_by_id)} records from download database: {metadata_csv}")

    # Step 1: Preflight SHA backfill on existing metadata rows
    prefilled = preflight_backfill_missing_sha(
        metadata_rows,
        metadata_by_id,
        download_dir,
    )
    if prefilled > 0:
        print(f"Preflight backfill updated sha256 for {prefilled} existing rows.")

    # Step 2: Fetch agencies once
    all_agency_info = get_all_agency_info()
    if not all_agency_info:
        raise RuntimeError("Failed to fetch agency information from API")

    agency_list = (
        all_agency_info.get("returnValue", {})
        .get("objectData", {})
        .get("responseResult", [])
    )
    print(f"Fetched {len(agency_list)} agencies. Starting incremental discovery/download.")

    new_rows: List[Dict[str, str]] = []
    attempted_new = 0

    for agency in agency_list:
        if args.limit is not None and len(new_rows) >= args.limit:
            break

        agency_id = (agency.get("agencyId") or "").strip()
        agency_name = (agency.get("AgencyName") or "").strip()
        if not agency_id:
            continue

        pdf_results = get_content_details_method(agency_id)
        if not pdf_results:
            continue

        records = pdf_results.get("returnValue", {}).get("contentVersionRes", [])
        for record in records:
            if args.limit is not None and len(new_rows) >= args.limit:
                break

            content_document_id = (record.get("ContentDocumentId") or "").strip()
            if not content_document_id:
                continue

            existing = metadata_by_id.get(content_document_id)
            if existing:
                existing_sha = (existing.get("sha256") or "").strip()
                if existing_sha:
                    continue

                local_path = resolve_local_file_path(existing, download_dir)
                if local_path:
                    existing["downloaded_path"] = local_path
                    existing["downloaded_filename"] = os.path.basename(local_path)
                    existing["generated_filename"] = existing.get("generated_filename") or os.path.basename(local_path)
                    existing["sha256"] = compute_sha256(local_path)
                    existing["download_status"] = "backfilled_existing"
                    existing["id_match_checked"] = "true"
                    metadata_by_id[content_document_id] = existing
                    continue

            attempted_new += 1
            created_date_iso = parse_created_date_to_iso(record.get("CreatedDate", ""))

            out_path = download_michigan_pdf(
                document_id=content_document_id,
                document_agency=agency_name if agency_name else None,
                document_name=record.get("Title", "") or None,
                document_date=created_date_iso,
                output_dir=download_dir,
            )

            if not out_path:
                continue

            sha256 = compute_sha256(out_path)
            new_row = build_row(record, agency_name, agency_id, out_path, sha256)
            new_rows.append(new_row)
            metadata_by_id[content_document_id] = new_row

            print(
                f"Downloaded new file {len(new_rows)}"
                f"/{args.limit if args.limit is not None else '?'}: {content_document_id}"
            )

            if args.sleep_seconds and args.sleep_seconds > 0:
                import time
                time.sleep(args.sleep_seconds)

    # Write cumulative metadata
    cumulative_rows = list(metadata_by_id.values())
    cumulative_rows.sort(key=lambda row: (row.get("agency_id", ""), row.get("ContentDocumentId", "")))
    write_csv_rows(metadata_csv, cumulative_rows)

    # Parse new downloads to parquet text files
    if not args.skip_pdf_parsing:
        parse_new_downloads_to_parquet(new_rows, parquet_dir)

    # Write run-specific output (new rows only)
    write_csv_rows(run_output_csv, new_rows)

    if args.limit is not None and len(new_rows) < args.limit:
        print(
            f"Limit requested {args.limit}, but only {len(new_rows)} new downloadable files were found."
        )

    print(f"Attempted new downloads: {attempted_new}")
    print(f"New files downloaded: {len(new_rows)}")
    print(f"Parquet output directory: {parquet_dir}")
    print(f"Download database output: {metadata_csv}")
    print(f"Run metadata output (new files only): {run_output_csv}")


if __name__ == "__main__":
    main()
