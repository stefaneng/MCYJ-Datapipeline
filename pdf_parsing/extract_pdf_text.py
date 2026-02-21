#!/usr/bin/env python3
"""
Extract text from PDF files using pdfplumber and save to compressed Parquet files.

Each PDF is hashed using SHA256, and the output contains:
- sha256: SHA256 hash of the PDF file
- text: List of strings, one per page
- dateprocessed: ISO 8601 timestamp when the PDF was processed

Output is organized in a subdirectory with multiple timestamped Parquet files,
each representing a new ingestion batch. PDFs that are already processed
(based on their SHA256 hash across all existing Parquet files) are skipped.

Parquet files use compression (zstd) for efficient storage and are named
with timestamps: YYYYMMDD_HHMMSS_pdf_text.parquet
"""
import argparse
import hashlib
import json
import logging
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Set

import pandas as pd
import pdfplumber

# Set up logger
logger = logging.getLogger(__name__)


def calculate_sha256(file_path: str) -> str:
    """Calculate SHA256 hash of a file with broad Python-version compatibility."""
    with open(file_path, "rb") as f:
        if hasattr(hashlib, "file_digest"):
            digest = hashlib.file_digest(f, "sha256")
            return digest.hexdigest()

        # Fallback for Python versions without hashlib.file_digest (e.g., 3.9)
        hasher = hashlib.sha256()
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
        return hasher.hexdigest()


def load_processed_ids(parquet_dir: str) -> Set[str]:
    """Load set of already processed PDF IDs from all Parquet files in output directory."""
    processed = set()
    output_path = Path(parquet_dir)

    if not output_path.exists():
        return processed

    # Find all parquet files in the directory
    parquet_files = list(output_path.glob("*.parquet"))

    for parquet_file in parquet_files:
        try:
            df = pd.read_parquet(parquet_file)
            if 'sha256' in df.columns:
                processed.update(df['sha256'].tolist())
        except Exception as e:
            logger.warning(f"Could not read {parquet_file}: {e}")
            continue

    return processed


def load_all_records(parquet_dir: str) -> Dict[str, dict]:
    """Load all records from Parquet files in output directory, indexed by sha256."""
    records = {}
    output_path = Path(parquet_dir)

    if not output_path.exists():
        return records

    # Find all parquet files in the directory
    parquet_files = list(output_path.glob("*.parquet"))

    for parquet_file in parquet_files:
        try:
            df = pd.read_parquet(parquet_file)
            for _, row in df.iterrows():
                record = row.to_dict()
                if 'sha256' in record:
                    records[record['sha256']] = record
        except Exception as e:
            logger.warning(f"Could not read {parquet_file}: {e}")
            continue

    return records


def extract_text_from_pdf(pdf_path: str) -> list[str]:
    """Extract text from PDF, returning a list of strings (one per page)."""
    pages_text = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            pages_text.append(text)
    return pages_text


def format_time(seconds: float) -> str:
    """Format time in seconds to human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def process_directory(pdf_dir: str, parquet_dir: str, limit: int = None) -> None:
    """Process all PDFs in directory and save results to timestamped Parquet file.

    Args:
        pdf_dir: Directory containing PDF files
        parquet_dir: Output directory for Parquet files
        limit: Maximum number of PDFs to process (excludes already-processed/skipped files)
    """
    pdf_dir_path = Path(pdf_dir)

    if not pdf_dir_path.exists():
        logger.error(f"Directory '{pdf_dir}' does not exist")
        sys.exit(1)

    if not pdf_dir_path.is_dir():
        logger.error(f"'{pdf_dir}' is not a directory")
        sys.exit(1)

    # Create output directory if it doesn't exist
    output_path = Path(parquet_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Load already processed IDs from all existing Parquet files
    processed_ids = load_processed_ids(parquet_dir)
    logger.info(f"Found {len(processed_ids)} already processed PDFs across existing Parquet files")

    # Find all PDF files
    pdf_files = list(pdf_dir_path.glob("*.pdf")) + list(pdf_dir_path.glob("*.PDF"))
    logger.info(f"Found {len(pdf_files)} PDF files in directory")

    # Count how many actually need processing by checking which ones are already done
    new_files_count = 0
    for pdf_path in pdf_files:
        try:
            pdf_hash = calculate_sha256(str(pdf_path))
            if pdf_hash not in processed_ids:
                new_files_count += 1
        except Exception:
            # If we can't hash it, count it as needing processing
            new_files_count += 1

    to_process_count = new_files_count

    # Apply limit if specified
    if limit is not None:
        to_process_count = min(to_process_count, limit)
        logger.info(f"Found {new_files_count} new PDFs, will process up to {to_process_count} (limit: {limit})")
    else:
        logger.info(f"Found {new_files_count} new PDFs to process")

    if to_process_count == 0:
        logger.info("No new PDFs to process!")
        return

    # Generate timestamped filename for this batch
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_parquet = output_path / f"{timestamp}_pdf_text.parquet"

    # Collect all records for this batch
    records = []
    processed_count = 0
    skipped_count = 0
    error_count = 0
    start_time = time.time()

    for idx, pdf_path in enumerate(sorted(pdf_files), 1):
        try:
            # Calculate SHA256 hash
            pdf_hash = calculate_sha256(str(pdf_path))

            # Skip if already processed
            if pdf_hash in processed_ids:
                logger.info(f"[{idx}/{len(pdf_files)}] Skipping (already processed): {pdf_path.name}")
                skipped_count += 1
                continue

            # Check if we've hit the limit
            if limit is not None and processed_count >= limit:
                logger.info(f"Reached processing limit of {limit} PDFs, stopping")
                break

            # Extract text
            logger.info(f"[{idx}/{len(pdf_files)}] Processing: {pdf_path.name}")
            pages_text = extract_text_from_pdf(str(pdf_path))

            # Create record with timestamp
            record = {
                "sha256": pdf_hash,
                "text": pages_text,  # List of strings, one per page
                "dateprocessed": datetime.now().isoformat()
            }

            records.append(record)
            processed_count += 1

            # Add to processed_ids to prevent duplicates within the same batch
            processed_ids.add(pdf_hash)

            # Calculate time estimates
            elapsed_time = time.time() - start_time
            avg_time_per_pdf = elapsed_time / processed_count
            remaining = to_process_count - processed_count
            estimated_remaining = avg_time_per_pdf * remaining

            elapsed_str = format_time(elapsed_time)
            remaining_str = format_time(estimated_remaining)

            logger.info(f"  -> Processed {len(pages_text)} pages")
            logger.info(f"  -> Time: {elapsed_str} elapsed, ~{remaining_str} remaining (est.)")

        except Exception as e:
            logger.error(f"Error processing {pdf_path.name}: {e}")
            error_count += 1
            continue

    # Save all records to Parquet file with compression
    if records:
        df = pd.DataFrame(records)
        df.to_parquet(output_parquet, compression='zstd', index=False)
        logger.info(f"Saved {len(records)} records to {output_parquet}")
    else:
        logger.info("No new records to save")

    logger.info("Summary:")
    logger.info(f"  Processed: {processed_count}")
    logger.info(f"  Skipped: {skipped_count}")
    logger.info(f"  Errors: {error_count}")


def spot_check(pdf_dir: str, parquet_dir: str, num_checks: int) -> None:
    """Spot check existing records by re-extracting and comparing."""
    pdf_dir_path = Path(pdf_dir)

    if not pdf_dir_path.exists():
        logger.error(f"Directory '{pdf_dir}' does not exist")
        sys.exit(1)

    if not pdf_dir_path.is_dir():
        logger.error(f"'{pdf_dir}' is not a directory")
        sys.exit(1)

    # Load all existing records from Parquet files
    logger.info(f"Loading existing records from {parquet_dir}...")
    records = load_all_records(parquet_dir)
    logger.info(f"Loaded {len(records)} existing records")

    if len(records) == 0:
        logger.info("No records to spot check!")
        return

    # Find all PDF files
    pdf_files = list(pdf_dir_path.glob("*.pdf")) + list(pdf_dir_path.glob("*.PDF"))

    # Filter to only PDFs we have records for
    pdf_files_with_records = []
    for pdf_path in pdf_files:
        try:
            pdf_hash = calculate_sha256(str(pdf_path))
            if pdf_hash in records:
                pdf_files_with_records.append((pdf_path, pdf_hash))
        except Exception:
            continue

    if len(pdf_files_with_records) == 0:
        logger.info("No PDFs found that match existing records!")
        return

    # Sample up to num_checks PDFs
    sample_size = min(num_checks, len(pdf_files_with_records))
    sample = random.sample(pdf_files_with_records, sample_size)

    logger.info(f"Spot checking {sample_size} PDFs...")

    passed = 0
    failed = 0

    for pdf_path, pdf_hash in sample:
        try:
            logger.info(f"Checking: {pdf_path.name}")

            # Re-extract text
            pages_text = extract_text_from_pdf(str(pdf_path))

            # Get existing record
            existing_record = records[pdf_hash]
            existing_text = existing_record["text"]

            # Compare
            if pages_text == existing_text:
                logger.info(f"  ✓ PASS - {len(pages_text)} pages match")
                passed += 1
            else:
                logger.error(f"  ✗ FAIL - Text mismatch!")
                logger.error(f"    Expected {len(existing_text)} pages, got {len(pages_text)} pages")
                if len(pages_text) == len(existing_text):
                    # Same number of pages, check which pages differ
                    for i, (old, new) in enumerate(zip(existing_text, pages_text)):
                        if old != new:
                            logger.error(f"    Page {i+1} differs")
                failed += 1

        except Exception as e:
            logger.error(f"  ✗ ERROR: {e}")
            failed += 1

    logger.info("Spot Check Summary:")
    logger.info(f"  Passed: {passed}/{sample_size}")
    logger.info(f"  Failed: {failed}/{sample_size}")

    if failed == 0:
        logger.info("✓ All spot checks passed!")
    else:
        logger.error(f"✗ {failed} spot check(s) failed")
        sys.exit(1)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract text from PDFs and save to compressed Parquet files"
    )
    parser.add_argument(
        "--pdf-dir",
        required=True,
        help="Directory containing PDF files to process"
    )
    parser.add_argument(
        "-o", "--parquet-dir",
        default="pdf_parsing/parquet_files",
        help="Output directory for timestamped Parquet files (default: pdf_parsing/parquet_files)"
    )
    parser.add_argument(
        "--spot-check",
        type=int,
        metavar="N",
        help="Spot check N random PDFs by re-extracting and comparing with existing records"
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Process at most N PDFs (skipped files don't count toward limit)"
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
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    if args.spot_check is not None:
        spot_check(args.pdf_dir, args.parquet_dir, args.spot_check)
    else:
        process_directory(args.pdf_dir, args.parquet_dir, limit=args.limit)


if __name__ == "__main__":
    main()
