#!/bin/bash

# Build script for generating website data and building the site

set -e  # Exit on error

echo "==> Step 0: Installing Python dependencies with pip..."
# Install dependencies from pyproject.toml
cd ..
pip install -e .
cd website

echo ""
echo "==> Step 1: Generating JSON data for website..."
python3 generate_website_data.py \
  --document-csv ../pdf_parsing/document_info.csv \
  --sir-summaries-csv ../pdf_parsing/sir_summaries.csv \
  --sir-violation-levels-csv ../pdf_parsing/sir_violation_levels.csv \
  --keyword-reduction-csv ../pdf_parsing/violation_curation_keyword_reduction.csv \
  --facility-info-csv ../facility_information/facility_information.csv \
  --output-dir public/data

echo ""
echo "==> Step 2: Exporting parquet documents to individual JSON files..."
python3 export_parquet_to_json.py \
  --parquet-dir ../pdf_parsing/parquet_files \
  --output-dir public/documents \
  --document-csv ../pdf_parsing/document_info.csv \
  --sir-summaries-csv ../pdf_parsing/sir_summaries.csv \
  --sir-violation-levels-csv ../pdf_parsing/sir_violation_levels.csv \
  --keyword-reduction-csv ../pdf_parsing/violation_curation_keyword_reduction.csv

echo ""
echo "==> Step 3: Building website with Vite..."
npm run build

echo ""
echo "==> Build complete! Output is in dist/"
