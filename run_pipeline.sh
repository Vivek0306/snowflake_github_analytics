#!/bin/bash

# Exit immediately if any command fails
set -e

echo "Activating python virtual environment..."

source venv/bin/activate


echo "Starting data ingestion pipeline..."

python ingestion/ingest_repos.py
python ingestion/ingest_commits.py
python ingestion/ingest_pulls.py
python ingestion/ingest_issues.py
python ingestion/ingest_contributors.py

echo "All data ingested. Loading to Snowflake..."
python ingestion/load_to_snowflake.py

echo "Pipeline completed successfully!"

