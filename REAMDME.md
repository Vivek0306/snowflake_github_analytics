# GitHub Analytics — dbt + Snowflake

An end-to-end ELT pipeline that ingests GitHub activity data and models it into an analytics-ready data warehouse.

## What it does

Pulls data from the GitHub REST API across 13 popular open-source repositories, loads it into Snowflake, and transforms it using dbt into facts, dimensions, and aggregations for analysis.

## Stack

- **Ingestion** — Python + PySpark (GitHub API → local Parquet)
- **Loading** — Snowflake internal stage + COPY INTO
- **Warehouse** — Snowflake
- **Transformation** — dbt Core
- **Source** — GitHub REST API

## Data collected

| Entity | Rows |
|---|---|
| Repos | 13 |
| Commits | 5,960 |
| Pull Requests | 6,000 |
| Issues | 1,847 |
| Contributors | 2,352 |

## Project structure

```
ingestion/        # PySpark scripts to fetch and load data
dbt_project/      # dbt models — staging and mart layers
data/raw/         # local Parquet files (gitignored)
```

## Schemas

- `RAW` — raw tables loaded directly from the API
- `STAGING` — cleaned and typed, 1:1 with raw
- `ANALYTICS` — facts, dimensions, and aggregations

## Repos tracked

apache/spark, apache/airflow, dbt-labs/dbt-core, pandas-dev/pandas,
tiangolo/fastapi, django/django, pytorch/pytorch, tensorflow/tensorflow,
microsoft/vscode, golang/go, rust-lang/rust, vercel/next.js,
snowflakedb/snowflake-connector-python

## Running the pipeline

1. Set up `.env` with GitHub token and Snowflake credentials
2. Run ingestion scripts in order:
   ```bash
   python ingestion/ingest_repos.py
   python ingestion/ingest_commits.py
   python ingestion/ingest_pull_requests.py
   python ingestion/ingest_issues.py
   python ingestion/ingest_contributors.py
   ```
3. Load into Snowflake:
   ```bash
   python ingestion/load_to_snowflake.py
   ```
4. Run dbt transformations:
   ```bash
   cd dbt_project
   dbt run
   dbt test
   ```