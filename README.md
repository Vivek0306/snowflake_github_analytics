# GitHub Analytics Pipeline

An end-to-end ELT pipeline that ingests GitHub activity data, loads it into Snowflake, and transforms it into an analytics-ready data warehouse using dbt.

> **Data note:** Commit data is capped at 500 records per repository due to GitHub API pagination limits. Metrics derived from commits should be treated as samples of recent activity rather than complete historical counts. Repository metadata (stars, forks, watchers) and PR/issue data are fully accurate within the fetched range.

---

## Overview

This project tracks development activity across 13 popular open-source repositories and answers questions like:

- Which repos are most actively maintained?
- How long does it take for PRs to get merged?
- Who are the top contributors across the ecosystem?
- Which programming languages have the most community activity?
- How do issue resolution times compare across repos?

The pipeline follows the modern ELT pattern — raw data lands in Snowflake first, then dbt transforms it inside the warehouse into clean, tested, documented analytics tables.

---

## Architecture

```
GitHub REST API
      │
      │  Python + PySpark
      │  (repos, commits, PRs, issues, contributors)
      ▼
Local Parquet Files
      │
      │  Snowflake Internal Stage
      │  PUT + COPY INTO
      ▼
Snowflake RAW Schema
      │
      │  dbt Staging Layer
      │  (clean, rename, deduplicate)
      ▼
Snowflake STAGING Schema
      │
      │  dbt Marts Layer
      │  (facts, dimensions, aggregations)
      ▼
Snowflake ANALYTICS Schema
```

---

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python + PySpark |
| Intermediate Storage | Local Parquet |
| Warehouse | Snowflake |
| Bulk Loading | Snowflake Internal Stage + COPY INTO |
| Transformation | dbt Core |
| Testing | dbt tests (generic + singular) |
| Documentation | dbt docs |
| Source | GitHub REST API |

---

## Data Collected

| Entity | Rows | Notes |
|---|---|---|
| Repositories | 13 | Full metadata — stars, forks, language, topics |
| Commits | 5,960 | Most recent 500 per repo since Jan 2024 |
| Pull Requests | 6,000 | Most recent 500 per repo, open and closed |
| Issues | 1,847 | PRs filtered out from the issues endpoint |
| Contributors | 2,352 | Includes anonymous and bot contributors |

---

## Snowflake Schema Design

The warehouse is organized into three schemas that follow the standard layering pattern:

| Schema | Purpose |
|---|---|
| `RAW` | Raw tables loaded directly from the API. Never modified. Source of truth. |
| `STAGING` | dbt views. One per raw table. Cleaned column names, correct types, duplicates removed. |
| `ANALYTICS` | dbt tables. Facts, dimensions, and aggregations ready for analysis. |

---

## dbt Models

### Staging Layer
Thin views over raw tables. No business logic — just cleaning and typing.

| Model | Description |
|---|---|
| `stg_repos` | Cleaned repo metadata |
| `stg_commits` | Cleaned commit data |
| `stg_pull_requests` | Cleaned PR data with ROW_NUMBER() deduplication |
| `stg_issues` | Cleaned issue data, PRs filtered out |
| `stg_contributors` | Cleaned contributor data |

### Dimensions
Descriptive tables that provide context to facts. One row per entity.

| Model | Description |
|---|---|
| `dim_repos` | One row per repo — language, stars, forks, total contributors |
| `dim_users` | One row per contributor — total commits, repos contributed to, active period |

### Facts
Event tables. One row per event with derived metrics.

| Model | Description |
|---|---|
| `fct_commits` | One row per commit — author, repo, timestamp, language |
| `fct_pull_requests` | One row per PR — merge status, hours to merge, PR size (small/medium/large) |
| `fct_issues` | One row per issue — close status, hours to close, discussion level |

### Aggregations
Pre-computed summaries for analysis and dashboarding.

| Model | Description |
|---|---|
| `agg_repo_activity` | Per-repo rollup — commit counts, PR merge rate, issue closure rate, avg resolution times |
| `agg_language_trends` | Per-language rollup — total stars, forks, commits, contributors, avg merge time |
| `agg_contributor_stats` | Per-contributor per-repo — commits, PRs opened, PRs merged, issues opened, active days |

---

## Lineage Graph

The lineage graph below shows how data flows from raw sources through staging into the analytics layer. Every arrow represents a `ref()` dependency tracked by dbt.

<!-- Replace with your screenshot: dbt docs serve → open lineage graph → Expand All -->
![dbt Lineage Graph](images/lineage_full.png)

---

## Key Engineering Decisions

**PUT + COPY INTO instead of write_pandas()**
Data is loaded into Snowflake by staging local Parquet files in Snowflake's internal table stage (`@%tablename`) and bulk loading via `COPY INTO`. This is the production-standard Snowflake loading pattern — significantly faster than row-by-row inserts and avoids type-handling issues in the Python connector.

**Deduplication with ROW_NUMBER()**
The GitHub API returns duplicate records when a PR sits on a page boundary during pagination. Rather than relying on `DISTINCT` (which only works when every column is identical), staging models use `ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY ingested_at DESC)` to keep exactly one row per entity — the most recently ingested one.

**Spark schema declared explicitly, never inferred**
Every ingestion script declares a full `StructType` schema rather than letting Spark infer types from the data. This prevents silent type mismatches — for example, a column that is `None` in the first few rows being inferred as `StringType` when it should be `LongType`.

**ELT not ETL**
Raw data is loaded into Snowflake first, then transformed inside the warehouse using dbt. Transformation logic lives in version-controlled SQL files, not in the ingestion scripts. This makes transformations independently testable, auditable, and easy to change without touching the ingestion layer.

**Spark schema used for DDL, not pandas dtypes**
When generating `CREATE TABLE` DDL before loading, the script reads column types from the Spark DataFrame schema (`df_spark.schema.fields`) rather than pandas dtypes. Spark reads types directly from Parquet metadata — `TimestampType`, `LongType`, etc. — which map cleanly to Snowflake types without ambiguity.

---

## Tests

45 dbt tests cover the full model layer.

**Generic tests** declared in `schema.yml`:
- `not_null` and `unique` on all primary keys across staging and mart models
- `accepted_values` on categorical columns — `pr_state` (open/closed), `pr_size` (small/medium/large), `discussion_level` (no/light/heavy discussion)
- `relationships` tests verifying foreign key integrity between `fct_commits`, `fct_pull_requests`, `fct_issues` and `dim_repos`

**Singular tests** in `tests/`:
- `assert_pr_merged_after_created.sql` — asserts no PR has a `merged_at` timestamp earlier than its `created_at`. Returns zero rows if data is valid.

---

## Project Structure

```
github-analytics/
├── ingestion/
│   ├── ingest_repos.py              # fetch repo metadata
│   ├── ingest_commits.py            # fetch commits
│   ├── ingest_pulls.py      # fetch pull requests
│   ├── ingest_issues.py             # fetch issues (PRs filtered out)
│   ├── ingest_contributors.py       # fetch contributors
│   └── load_to_snowflake.py         # PUT + COPY INTO loader
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml          # raw table declarations
│   │   │   ├── schema.yml           # staging tests and descriptions
│   │   │   └── stg_*.sql
│   │   └── marts/
│   │       ├── schema.yml           # mart tests and descriptions
│   │       ├── dim_*.sql
│   │       ├── fct_*.sql
│   │       └── agg_*.sql
│   ├── tests/
│   │   └── assert_pr_merged_after_created.sql
│   └── macros/
│       └── generate_schema_name.sql # overrides dbt schema naming convention
├── data/                            # gitignored — local Parquet files
├── .env                             # gitignored — credentials
└── README.md
```

---

## Running the Pipeline

**1. Set up environment**

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file in the project root:

```
GITHUB_TOKEN=your_github_personal_access_token
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_DATABASE=github_analytics
SNOWFLAKE_WAREHOUSE=github_wh
```

**2. Run ingestion scripts**

Run in this order — `load_to_snowflake.py` depends on all five Parquet outputs:

```bash
python ingestion/ingest_repos.py
python ingestion/ingest_commits.py
python ingestion/ingest_pulls.py
python ingestion/ingest_issues.py
python ingestion/ingest_contributors.py
python ingestion/load_to_snowflake.py
```
or

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```


**3. Run dbt**

```bash
cd dbt_project
dbt run       # build all models
dbt test      # run all 45 tests
dbt docs generate && dbt docs serve   # open documentation site
```

---

## Repos Tracked

| Repo | Primary Language |
|---|---|
| apache/spark | Scala |
| apache/airflow | Python |
| dbt-labs/dbt-core | Python |
| pandas-dev/pandas | Python |
| tiangolo/fastapi | Python |
| django/django | Python |
| pytorch/pytorch | Python |
| tensorflow/tensorflow | C++ |
| microsoft/vscode | TypeScript |
| golang/go | Go |
| rust-lang/rust | Rust |
| vercel/next.js | JavaScript |
| snowflakedb/snowflake-connector-python | Python |

---

## Potential Enhancements

- **Airflow orchestration** — schedule the full pipeline as a DAG with proper task dependencies
- **Incremental dbt models** — process only new records on each run instead of full rebuilds
- **GitHub Archive as source** — replace the paginated REST API with GH Archive for complete historical data with no rate limits
- **dbt source freshness** — add freshness thresholds to alert when raw data goes stale
- **Dashboard** — connect Snowflake to Tableau or Metabase to visualize the aggregation tables