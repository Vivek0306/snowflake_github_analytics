import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType
)
from pyspark.sql.functions import col, to_timestamp

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

TARGET_REPOS = [
    "apache/spark", "apache/airflow", "dbt-labs/dbt-core",
    "snowflakedb/snowflake-connector-python", "pandas-dev/pandas",
    "tiangolo/fastapi", "django/django", "pytorch/pytorch",
    "tensorflow/tensorflow", "microsoft/vscode",
    "golang/go", "rust-lang/rust", "vercel/next.js"
]

OUTPUT_DIR = "data/raw/issues"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def api_get(url, params=None):
    while True:
        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code == 403:
            reset_time = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            sleep_secs = max(reset_time - time.time(), 0) + 5
            print(f"  Rate limited. Sleeping {sleep_secs:.0f}s...")
            time.sleep(sleep_secs)
            continue
        resp.raise_for_status()
        return resp.json()


def paginate(url, params=None, max_pages=5):
    results = []
    params = params or {}
    params["per_page"] = 100

    for page in range(1, max_pages + 1):
        params["page"] = page
        data = api_get(url, params)

        if not data:
            break

        results.extend(data)
        print(f"    Page {page}: {len(data)} records")

        if len(data) < 100:
            break

        time.sleep(0.5)

    return results


def fetch_issues_for_repo(owner, repo, repo_id):
    print(f"  Fetching issues: {owner}/{repo}")
    raw = paginate(
        f"https://api.github.com/repos/{owner}/{repo}/issues",
        params={
            "state": "all",
            "sort": "updated",
            "direction": "desc"
        }
    )

    rows = []
    for issue in raw:
        # IMPORTANT: GitHub's issues endpoint returns PRs too
        # because PRs are a superset of issues internally.
        # The presence of the "pull_request" key is the filter.
        if "pull_request" in issue:
            continue

        rows.append({
            "issue_id":     issue["id"],
            "issue_number": issue["number"],
            "repo_id":      repo_id,
            "repo_full_name": f"{owner}/{repo}",
            "title":        issue["title"][:300],
            "state":        issue["state"],
            "author_login": issue["user"]["login"] if issue.get("user") else None,
            "created_at":   issue["created_at"],
            "updated_at":   issue["updated_at"],
            "closed_at":    issue.get("closed_at"),
            "comments":     issue.get("comments", 0),
            # Labels come as a list of dicts — flatten to comma-separated string
            # e.g. [{"name": "bug"}, {"name": "help wanted"}] → "bug,help wanted"
            # dbt will later split this into a proper labels dimension
            "labels":       ",".join([l["name"] for l in issue.get("labels", [])]),
            "ingested_at":  datetime.now()
        })

    return rows


def main():
    spark = SparkSession.builder \
        .appName("github_ingest_issues") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    repos_df = spark.read.parquet("data/raw/repos")
    repo_lookup = {
        row["full_name"]: row["repo_id"]
        for row in repos_df.collect()
    }

    print(f"Loaded {len(repo_lookup)} repos.\n")

    all_rows = []
    for full_name in TARGET_REPOS:
        owner, repo = full_name.split("/")
        repo_id = repo_lookup.get(full_name)
        if not repo_id:
            print(f"  WARNING: {full_name} not in repos Parquet, skipping.")
            continue
        try:
            rows = fetch_issues_for_repo(owner, repo, repo_id)
            all_rows.extend(rows)
            print(f"  → {len(rows)} issues collected (PRs filtered out)\n")
        except Exception as e:
            print(f"  ERROR on {full_name}: {e}\n")

    print(f"Total issues fetched: {len(all_rows)}")

    schema = StructType([
        StructField("issue_id",       LongType(),    nullable=False),
        StructField("issue_number",   LongType(),    nullable=False),
        StructField("repo_id",        LongType(),    nullable=False),
        StructField("repo_full_name", StringType(),  nullable=False),
        StructField("title",          StringType(),  nullable=True),
        StructField("state",          StringType(),  nullable=True),
        StructField("author_login",   StringType(),  nullable=True),
        StructField("created_at",     StringType(),  nullable=True),
        StructField("updated_at",     StringType(),  nullable=True),
        StructField("closed_at",      StringType(),  nullable=True),
        StructField("comments",       IntegerType(), nullable=True),
        StructField("labels",         StringType(),  nullable=True),
        StructField("ingested_at",    StringType(),  nullable=True),
    ])

    df = spark.createDataFrame(all_rows, schema=schema)

    df = df \
        .withColumn("created_at",  to_timestamp(col("created_at"))) \
        .withColumn("updated_at",  to_timestamp(col("updated_at"))) \
        .withColumn("closed_at",   to_timestamp(col("closed_at"))) \
        .withColumn("ingested_at", to_timestamp(col("ingested_at")))

    print("\nSample issues:")
    df.select("repo_full_name", "author_login", "state", "labels", "created_at") \
      .show(5, truncate=True)

    df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"\n✅ Issues written to {OUTPUT_DIR}")
    print(f"   Row count: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()