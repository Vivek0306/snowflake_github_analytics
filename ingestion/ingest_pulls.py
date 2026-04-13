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

OUTPUT_DIR = "data/raw/pull_requests"
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


def fetch_prs_for_repo(owner, repo, repo_id):
    print(f"  Fetching PRs: {owner}/{repo}")
    raw = paginate(
        f"https://api.github.com/repos/{owner}/{repo}/pulls",
        params={
            "state": "all",         # fetch both open and closed PRs
            "sort": "updated",      # most recently updated first
            "direction": "desc"
        }
    )

    rows = []
    for pr in raw:
        rows.append({
            "pr_id":           pr["id"],
            "pr_number":       pr["number"],
            "repo_id":         repo_id,
            "repo_full_name":  f"{owner}/{repo}",
            "title":           pr["title"][:300],
            "state":           pr["state"],
            "author_login":    pr["user"]["login"] if pr.get("user") else None,
            "created_at":      pr["created_at"],
            "updated_at":      pr["updated_at"],
            "closed_at":       pr.get("closed_at"),
            "merged_at":       pr.get("merged_at"),
            # These fields only come back on the list endpoint if PR is open
            # For closed PRs they may be 0 — that's fine for our analytics
            "comments":        pr.get("comments", 0),
            "review_comments": pr.get("review_comments", 0),
            "commits":         pr.get("commits", 0),
            "additions":       pr.get("additions", 0),
            "deletions":       pr.get("deletions", 0),
            "changed_files":   pr.get("changed_files", 0),
            "ingested_at":     datetime.now()
        })

    return rows


def main():
    spark = SparkSession.builder \
        .appName("github_ingest_pull_requests") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Load repo_ids from Part 1 output
    repos_df = spark.read.parquet("data/raw/repos")
    repo_lookup = {
        row["full_name"]: row["repo_id"]
        for row in repos_df.collect()
    }

    print(f"Loaded {len(repo_lookup)} repos.\n")

    # Fetch PRs
    all_rows = []
    for full_name in TARGET_REPOS:
        owner, repo = full_name.split("/")
        repo_id = repo_lookup.get(full_name)
        if not repo_id:
            print(f"  WARNING: {full_name} not in repos Parquet, skipping.")
            continue
        try:
            rows = fetch_prs_for_repo(owner, repo, repo_id)
            all_rows.extend(rows)
            print(f"  → {len(rows)} PRs collected\n")
        except Exception as e:
            print(f"  ERROR on {full_name}: {e}\n")

    print(f"Total PRs fetched: {len(all_rows)}")

    schema = StructType([
        StructField("pr_id",           LongType(),    nullable=False),
        StructField("pr_number",       LongType(),    nullable=False),
        StructField("repo_id",         LongType(),    nullable=False),
        StructField("repo_full_name",  StringType(),  nullable=False),
        StructField("title",           StringType(),  nullable=True),
        StructField("state",           StringType(),  nullable=True),
        StructField("author_login",    StringType(),  nullable=True),
        StructField("created_at",      StringType(),  nullable=True),
        StructField("updated_at",      StringType(),  nullable=True),
        StructField("closed_at",       StringType(),  nullable=True),
        StructField("merged_at",       StringType(),  nullable=True),
        StructField("comments",        IntegerType(), nullable=True),
        StructField("review_comments", IntegerType(), nullable=True),
        StructField("commits",         IntegerType(), nullable=True),
        StructField("additions",       IntegerType(), nullable=True),
        StructField("deletions",       IntegerType(), nullable=True),
        StructField("changed_files",   IntegerType(), nullable=True),
        StructField("ingested_at",     StringType(),  nullable=True),
    ])

    df = spark.createDataFrame(all_rows, schema=schema)

    df = df \
        .withColumn("created_at",  to_timestamp(col("created_at"))) \
        .withColumn("updated_at",  to_timestamp(col("updated_at"))) \
        .withColumn("closed_at",   to_timestamp(col("closed_at"))) \
        .withColumn("merged_at",   to_timestamp(col("merged_at"))) \
        .withColumn("ingested_at", to_timestamp(col("ingested_at")))

    print("\nSample PRs:")
    df.select("repo_full_name", "author_login", "state", "created_at", "merged_at") \
      .show(5, truncate=True)

    df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"\n✅ Pull requests written to {OUTPUT_DIR}")
    print(f"   Row count: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()