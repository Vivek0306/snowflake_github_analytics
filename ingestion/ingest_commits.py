import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType
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

OUTPUT_DIR = "data/raw/commits"
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
    """
    Fetch multiple pages from a GitHub endpoint.
    Each page has up to 100 records (GitHub's max).
    max_pages=5 means up to 500 commits per repo.
    """
    results = []
    params = params or {}
    params["per_page"] = 100

    for page in range(1, max_pages + 1):
        params["page"] = page
        data = api_get(url, params)

        if not data:
            # empty page means we've exhausted all records
            break

        results.extend(data)
        print(f"    Page {page}: {len(data)} records")

        # stop early if this page wasn't full — no more pages after this
        if len(data) < 100:
            break

        time.sleep(0.5)

    return results


def fetch_commits_for_repo(owner, repo, repo_id):
    """Fetch commits for one repo and return as list of dicts."""
    print(f"  Fetching commits: {owner}/{repo}")
    raw = paginate(
        f"https://api.github.com/repos/{owner}/{repo}/commits",
        params={"since": "2024-01-01T00:00:00Z"}
    )

    rows = []
    for c in raw:
        # commit.author has name/email/date (git metadata)
        # c["author"] has login (GitHub user — can be None if unlinked account)
        commit = c.get("commit", {})
        git_author = commit.get("author") or {}
        github_user = c.get("author") or {}

        rows.append({
            "sha":            c["sha"],
            "repo_id":        repo_id,
            "repo_full_name": f"{owner}/{repo}",
            "author_login":   github_user.get("login"),   # GitHub username
            "author_name":    git_author.get("name"),     # git config name
            "author_email":   git_author.get("email"),    # git config email
            "message":        commit.get("message", "")[:500],
            "committed_at":   git_author.get("date"),
            "ingested_at":    datetime.now()
        })

    return rows


def main():
    # --- 1. Read repo_ids from the repos Parquet we already wrote ---
    spark = SparkSession.builder \
        .appName("github_ingest_commits") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    repos_df = spark.read.parquet("data/raw/repos")
    # collect() brings the small repos table into Python as a list of Row objects
    repo_lookup = {
        row["full_name"]: row["repo_id"]
        for row in repos_df.collect()
    }

    print(f"Loaded {len(repo_lookup)} repos from Part 1 output.\n")

    # --- 2. Fetch commits via GitHub API ---
    all_rows = []
    for full_name in TARGET_REPOS:
        owner, repo = full_name.split("/")
        repo_id = repo_lookup.get(full_name)
        if not repo_id:
            print(f"  WARNING: {full_name} not found in repos Parquet, skipping.")
            continue
        try:
            rows = fetch_commits_for_repo(owner, repo, repo_id)
            all_rows.extend(rows)
            print(f"  → {len(rows)} commits collected\n")
        except Exception as e:
            print(f"  ERROR on {full_name}: {e}\n")

    print(f"Total commits fetched: {len(all_rows)}")

    # --- 3. Define schema ---
    schema = StructType([
        StructField("sha",            StringType(), nullable=False),
        StructField("repo_id",        LongType(),   nullable=False),
        StructField("repo_full_name", StringType(), nullable=False),
        StructField("author_login",   StringType(), nullable=True),  # nullable: unlinked accounts
        StructField("author_name",    StringType(), nullable=True),
        StructField("author_email",   StringType(), nullable=True),
        StructField("message",        StringType(), nullable=True),
        StructField("committed_at",   StringType(), nullable=True),
        StructField("ingested_at",    StringType(), nullable=True),
    ])

    # --- 4. Create DataFrame and cast timestamps ---
    df = spark.createDataFrame(all_rows, schema=schema)
    df = df \
        .withColumn("committed_at", to_timestamp(col("committed_at"))) \
        .withColumn("ingested_at",  to_timestamp(col("ingested_at")))

    # --- 5. Preview ---
    print("\nSample commits:")
    df.select("repo_full_name", "author_login", "committed_at", "message") \
      .show(5, truncate=True)

    # --- 6. Write Parquet ---
    df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"\n✅ Commits written to {OUTPUT_DIR}")
    print(f"   Row count: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()