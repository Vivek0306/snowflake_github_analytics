import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType
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

OUTPUT_DIR = "data/raw/repos"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def api_get(url):
    """Single API call with rate limit handling."""
    while True:
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code == 403:
            reset_time = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            sleep_secs = max(reset_time - time.time(), 0) + 5
            print(f"  Rate limited. Sleeping {sleep_secs:.0f}s...")
            time.sleep(sleep_secs)
            continue
        resp.raise_for_status()
        return resp.json()


def fetch_repo(owner, repo):
    """Fetch metadata for a single repo and return as a dict."""
    print(f"  Fetching: {owner}/{repo}")
    data = api_get(f"https://api.github.com/repos/{owner}/{repo}")
    return {
        "repo_id": data["id"],
        "full_name": data["full_name"],
        "owner_login": data["owner"]["login"],
        "name": data["name"],
        "description": data.get("description"),
        "language": data.get("language"),
        "stargazers_count": data["stargazers_count"],
        "forks_count": data["forks_count"],
        "watchers_count": data["watchers_count"],
        "open_issues_count": data["open_issues_count"],
        "created_at": data["created_at"],
        "updated_at": data["updated_at"],
        "topics": ",".join(data.get("topics", [])),
        "ingested_at": datetime.now()
    }


def main():
    # --- 1. Collect raw data via GitHub API (plain Python, no Spark yet) ---
    print("Fetching repo metadata from GitHub API...")
    rows = []
    for full_name in TARGET_REPOS:
        owner, repo = full_name.split("/")
        try:
            rows.append(fetch_repo(owner, repo))
            time.sleep(0.3)
        except Exception as e:
            print(f"  ERROR on {full_name}: {e}")

    print(f"\nFetched {len(rows)} repos. Creating Spark DataFrame...")

    # --- 2. Define schema explicitly ---
    # Never let Spark infer schema from API data — types can be wrong
    schema = StructType([
        StructField("repo_id",          LongType(),   nullable=False),
        StructField("full_name",        StringType(), nullable=False),
        StructField("owner_login",      StringType(), nullable=True),
        StructField("name",             StringType(), nullable=True),
        StructField("description",      StringType(), nullable=True),
        StructField("language",         StringType(), nullable=True),
        StructField("stargazers_count", LongType(),   nullable=True),
        StructField("forks_count",      LongType(),   nullable=True),
        StructField("watchers_count",   LongType(),   nullable=True),
        StructField("open_issues_count",LongType(),   nullable=True),
        StructField("created_at",       StringType(), nullable=True),
        StructField("updated_at",       StringType(), nullable=True),
        StructField("topics",           StringType(), nullable=True),
        StructField("ingested_at",      StringType(), nullable=True),
    ])

    # --- 3. Build SparkSession ---
    spark = SparkSession.builder \
        .appName("github_ingest_repos") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # --- 4. Create DataFrame from collected rows ---
    df = spark.createDataFrame(rows, schema=schema)

    # --- 5. Cast string timestamps to proper timestamp columns ---
    # GitHub API returns ISO8601 strings like "2024-01-15T10:30:00Z"
    # to_timestamp() parses them into Spark TimestampType
    df = df \
        .withColumn("created_at",   to_timestamp(col("created_at"))) \
        .withColumn("updated_at",   to_timestamp(col("updated_at"))) \
        .withColumn("ingested_at",  to_timestamp(col("ingested_at")))

    # --- 6. Show a preview ---
    print("\nSample data:")
    df.select("full_name", "language", "stargazers_count", "forks_count").show(truncate=False)

    # --- 7. Write as Parquet ---
    # overwrite mode is fine here — repos metadata is small and fully re-fetched each run
    df.write.mode("overwrite").parquet(OUTPUT_DIR)
    print(f"\n✅ Repos written to {OUTPUT_DIR}")
    print(f"   Row count: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()