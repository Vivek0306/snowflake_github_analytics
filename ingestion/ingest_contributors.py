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

OUTPUT_DIR = "data/raw/contributors"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def api_get(url, params=None):
    while True:
        resp = requests.get(url, headers=HEADERS, params=params)

        # GitHub returns 202 for contributor stats when it's
        # computing them in the background — wait and retry
        if resp.status_code == 202:
            print(f"  GitHub is computing stats, retrying in 3s...")
            time.sleep(3)
            continue

        if resp.status_code == 403:
            reset_time = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            sleep_secs = max(reset_time - time.time(), 0) + 5
            print(f"  Rate limited. Sleeping {sleep_secs:.0f}s...")
            time.sleep(sleep_secs)
            continue

        resp.raise_for_status()
        return resp.json()


def paginate(url, params=None, max_pages=2):
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


def fetch_contributors_for_repo(owner, repo, repo_id):
    print(f"  Fetching contributors: {owner}/{repo}")
    raw = paginate(
        f"https://api.github.com/repos/{owner}/{repo}/contributors",
        # anon=1 includes contributors whose commits have emails
        # not linked to any GitHub account
        params={"anon": "1"}
    )

    rows = []
    for c in raw:
        # Anonymous contributors have type="Anonymous" and no login
        # They only have a name field instead
        is_anon = c.get("type") == "Anonymous"

        rows.append({
            "repo_id":            repo_id,
            "repo_full_name":     f"{owner}/{repo}",
            "contributor_login":  None if is_anon else c.get("login"),
            "contributor_name":   c.get("name") if is_anon else None,
            "contributions":      c.get("contributions", 0),
            "contributor_type":   c.get("type", "User"),  # "User" or "Anonymous" or "Bot"
            "ingested_at":        datetime.utcnow().isoformat()
        })

    return rows


def main():
    spark = SparkSession.builder \
        .appName("github_ingest_contributors") \
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
            rows = fetch_contributors_for_repo(owner, repo, repo_id)
            all_rows.extend(rows)
            print(f"  → {len(rows)} contributors collected\n")
        except Exception as e:
            print(f"  ERROR on {full_name}: {e}\n")

    print(f"Total contributors fetched: {len(all_rows)}")

    schema = StructType([
        StructField("repo_id",            LongType(),    nullable=False),
        StructField("repo_full_name",     StringType(),  nullable=False),
        StructField("contributor_login",  StringType(),  nullable=True),  # null for anon
        StructField("contributor_name",   StringType(),  nullable=True),  # only for anon
        StructField("contributions",      IntegerType(), nullable=True),
        StructField("contributor_type",   StringType(),  nullable=True),
        StructField("ingested_at",        StringType(),  nullable=True),
    ])

    df = spark.createDataFrame(all_rows, schema=schema)

    df = df.withColumn("ingested_at", to_timestamp(col("ingested_at")))

    print("\nSample contributors:")
    df.select("repo_full_name", "contributor_login", "contributions", "contributor_type") \
      .show(5, truncate=True)

    # breakdown by type — useful to see how many bots are in the dataset
    print("\nContributor type breakdown:")
    df.groupBy("contributor_type").count().show()

    df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"\n✅ Contributors written to {OUTPUT_DIR}")
    print(f"   Row count: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()