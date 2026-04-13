WITH commits AS (
    SELECT * FROM {{ ref('stg_commits') }}
),

repos AS (
    SELECT repo_id, repo_full_name, primary_language
    FROM {{ ref('dim_repos') }}
)

SELECT
    c.commit_id,
    c.repo_id,
    c.repo_full_name,
    c.author_login,
    c.author_name,
    c.commit_message,
    c.committed_at,
    DATE_TRUNC('week', c.committed_at)      AS commit_week,
    DATE_TRUNC('month', c.committed_at)     AS commit_month,
    r.primary_language,
    c.ingested_at
FROM commits c
LEFT JOIN repos r
    ON c.repo_id = r.repo_id