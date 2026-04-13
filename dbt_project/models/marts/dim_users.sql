WITH commit_authors AS (
    SELECT DISTINCT
        author_login,
        author_name,
        author_email
    FROM {{ ref('stg_commits') }}
    WHERE author_login IS NOT NULL
),

commit_counts AS (
    SELECT
        author_login,
        COUNT(*)                            AS total_commits,
        COUNT(DISTINCT repo_id)             AS repos_contributed_to,
        MIN(committed_at)                   AS first_commit_at,
        MAX(committed_at)                   AS latest_commit_at
    FROM {{ ref('stg_commits') }}
    WHERE author_login IS NOT NULL
    GROUP BY author_login
)

SELECT
    a.author_login                          AS user_login,
    a.author_name                           AS user_name,
    a.author_email                          AS user_email,
    c.total_commits,
    c.repos_contributed_to,
    c.first_commit_at,
    c.latest_commit_at
FROM commit_authors a
LEFT JOIN commit_counts c
    ON a.author_login = c.author_login