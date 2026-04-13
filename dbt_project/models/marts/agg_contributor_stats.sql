WITH commits AS (
    SELECT * FROM {{ ref('fct_commits') }}
),

prs AS (
    SELECT * FROM {{ ref('fct_pull_requests') }}
),

issues AS (
    SELECT * FROM {{ ref('fct_issues') }}
),

commit_stats AS (
    SELECT
        author_login,
        repo_id,
        repo_full_name,
        COUNT(*)                                AS total_commits,
        MIN(committed_at)                       AS first_commit_at,
        MAX(committed_at)                       AS latest_commit_at
    FROM commits
    WHERE author_login IS NOT NULL
    GROUP BY author_login, repo_id, repo_full_name
),

pr_stats AS (
    SELECT
        author_login,
        repo_id,
        COUNT(*)                                AS total_prs,
        SUM(CASE WHEN is_merged THEN 1 ELSE 0 END) AS merged_prs
    FROM prs
    WHERE author_login IS NOT NULL
    GROUP BY author_login, repo_id
),

issue_stats AS (
    SELECT
        author_login,
        repo_id,
        COUNT(*)                                AS total_issues_opened
    FROM issues
    WHERE author_login IS NOT NULL
    GROUP BY author_login, repo_id
)

SELECT
    c.author_login,
    c.repo_id,
    c.repo_full_name,
    c.total_commits,
    c.first_commit_at,
    c.latest_commit_at,
    DATEDIFF('day', c.first_commit_at, c.latest_commit_at) AS active_days,
    COALESCE(p.total_prs, 0)                    AS total_prs,
    COALESCE(p.merged_prs, 0)                   AS merged_prs,
    COALESCE(i.total_issues_opened, 0)          AS total_issues_opened
FROM commit_stats c
LEFT JOIN pr_stats p
    ON c.author_login = p.author_login AND c.repo_id = p.repo_id
LEFT JOIN issue_stats i
    ON c.author_login = i.author_login AND c.repo_id = i.repo_id
ORDER BY c.total_commits DESC