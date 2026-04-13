WITH commits AS (
    SELECT * FROM {{ ref('fct_commits') }}
),

prs AS (
    SELECT * FROM {{ ref('fct_pull_requests') }}
),

issues AS (
    SELECT * FROM {{ ref('fct_issues') }}
),

commit_agg AS (
    SELECT
        repo_id,
        repo_full_name,
        commit_week                             AS week,
        COUNT(*)                                AS total_commits,
        COUNT(DISTINCT author_login)            AS unique_authors
    FROM commits
    GROUP BY repo_id, repo_full_name, commit_week
),

pr_agg AS (
    SELECT
        repo_id,
        pr_week                                 AS week,
        COUNT(*)                                AS total_prs,
        SUM(CASE WHEN is_merged THEN 1 ELSE 0 END) AS merged_prs,
        AVG(hours_to_merge)                     AS avg_hours_to_merge
    FROM prs
    GROUP BY repo_id, pr_week
),

issue_agg AS (
    SELECT
        repo_id,
        issue_week                              AS week,
        COUNT(*)                                AS total_issues,
        SUM(CASE WHEN is_closed THEN 1 ELSE 0 END) AS closed_issues,
        AVG(hours_to_close)                     AS avg_hours_to_close
    FROM issues
    GROUP BY repo_id, issue_week
)

SELECT
    c.repo_id,
    c.repo_full_name,
    c.week,
    c.total_commits,
    c.unique_authors,
    COALESCE(p.total_prs, 0)                    AS total_prs,
    COALESCE(p.merged_prs, 0)                   AS merged_prs,
    COALESCE(p.avg_hours_to_merge, 0)           AS avg_hours_to_merge,
    COALESCE(i.total_issues, 0)                 AS total_issues,
    COALESCE(i.closed_issues, 0)                AS closed_issues,
    COALESCE(i.avg_hours_to_close, 0)           AS avg_hours_to_close
FROM commit_agg c
LEFT JOIN pr_agg p
    ON c.repo_id = p.repo_id AND c.week = p.week
LEFT JOIN issue_agg i
    ON c.repo_id = i.repo_id AND c.week = i.week