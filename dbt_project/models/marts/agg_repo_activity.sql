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
        COUNT(*)                                        AS total_commits,
        COUNT(DISTINCT author_login)                    AS unique_commit_authors,
        MIN(committed_at)                               AS first_commit_at,
        MAX(committed_at)                               AS last_commit_at
    FROM commits
    GROUP BY repo_id, repo_full_name
),

pr_agg AS (
    SELECT
        repo_id,
        total_prs,
        merged_prs,
        unmerged_prs,
        avg_hours_to_merge,
        avg_lines_changed_per_pr,
        CONCAT(
            ROUND(CAST(merged_prs AS FLOAT) / NULLIF(total_prs, 0) * 100, 1),
            '%'
        )                                               AS pr_merge_rate
    FROM (
        SELECT
            repo_id,
            COUNT(*)                                    AS total_prs,
            SUM(CASE WHEN is_merged THEN 1 ELSE 0 END) AS merged_prs,
            SUM(CASE WHEN NOT is_merged THEN 1 ELSE 0 END) AS unmerged_prs,
            ROUND(AVG(hours_to_merge), 1)               AS avg_hours_to_merge,
            ROUND(AVG(lines_added + lines_deleted), 0)  AS avg_lines_changed_per_pr
        FROM prs
        GROUP BY repo_id
    )
),

issue_agg AS (
    SELECT
        repo_id,
        total_issues,
        closed_issues,
        open_issues,
        avg_hours_to_close,
        CONCAT(
            ROUND(CAST(closed_issues AS FLOAT) / NULLIF(total_issues, 0) * 100, 1),
            '%'
        )                                               AS issue_closure_rate
    FROM (
        SELECT
            repo_id,
            COUNT(*)                                    AS total_issues,
            SUM(CASE WHEN is_closed THEN 1 ELSE 0 END) AS closed_issues,
            SUM(CASE WHEN NOT is_closed THEN 1 ELSE 0 END) AS open_issues,
            ROUND(AVG(hours_to_close), 1)               AS avg_hours_to_close
        FROM issues
        GROUP BY repo_id
    )
)

SELECT
    c.repo_id,
    c.repo_full_name,
    c.total_commits,
    c.unique_commit_authors,
    c.first_commit_at,
    c.last_commit_at,
    DATEDIFF('day', c.first_commit_at, c.last_commit_at) AS active_days,

    COALESCE(p.total_prs, 0)                            AS total_prs,
    COALESCE(p.merged_prs, 0)                           AS merged_prs,
    COALESCE(p.unmerged_prs, 0)                         AS unmerged_prs,
    COALESCE(p.avg_hours_to_merge, 0)                   AS avg_hours_to_merge,
    COALESCE(p.avg_lines_changed_per_pr, 0)             AS avg_lines_changed_per_pr,
    COALESCE(p.pr_merge_rate, '0%')                     AS pr_merge_rate,

    COALESCE(i.total_issues, 0)                         AS total_issues,
    COALESCE(i.closed_issues, 0)                        AS closed_issues,
    COALESCE(i.open_issues, 0)                          AS open_issues,
    COALESCE(i.avg_hours_to_close, 0)                   AS avg_hours_to_close,
    COALESCE(i.issue_closure_rate, '0%')                AS issue_closure_rate

FROM commit_agg c
LEFT JOIN pr_agg p ON c.repo_id = p.repo_id
LEFT JOIN issue_agg i ON c.repo_id = i.repo_id
ORDER BY c.total_commits DESC