WITH prs AS (
    SELECT * FROM {{ ref('stg_pull_requests') }}
),

repos AS (
    SELECT repo_id, primary_language
    FROM {{ ref('dim_repos') }}
)

SELECT
    p.pr_id,
    p.pr_number,
    p.repo_id,
    p.repo_full_name,
    p.pr_title,
    p.pr_state,
    p.author_login,
    p.pr_created_at,
    p.pr_closed_at,
    p.pr_merged_at,
    p.comment_count,
    p.review_comment_count,
    p.commit_count,
    p.lines_added,
    p.lines_deleted,
    p.files_changed,
    r.primary_language,

    -- derived columns — these didn't exist in raw data
    CASE
        WHEN p.pr_merged_at IS NOT NULL THEN TRUE
        ELSE FALSE
    END                                         AS is_merged,

    CASE
        WHEN p.pr_merged_at IS NOT NULL
        THEN DATEDIFF('hour', p.pr_created_at, p.pr_merged_at)
        ELSE NULL
    END                                         AS hours_to_merge,

    CASE
        WHEN p.lines_added + p.lines_deleted < 50   THEN 'small'
        WHEN p.lines_added + p.lines_deleted < 250  THEN 'medium'
        ELSE 'large'
    END                                         AS pr_size,

    DATE_TRUNC('week', p.pr_created_at)         AS pr_week,
    DATE_TRUNC('month', p.pr_created_at)        AS pr_month

FROM prs p
LEFT JOIN repos r
    ON p.repo_id = r.repo_id