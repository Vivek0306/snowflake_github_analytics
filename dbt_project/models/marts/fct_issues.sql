WITH issues AS (
    SELECT * FROM {{ ref('stg_issues') }}
),

repos AS (
    SELECT repo_id, primary_language
    FROM {{ ref('dim_repos') }}
)

SELECT
    i.issue_id,
    i.issue_number,
    i.repo_id,
    i.repo_full_name,
    i.issue_title,
    i.issue_state,
    i.author_login,
    i.issue_created_at,
    i.issue_closed_at,
    i.comment_count,
    i.labels,
    r.primary_language,

    -- derived columns
    CASE
        WHEN i.issue_closed_at IS NOT NULL THEN TRUE
        ELSE FALSE
    END                                         AS is_closed,

    CASE
        WHEN i.issue_closed_at IS NOT NULL
        THEN DATEDIFF('hour', i.issue_created_at, i.issue_closed_at)
        ELSE NULL
    END                                         AS hours_to_close,

    CASE
        WHEN i.comment_count = 0            THEN 'no discussion'
        WHEN i.comment_count < 5            THEN 'light discussion'
        ELSE 'heavy discussion'
    END                                         AS discussion_level,

    DATE_TRUNC('week', i.issue_created_at)      AS issue_week,
    DATE_TRUNC('month', i.issue_created_at)     AS issue_month

FROM issues i
LEFT JOIN repos r
    ON i.repo_id = r.repo_id