WITH repos AS (
    SELECT * FROM {{ ref('stg_repos') }}
),
contributor_counts AS (
    SELECT
        repo_id,
        COUNT(DISTINCT contributor_login)   AS total_contributors
    FROM {{ ref('stg_contributors') }}
    WHERE contributor_type = 'User'
    GROUP BY repo_id
)

SELECT
    r.repo_id,
    r.repo_full_name,
    r.owner_login,
    r.repo_name,
    r.description,
    r.primary_language,
    r.stars,
    r.forks,
    r.watchers,
    r.open_issues,
    r.topics,
    COALESCE(c.total_contributors, 0)       AS total_contributors,
    r.repo_created_at,
    r.repo_updated_at
FROM repos r
LEFT JOIN contributor_counts c
    ON r.repo_id = c.repo_id