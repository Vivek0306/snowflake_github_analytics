WITH repos AS (
    SELECT * FROM {{ ref('dim_repos') }}
),

commits AS (
    SELECT
        primary_language,
        COUNT(*)                                AS total_commits,
        COUNT(DISTINCT author_login)            AS unique_contributors
    FROM {{ ref('fct_commits') }}
    WHERE primary_language IS NOT NULL
    GROUP BY primary_language
),

prs AS (
    SELECT
        primary_language,
        COUNT(*)                                AS total_prs,
        AVG(hours_to_merge)                     AS avg_hours_to_merge
    FROM {{ ref('fct_pull_requests') }}
    WHERE primary_language IS NOT NULL
    GROUP BY primary_language
),

repo_agg AS (
    SELECT
        primary_language,
        COUNT(*)                                AS total_repos,
        SUM(stars)                              AS total_stars,
        SUM(forks)                              AS total_forks,
        AVG(stars)                              AS avg_stars_per_repo
    FROM repos
    WHERE primary_language IS NOT NULL
    GROUP BY primary_language
)

SELECT
    r.primary_language,
    r.total_repos,
    r.total_stars,
    r.total_forks,
    r.avg_stars_per_repo,
    COALESCE(c.total_commits, 0)                AS total_commits,
    COALESCE(c.unique_contributors, 0)          AS unique_contributors,
    COALESCE(p.total_prs, 0)                    AS total_prs,
    COALESCE(p.avg_hours_to_merge, 0)           AS avg_hours_to_merge
FROM repo_agg r
LEFT JOIN commits c
    ON r.primary_language = c.primary_language
LEFT JOIN prs p
    ON r.primary_language = p.primary_language
ORDER BY r.total_stars DESC