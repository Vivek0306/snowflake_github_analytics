WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_repos') }}
),
renamed AS (
    SELECT
        REPO_ID                                 AS repo_id,
        FULL_NAME                               AS repo_full_name,
        OWNER_LOGIN                             AS owner_login,
        NAME                                    AS repo_name,
        DESCRIPTION                             AS description,
        LANGUAGE                                AS primary_language,
        STARGAZERS_COUNT                        AS stars,
        FORKS_COUNT                             AS forks,
        WATCHERS_COUNT                          AS watchers,
        OPEN_ISSUES_COUNT                       AS open_issues,
        TOPICS                                  AS topics,
        CREATED_AT                              AS repo_created_at,
        UPDATED_AT                              AS repo_updated_at,
        INGESTED_AT                             AS ingested_at
    FROM source
)

SELECT * FROM renamed