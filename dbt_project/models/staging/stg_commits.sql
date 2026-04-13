WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_commits') }}
),

renamed AS (
    SELECT
        SHA                         AS commit_id,
        REPO_ID                     AS repo_id,
        REPO_FULL_NAME              AS repo_full_name,
        AUTHOR_LOGIN                AS author_login,
        AUTHOR_NAME                 AS author_name,
        AUTHOR_EMAIL                AS author_email,
        MESSAGE                     AS commit_message,
        COMMITTED_AT                AS committed_at,
        INGESTED_AT                 AS ingested_at
    FROM source
)

SELECT * FROM renamed