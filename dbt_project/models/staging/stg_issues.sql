WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_issues') }}
),

renamed AS (
    SELECT
        ISSUE_ID                    AS issue_id,
        ISSUE_NUMBER                AS issue_number,
        REPO_ID                     AS repo_id,
        REPO_FULL_NAME              AS repo_full_name,
        TITLE                       AS issue_title,
        STATE                       AS issue_state,
        AUTHOR_LOGIN                AS author_login,
        CREATED_AT                  AS issue_created_at,
        UPDATED_AT                  AS issue_updated_at,
        CLOSED_AT                   AS issue_closed_at,
        COMMENTS                    AS comment_count,
        LABELS                      AS labels,
        INGESTED_AT                 AS ingested_at
    FROM source
)

SELECT * FROM renamed