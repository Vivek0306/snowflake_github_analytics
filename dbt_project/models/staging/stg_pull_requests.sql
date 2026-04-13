WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_pull_requests') }}
),

renamed AS (
    SELECT
        PR_ID                       AS pr_id,
        PR_NUMBER                   AS pr_number,
        REPO_ID                     AS repo_id,
        REPO_FULL_NAME              AS repo_full_name,
        TITLE                       AS pr_title,
        STATE                       AS pr_state,
        AUTHOR_LOGIN                AS author_login,
        CREATED_AT                  AS pr_created_at,
        UPDATED_AT                  AS pr_updated_at,
        CLOSED_AT                   AS pr_closed_at,
        MERGED_AT                   AS pr_merged_at,
        COMMENTS                    AS comment_count,
        REVIEW_COMMENTS             AS review_comment_count,
        COMMITS                     AS commit_count,
        ADDITIONS                   AS lines_added,
        DELETIONS                   AS lines_deleted,
        CHANGED_FILES               AS files_changed,
        INGESTED_AT                 AS ingested_at
    FROM source
)

SELECT * FROM renamed