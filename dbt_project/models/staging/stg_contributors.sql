WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_contributors') }}
),

renamed AS (
    SELECT
        REPO_ID                     AS repo_id,
        REPO_FULL_NAME              AS repo_full_name,
        CONTRIBUTOR_LOGIN           AS contributor_login,
        CONTRIBUTOR_NAME            AS contributor_name,
        CONTRIBUTIONS               AS contribution_count,
        CONTRIBUTOR_TYPE            AS contributor_type,
        INGESTED_AT                 AS ingested_at
    FROM source
)

SELECT * FROM renamed