SELECT
    pr_id,  
FROM {{ ref('fct_pull_requests') }}
WHERE pr_merged_at IS NOT NULL
  AND pr_merged_at < pr_created_at