SELECT 
    company_name,
    COUNT(*) AS num_postings
FROM {{ source('linkedin_job_market', 'fact_gold_postings') }}
GROUP BY company_name
ORDER BY num_postings DESC
LIMIT 20
