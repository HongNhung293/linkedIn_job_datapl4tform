SELECT 
    skill_name,
    COUNT(*) AS demand_count
FROM {{ source('linkedin_job_market', 'dim_gold_job_skills') }}
GROUP BY skill_name
ORDER BY demand_count DESC
