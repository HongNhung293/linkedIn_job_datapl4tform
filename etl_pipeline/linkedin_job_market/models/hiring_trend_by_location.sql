
SELECT 
    f.location,
    COUNT(DISTINCT f.job_id) AS total_jobs,
    AVG(js.med_salary) AS avg_salary
FROM {{ source('linkedin_job_market', 'fact_gold_postings') }} f
LEFT JOIN {{ source('linkedin_job_market', 'dim_gold_job_salaries') }} js 
  ON f.job_id = js.job_id
GROUP BY f.location
ORDER BY total_jobs DESC
