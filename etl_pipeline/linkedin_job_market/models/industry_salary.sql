SELECT 
    j_ind.industry_name,
    COUNT(DISTINCT j.job_id) AS total_jobs,
    AVG(js.med_salary) AS avg_median_salary,
    AVG(js.min_salary) AS avg_min_salary,
    AVG(js.max_salary) AS avg_max_salary
FROM {{ source('linkedin_job_market', 'dim_gold_job_industries') }} j_ind
JOIN {{ source('linkedin_job_market', 'dim_gold_job_salaries') }} js 
  ON j_ind.job_id = js.job_id
JOIN {{ source('linkedin_job_market', 'fact_gold_postings') }} j 
  ON j_ind.job_id = j.job_id
GROUP BY j_ind.industry_name
ORDER BY avg_median_salary DESC
