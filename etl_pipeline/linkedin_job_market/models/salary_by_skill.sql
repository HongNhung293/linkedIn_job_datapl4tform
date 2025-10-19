SELECT 
    j.skill_name,
    AVG(s.med_salary) AS avg_salary,
    MIN(s.min_salary) AS min_salary,
    MAX(s.max_salary) AS max_salary
FROM {{ source('linkedin_job_market', 'fact_gold_postings') }} f
JOIN {{ source('linkedin_job_market', 'dim_gold_job_skills') }} j
  ON f.job_id = j.job_id
JOIN {{ source('linkedin_job_market', 'dim_gold_job_salaries') }} s
  ON f.job_id = s.job_id
GROUP BY j.skill_name
ORDER BY avg_salary DESC
