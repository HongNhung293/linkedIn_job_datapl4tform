-- Top companies hiring
-- Theo city (fact_gold_postings)
SELECT 
    company_name,
    COUNT(*) AS num_postings
FROM warehouse.fact_gold_postings
GROUP BY company_name
ORDER BY num_postings DESC
LIMIT 20;


-- Salary distribution
-- Theo job title
SELECT 
    f.title,
    AVG(s.med_salary) AS avg_salary,
    MIN(s.min_salary) AS min_salary,
    MAX(s.max_salary) AS max_salary
FROM warehouse.fact_gold_postings f
JOIN warehouse.dim_gold_job_salaries s
  ON f.job_id = s.job_id
GROUP BY f.title
ORDER BY avg_salary DESC;

-- Theo skill
SELECT 
    j.skill_name,
    AVG(s.med_salary) AS avg_salary,
    MIN(s.min_salary) AS min_salary,
    MAX(s.max_salary) AS max_salary
FROM warehouse.fact_gold_postings f
JOIN warehouse.dim_gold_job_skills j
  ON f.job_id = j.job_id
JOIN warehouse.dim_gold_job_salaries s
  ON f.job_id = s.job_id
GROUP BY j.skill_name
ORDER BY avg_salary DESC;


-- Theo location
SELECT 
    f.location,
    AVG(s.med_salary) AS avg_salary,
    MIN(s.min_salary) AS min_salary,
    MAX(s.max_salary) AS max_salary
FROM warehouse.fact_gold_postings f
JOIN warehouse.dim_gold_job_salaries s
  ON f.job_id = s.job_id
GROUP BY f.location
ORDER BY avg_salary DESC;


-- Skills demand: kỹ năng được yêu cầu nhiều nhất
SELECT 
    skill_name,
    COUNT(*) AS demand_count
FROM warehouse.dim_gold_job_skills
GROUP BY skill_name
ORDER BY demand_count DESC
LIMIT 20;



-- Mối quan hệ giữa ngành nghề và mức lương
SELECT 
    j_ind.industry_name,
    COUNT(DISTINCT j.job_id) AS total_jobs,
    AVG(js.med_salary) AS avg_median_salary,
    AVG(js.min_salary) AS avg_min_salary,
    AVG(js.max_salary) AS avg_max_salary
FROM warehouse.dim_gold_job_industries j_ind
JOIN warehouse.dim_gold_job_salaries js ON j_ind.job_id = js.job_id
JOIN warehouse.fact_gold_postings j ON j_ind.job_id = j.job_id
GROUP BY j_ind.industry_name
ORDER BY avg_median_salary DESC
LIMIT 10;


-- Xu hướng tuyển dụng theo địa lý
SELECT 
    f."location",
    COUNT(DISTINCT f.job_id) AS total_jobs,
    AVG(js.med_salary) AS avg_salary
FROM warehouse.fact_gold_postings f
LEFT JOIN warehouse.dim_gold_job_salaries js ON f.job_id = js.job_id
GROUP BY f.location
ORDER BY total_jobs DESC
LIMIT 10;


