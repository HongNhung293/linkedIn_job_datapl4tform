SET foreign_key_checks = 0;

LOAD DATA LOCAL INFILE '/tmp/linkedin_23/companies/companies.csv'
INTO TABLE companies
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(company_id, name, description, company_size, state, country, city, zip_code, address, url);

LOAD DATA LOCAL INFILE '/tmp/linkedin_23/companies/company_industries.csv'
INTO TABLE company_industries
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(company_id, industry);

LOAD DATA LOCAL INFILE '/tmp/linkedin_23/companies/company_specialities.csv'
INTO TABLE company_specialities
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(company_id, speciality);

LOAD DATA LOCAL INFILE '/tmp/linkedin_23/companies/employee_counts.csv'
INTO TABLE employee_counts
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(company_id, employee_count, follower_count, time_recorded);

LOAD DATA LOCAL INFILE '/tmp/linkedin_23/jobs/benefits.csv'
INTO TABLE benefits
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(job_id, inferred, benefit_type);

LOAD DATA LOCAL INFILE '/tmp/linkedin_23/jobs/job_industries.csv'
INTO TABLE job_industries
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(job_id, industry_id);

LOAD DATA LOCAL INFILE '/tmp/linkedin_23/jobs/job_skills.csv'
INTO TABLE job_skills
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(job_id, skill_abr);

LOAD DATA LOCAL INFILE '/tmp/linkedin_23/jobs/salaries.csv'
INTO TABLE salaries
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(salary_id, job_id, max_salary, med_salary, min_salary, pay_period, currency, compensation_type);

LOAD DATA LOCAL INFILE '/tmp/linkedin_23/mappings/industries.csv'
INTO TABLE industries
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(industry_id, industry_name);


LOAD DATA LOCAL INFILE '/tmp/linkedin_23/mappings/skills.csv'
INTO TABLE skills
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(skill_abr, skill_name);

LOAD DATA LOCAL INFILE '/tmp/linkedin_23/postings.csv'
INTO TABLE postings
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(job_id, company_name, title, description, max_salary, pay_period, location, company_id,
 views, med_salary, min_salary, formatted_work_type, applies, original_listed_time, remote_allowed,
 job_posting_url, application_url, application_type, expiry, closed_time, formatted_experience_level,
 skills_desc, listed_time, posting_domain, sponsored, work_type, currency, compensation_type,
 normalized_salary, zip_code, fips);

SET foreign_key_checks = 1;