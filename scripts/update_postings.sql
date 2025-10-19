-- 1. Tạo bảng tạm (cấu trúc giống postings)
CREATE TABLE postings_tmp LIKE postings;

-- 2. Nạp dữ liệu mới vào bảng tạm
LOAD DATA LOCAL INFILE '/tmp/postings_repair.csv'
INTO TABLE postings_tmp
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(job_id, company_name, title, description, max_salary, pay_period, location, company_id,
 views, med_salary, min_salary, formatted_work_type, applies, original_listed_time, remote_allowed,
 job_posting_url, application_url, application_type, expiry, closed_time, formatted_experience_level,
 skills_desc, listed_time, posting_domain, sponsored, work_type, currency, compensation_type,
 normalized_salary, zip_code, fips);

-- 3. Update dữ liệu trong bảng postings dựa vào job_id
UPDATE postings p
JOIN postings_tmp t ON p.job_id = t.job_id
SET
    p.company_name = t.company_name,
    p.title = t.title,
    p.description = t.description,
    p.max_salary = t.max_salary,
    p.pay_period = t.pay_period,
    p.location = t.location,
    p.company_id = t.company_id,
    p.views = t.views,
    p.med_salary = t.med_salary,
    p.min_salary = t.min_salary,
    p.formatted_work_type = t.formatted_work_type,
    p.applies = t.applies,
    p.original_listed_time = t.original_listed_time,
    p.remote_allowed = t.remote_allowed,
    p.job_posting_url = t.job_posting_url,
    p.application_url = t.application_url,
    p.application_type = t.application_type,
    p.expiry = t.expiry,
    p.closed_time = t.closed_time,
    p.formatted_experience_level = t.formatted_experience_level,
    p.skills_desc = t.skills_desc,
    p.listed_time = t.listed_time,
    p.posting_domain = t.posting_domain,
    p.sponsored = t.sponsored,
    p.work_type = t.work_type,
    p.currency = t.currency,
    p.compensation_type = t.compensation_type,
    p.normalized_salary = t.normalized_salary,
    p.zip_code = t.zip_code,
    p.fips = t.fips;

-- 4. Xóa bảng tạm nếu không cần nữa
DROP TABLE postings_tmp;
