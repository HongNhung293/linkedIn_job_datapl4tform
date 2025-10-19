-- company
DROP TABLE IF EXISTS companies;
CREATE TABLE companies (
    company_id BIGINT PRIMARY KEY, 
    name VARCHAR(255),
    description TEXT,
    company_size INT,
    state VARCHAR(100),
    country VARCHAR(100),
    city VARCHAR(150),
    zip_code VARCHAR(20),
    address VARCHAR(255),
    url VARCHAR(500)
);

DROP TABLE IF EXISTS company_industries;
CREATE TABLE company_industries (
    company_id BIGINT,
    industry VARCHAR(255),
    PRIMARY KEY (company_id, industry),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

DROP TABLE IF EXISTS company_specialities;
CREATE TABLE company_specialities (
    company_id BIGINT,
    speciality VARCHAR(255),
    PRIMARY KEY (company_id, speciality),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

DROP TABLE IF EXISTS employee_counts;
CREATE TABLE employee_counts (
    company_id BIGINT,
    employee_count INT,
    follower_count BIGINT,
    time_recorded BIGINT, -- Unix timestamp
    PRIMARY KEY (company_id, time_recorded),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

-- postings
DROP TABLE IF EXISTS postings;
CREATE TABLE postings (
    job_id BIGINT PRIMARY KEY,
    company_name VARCHAR(255),
    title VARCHAR(255),
    description TEXT,
    max_salary DECIMAL(15,2),
    pay_period VARCHAR(10),
    location VARCHAR(255),
    company_id BIGINT,
    views BIGINT,
    med_salary DECIMAL(15,2),
    min_salary DECIMAL(15,2),
    formatted_work_type VARCHAR(50),
    applies INT,
    original_listed_time BIGINT,
    remote_allowed TINYINT(1),
    job_posting_url TEXT,
    application_url TEXT,
    application_type VARCHAR(100),
    expiry BIGINT,
    closed_time BIGINT,
    formatted_experience_level VARCHAR(50),
    skills_desc TEXT,
    listed_time BIGINT,
    posting_domain TEXT,
    sponsored TINYINT(1) DEFAULT 0,
    work_type VARCHAR(50),
    currency VARCHAR(10),
    compensation_type VARCHAR(50),
    normalized_salary DECIMAL(15,2),
    zip_code VARCHAR(20),
    fips VARCHAR(20),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

-- jobs
DROP TABLE IF EXISTS benefits;
CREATE TABLE benefits (
    job_id BIGINT PRIMARY KEY,
    inferred TINYINT(1) DEFAULT 0, -- 0/1, true/false
    benefit_type VARCHAR(255),
    FOREIGN KEY (job_id) REFERENCES postings(job_id)
);

-- mapping
DROP TABLE IF EXISTS industries;
CREATE TABLE industries (
    industry_id BIGINT PRIMARY KEY,
    industry_name TEXT
);

DROP TABLE IF EXISTS job_industries;
CREATE TABLE job_industries (
    job_id BIGINT,
    industry_id BIGINT,
    PRIMARY KEY (job_id, industry_id),
    FOREIGN KEY (job_id) REFERENCES postings(job_id),
    FOREIGN KEY (industry_id) REFERENCES industries(industry_id)
);

DROP TABLE IF EXISTS skills;
CREATE TABLE skills (
    skill_abr VARCHAR(50) PRIMARY KEY,
    skill_name TEXT
);

DROP TABLE IF EXISTS job_skills;
CREATE TABLE job_skills (
    job_id BIGINT,
    skill_abr VARCHAR(50),
    PRIMARY KEY (job_id, skill_abr),
    FOREIGN KEY (job_id) REFERENCES postings(job_id),
    FOREIGN KEY (skill_abr) REFERENCES skills(skill_abr)
);

DROP TABLE IF EXISTS salaries;
CREATE TABLE salaries (
    salary_id BIGINT,
    job_id BIGINT,
    max_salary DECIMAL(15,2),
    med_salary DECIMAL(15,2),
    min_salary DECIMAL(15,2),
    pay_period VARCHAR(10),
    currency VARCHAR(10), 
    compensation_type VARCHAR(50),
    PRIMARY KEY (salary_id, job_id),
    FOREIGN KEY (job_id) REFERENCES postings(job_id)
);
