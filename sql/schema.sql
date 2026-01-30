
-- Drop tables if they exist (clean setup)
DROP TABLE IF EXISTS staging_jobs CASCADE;
DROP TABLE IF EXISTS job_skills CASCADE;
DROP TABLE IF EXISTS jobs CASCADE;
DROP TABLE IF EXISTS skills CASCADE;
DROP TABLE IF EXISTS skill_categories CASCADE;
DROP TABLE IF EXISTS companies CASCADE;
DROP TABLE IF EXISTS locations CASCADE;
DROP TABLE IF EXISTS platforms CASCADE;
DROP TABLE IF EXISTS schedule_types CASCADE;



--------------------
-- STAGING TABLES --
--------------------

CREATE TABLE staging_jobs (
    id SERIAL PRIMARY KEY,
    job_title_short VARCHAR(255),
    job_title TEXT,
    job_location TEXT,
    job_via TEXT,
    job_schedule_type VARCHAR(100),
    job_work_from_home BOOLEAN,
    search_location TEXT,
    job_posted_date TIMESTAMP,
    job_no_degree_mention BOOLEAN,
    job_health_insurance BOOLEAN,
    job_country VARCHAR(100),
    salary_rate VARCHAR(20),
    salary_year_avg NUMERIC(10, 2),
    salary_hour_avg NUMERIC(10, 2),
    company_name TEXT,
    job_skills TEXT[],
    job_type_skills JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_staging_company ON staging_jobs(company_name);
CREATE INDEX idx_staging_country ON staging_jobs(job_country);
CREATE INDEX idx_staging_title_short ON staging_jobs(job_title_short);


----------------------
-- DIMENSION TABLES --
----------------------

CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    company_name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_companies_name ON companies(company_name);

CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    main_city VARCHAR(100),
    main_state_province VARCHAR(100),
    country VARCHAR(100) NOT NULL,
    full_location TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(main_city, main_state_province, country, full_location)
);
CREATE INDEX idx_locations_country ON locations(country);

CREATE TABLE platforms (
    platform_id SERIAL PRIMARY KEY,
    platform_name VARCHAR(200) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE schedule_types (
    schedule_type_id SERIAL PRIMARY KEY,
    schedule_type_name VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE skill_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE skills (
    skill_id SERIAL PRIMARY KEY,
    skill_name VARCHAR(100) NOT NULL UNIQUE,
    category_id INTEGER REFERENCES skill_categories(category_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_skills_name ON skills(skill_name);
CREATE INDEX idx_skills_category ON skills(category_id);


-----------------
-- FACT TABLES --
-----------------

CREATE TABLE jobs (
    job_id SERIAL PRIMARY KEY,
    job_title VARCHAR(500) NOT NULL,
    job_title_short VARCHAR(100),
    company_id INTEGER NOT NULL REFERENCES companies(company_id),
    location_id INTEGER NOT NULL REFERENCES locations(location_id),
    platform_id INTEGER REFERENCES platforms(platform_id),
    schedule_type_id INTEGER REFERENCES schedule_types(schedule_type_id),
    job_work_from_home BOOLEAN DEFAULT FALSE,
    job_posted_date TIMESTAMP,
    job_no_degree_mention BOOLEAN DEFAULT FALSE,
    job_health_insurance BOOLEAN DEFAULT FALSE,
    salary_rate VARCHAR(20),
    salary_year_avg NUMERIC(10, 2),
    salary_hour_avg NUMERIC(10, 2),
    search_location TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_salary_rate CHECK (salary_rate IN ('hour', 'day', 'week', 'month', 'year') OR salary_rate IS NULL)
);
CREATE INDEX idx_jobs_company ON jobs(company_id);
CREATE INDEX idx_jobs_location ON jobs(location_id);
CREATE INDEX idx_jobs_posted_date ON jobs(job_posted_date);
CREATE INDEX idx_jobs_title_short ON jobs(job_title_short);
CREATE INDEX idx_jobs_remote ON jobs(job_work_from_home);


----------------------------
-- JUNCTION/BRIDGE TABLES --
----------------------------

CREATE TABLE job_skills (
    job_id INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    skill_id INTEGER NOT NULL REFERENCES skills(skill_id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, skill_id)
);
CREATE INDEX idx_job_skills_job ON job_skills(job_id);
CREATE INDEX idx_job_skills_skill ON job_skills(skill_id);
