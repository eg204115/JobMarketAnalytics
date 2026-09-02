CREATE TABLE IF NOT EXISTS gold.fact_job_postings (
    source_job_id     STRING NOT NULL,    
    source_name       STRING,
    company_key       BIGINT,
    location_key      BIGINT,
    date_key          BIGINT,              
    salary_min        DOUBLE,
    salary_max        DOUBLE,
    currency          STRING,
    is_remote         BOOLEAN,
    skill_count       INT,
    loaded_at         TIMESTAMP,
    contract_type     STRING              
)
USING DELTA
PARTITIONED BY (date_key);

