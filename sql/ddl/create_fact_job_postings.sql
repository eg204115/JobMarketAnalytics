CREATE TABLE IF NOT EXISTS gold.fact_job_postings (
    posting_key       BIGINT GENERATED ALWAYS AS IDENTITY,
    source_job_id     STRING NOT NULL,     -- natural key, used for MERGE matching
    source_name       STRING,
    company_key       BIGINT,
    location_key      BIGINT,
    date_key          INT,
    salary_min        DOUBLE,
    salary_max        DOUBLE,
    currency          STRING,
    is_remote         BOOLEAN,
    skill_count       INT,
    loaded_at         TIMESTAMP
)
USING DELTA
PARTITIONED BY (date_key);