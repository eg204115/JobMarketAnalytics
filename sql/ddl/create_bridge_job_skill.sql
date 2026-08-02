-- Bridge table resolving the many-to-many relationship between
-- fact_job_postings and dim_skill. Composite key, no surrogate key needed.
CREATE TABLE IF NOT EXISTS gold.bridge_job_skill (
    posting_key   BIGINT NOT NULL,   -- FK -> gold.fact_job_postings.posting_key
    skill_key     BIGINT NOT NULL    -- FK -> gold.dim_skill.skill_key
)
USING DELTA;