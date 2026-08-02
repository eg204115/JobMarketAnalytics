-- SCD Type 2: multiple rows per company_natural_key over time, exactly one
-- with is_current = true. effective_end_date = NULL for the current version.
CREATE TABLE IF NOT EXISTS gold.dim_company (
    company_key            BIGINT GENERATED ALWAYS AS IDENTITY,
    company_natural_key    STRING NOT NULL,   -- normalized company name
    company_name           STRING,
    size_bucket             STRING,            -- derived: small/medium/large by posting volume
    effective_start_date   DATE NOT NULL,
    effective_end_date     DATE,
    is_current              BOOLEAN NOT NULL
)
USING DELTA;