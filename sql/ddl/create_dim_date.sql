-- Static/pre-populated calendar dimension; date_key is a smart key (YYYYMMDD int),
-- not an identity column, since it's derived deterministically from full_date.
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key    INT NOT NULL,     -- e.g. 20250115 for 2025-01-15
    full_date   DATE NOT NULL,
    year        INT,
    month       INT,
    quarter     INT,
    day_name    STRING
)
USING DELTA;