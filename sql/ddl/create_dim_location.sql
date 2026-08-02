-- Type 1 dimension: no history tracking, overwrite on change
CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_key       BIGINT GENERATED ALWAYS AS IDENTITY,
    location_natural_key STRING NOT NULL,   -- normalized "country|region" or raw source string
    canonical_country   STRING,
    region               STRING
)
USING DELTA;