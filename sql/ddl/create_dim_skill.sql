-- Type 1 dimension: one row per distinct normalized skill
CREATE TABLE IF NOT EXISTS gold.dim_skill (
    skill_key       BIGINT GENERATED ALWAYS AS IDENTITY,
    skill_name       STRING NOT NULL,   -- normalized skill label, used for MERGE matching
    skill_category   STRING             -- derived: e.g. language / cloud / tool / soft-skill
)
USING DELTA;