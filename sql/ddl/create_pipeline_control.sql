-- Control table behind the Lookup + ForEach pattern in master_pipeline.json.
-- Enabling or disabling a source becomes a data change here rather than a
-- pipeline redeploy, which is the same argument config/sources.yaml already
-- makes for the connectors themselves (Chapter 2) — extended up into the
-- orchestration layer.
CREATE TABLE IF NOT EXISTS control.pipeline_sources (
    source_name          STRING  NOT NULL,
    enabled              BOOLEAN NOT NULL,
    layer                STRING  NOT NULL,   -- 'ingestion' | 'bronze' | 'silver' | 'gold'
    last_run_date        DATE,
    last_run_status      STRING,             -- 'success' | 'failed' | 'running'
    consecutive_failures INT
)
USING DELTA;

-- Seed data mirrors config/sources.yaml. The two are kept in sync by hand for
-- this project. A larger team would generate one from the other, and
-- run_ingestion.run() fails loudly rather than silently no-opping when a name
-- here matches nothing enabled in sources.yaml.
--
-- MERGE rather than INSERT so re-running this script against an existing table
-- does not duplicate the seed rows or reset the run-history columns of a source
-- already being tracked.
MERGE INTO control.pipeline_sources AS target
USING (
    SELECT * FROM VALUES
        ('adzuna',  true,  'ingestion'),
        ('jooble',  true,  'ingestion'),
        ('usajobs', false, 'ingestion')   -- scaffolded but off (Chapter 1)
    AS seed (source_name, enabled, layer)
) AS source
ON target.source_name = source.source_name AND target.layer = source.layer
WHEN MATCHED THEN UPDATE SET target.enabled = source.enabled
WHEN NOT MATCHED THEN INSERT (
    source_name, enabled, layer, last_run_date, last_run_status, consecutive_failures
)
VALUES (
    source.source_name, source.enabled, source.layer, NULL, NULL, 0
);
