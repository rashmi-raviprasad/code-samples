-- keyword_stats_staging.sql
-- Staging layer for keyword_stats (includes raw ingestion and validation)

-- Create staging table
CREATE TABLE IF NOT EXISTS keyword_stats_staging (
    click_timestamp TIMESTAMP,
    keyword_id VARCHAR(4),
    campaign_id VARCHAR(50),
    click_id VARCHAR(5) PRIMARY KEY,
    click_cost DECIMAL(10,2)
);

-- Load data from raw CSV
\copy keyword_stats_staging FROM 'raw_data/sa360_keyword_stats.csv' DELIMITER ',' CSV HEADER;

-- Validate data (several example checks)
-- Check for missing primary keys
SELECT * FROM keyword_stats_staging WHERE click_id IS NULL;

-- Check for duplicates
SELECT click_id, COUNT(*)
FROM keyword_stats_staging
GROUP BY click_id
HAVING COUNT(*) > 1;

