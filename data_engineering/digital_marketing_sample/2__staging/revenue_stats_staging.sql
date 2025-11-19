-- revenue_stats_staging.sql
-- Staging layer for revenue_stats (includes raw ingestion and validation)

-- Create staging table
CREATE TABLE IF NOT EXISTS revenue_stats_staging (
  click_timestamp TIMESTAMP,
  click_id VARCHAR(5) PRIMARY KEY,
  article_id VARCHAR(7),
  revenue DECIMAL(10,2)
);

-- Load data from raw CSV
\copy revenue_stats_staging FROM 'raw_data/revenue_stats.csv' DELIMITER ',' CSV HEADER;

-- Validate data (several example checks)
-- Check for missing primary keys
SELECT * FROM revenue_stats_staging WHERE click_id IS NULL;

-- Check for duplicates
SELECT click_id, COUNT(*)
FROM revenue_stats_staging
GROUP BY click_id
HAVING COUNT(*) > 1;

