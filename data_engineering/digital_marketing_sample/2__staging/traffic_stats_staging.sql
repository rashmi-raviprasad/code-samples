-- traffic_stats_staging.sql
-- Staging layer for traffic_stats (includes raw ingestion and validation)

-- Create staging table
CREATE TABLE IF NOT EXISTS traffic_stats_staging (
  date DATE,
  article_id VARCHAR(7),
  article_title VARCHAR(100),
  article_author VARCHAR(50),
  impressions INT
);

-- Load data from raw CSV
\copy traffic_stats_staging FROM 'raw_data/traffic_stats.csv' DELIMITER ',' CSV HEADER;

-- Validate data (several example checks)
-- Check for missing keys
SELECT * FROM traffic_stats_staging WHERE article_id IS NULL;

-- Check for duplicates
SELECT date, article_id, COUNT(*)
FROM traffic_stats_staging
GROUP BY date, article_id
HAVING COUNT(*) > 1;

