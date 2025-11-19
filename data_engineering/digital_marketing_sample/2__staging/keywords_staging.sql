-- keywords_staging.sql
-- Staging layer for keywords (includes raw ingestion and validation)

-- Create staging table
CREATE TABLE IF NOT EXISTS keywords_staging (
  keyword_id VARCHAR(4) PRIMARY KEY,
  campaign_id VARCHAR(50),
  keyword_search_term VARCHAR(100),
  device VARCHAR(20)
);

-- Load data from raw CSV
\copy keywords_staging FROM 'raw_data/sa360_keywords.csv' DELIMITER ',' CSV HEADER;

-- Validate data (several example checks)
-- Check for missing primary keys
SELECT * FROM keywords_staging WHERE keyword_id IS NULL;

-- Check for duplicates
SELECT keyword_id, COUNT(*)
FROM keywords_staging
GROUP BY keyword_id
HAVING COUNT(*) > 1;
