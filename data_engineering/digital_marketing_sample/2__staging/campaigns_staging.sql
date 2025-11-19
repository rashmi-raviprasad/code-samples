-- campaigns_staging.sql
-- Staging layer for campaigns (includes raw ingestion and validation)

-- Create staging table
CREATE TABLE IF NOT EXISTS campaigns_staging (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(50)
);

-- Load data from raw CSV
\copy campaigns_staging FROM 'raw_data/sa360_campaigns.csv' DELIMITER ',' CSV HEADER;

-- Validate data (several example checks)
-- Check for missing primary keys
SELECT * FROM campaigns_staging WHERE campaign_id IS NULL;

-- Check for duplicates
SELECT campaign_id, COUNT(*)
FROM campaigns_staging
GROUP BY campaign_id
HAVING COUNT(*) > 1;
