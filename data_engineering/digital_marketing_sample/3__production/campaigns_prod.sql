-- campaigns_prod.sql
-- Production layer for campaigns with incremental load

-- Create production table
CREATE TABLE IF NOT EXISTS campaigns_prod (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(50)
);

-- Incremental insert from staging
INSERT INTO campaigns_prod (campaign_id, campaign_name)
SELECT s.campaign_id, s.campaign_name
FROM campaigns_staging s
LEFT JOIN campaigns_prod p
    ON s.campaign_id = p.campaign_id
WHERE p.campaign_id IS NULL;

