-- keyword_stats_prod.sql
-- Production layer for keyword_stats with incremental load

-- Create production table
CREATE TABLE IF NOT EXISTS keyword_stats_prod (
    click_timestamp TIMESTAMP,
    keyword_id VARCHAR(4),
    campaign_id VARCHAR(50),
    click_id VARCHAR(5) PRIMARY KEY,
    click_cost DECIMAL(10,2)
);

-- Incremental insert from staging
INSERT INTO keyword_stats_prod (click_timestamp, keyword_id, campaign_id, click_id, click_cost)
SELECT s.click_timestamp, s.keyword_id, s.campaign_id, s.click_id, s.click_cost
FROM keyword_stats_staging s
LEFT JOIN keyword_stats_prod p
    ON s.click_id = p.click_id
WHERE p.click_id IS NULL;

