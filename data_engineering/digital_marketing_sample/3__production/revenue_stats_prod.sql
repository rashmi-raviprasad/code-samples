-- revenue_stats_prod.sql
-- Production layer for revenue_stats with incremental load

-- Create production table
CREATE TABLE IF NOT EXISTS revenue_stats_prod (
  click_timestamp TIMESTAMP,
  click_id VARCHAR(5) PRIMARY KEY,
  article_id VARCHAR(7),
  revenue DECIMAL(10,2)
);

-- Incremental insert from staging
INSERT INTO revenue_stats_prod (click_timestamp, click_id, article_id, revenue)
SELECT s.click_timestamp, s.click_id, s.article_id, s.revenue
FROM revenue_stats_staging s
LEFT JOIN revenue_stats_prod p
    ON s.click_id = p.click_id
WHERE p.click_id IS NULL;

