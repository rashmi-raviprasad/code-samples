-- traffic_stats_prod.sql
-- Production layer for traffic_stats with incremental load

-- Create production table
CREATE TABLE IF NOT EXISTS traffic_stats_prod (
  date DATE,
  article_id VARCHAR(7),
  article_title VARCHAR(100),
  article_author VARCHAR(50),
  impressions INT
);

-- Incremental insert from staging
INSERT INTO traffic_stats_prod (date, article_id, article_title, article_author, impressions)
SELECT s.date, s.article_id, s.article_title, s.article_author, s.impressions
FROM traffic_stats_staging s
LEFT JOIN traffic_stats_prod p
    ON s.date = p.date 
    AND s.article_id = p.article_id
WHERE p.article_id IS NULL
    AND p.date IS NULL;

