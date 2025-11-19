-- keywords_prod.sql
-- Production layer for keywords with incremental load

-- Create production table
CREATE TABLE IF NOT EXISTS keywords_prod (
  keyword_id VARCHAR(4) PRIMARY KEY,
  campaign_id VARCHAR(50),
  keyword_search_term VARCHAR(100),
  device VARCHAR(20)
);

-- Incremental insert from staging
INSERT INTO keywords_prod (keyword_id, campaign_id, keyword_search_term, device)
SELECT s.keyword_id, s.campaign_id, s.keyword_search_term, s.device
FROM keywords_staging s
LEFT JOIN keywords_prod p
    ON s.keyword_id = p.keyword_id
WHERE p.keyword_id IS NULL;

