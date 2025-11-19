-- paid_marketing_stats.sql
---- SQL script to aggregate paid marketing statistics including cost, revenue, clicks, and impressions
---- Joins data from sa360_campaigns, sa360_keywords, sa360_keyword_stats, traffic_stats, and revenue_stats tables
---- Output: paid_marketing_stats.csv

-- Parameters for scheduling runs:
---- Incremental runs based on date (updates daily based on click_timestamp and date fields)
---- Unique key: date, campaign_id, keyword_id, device, article_id

-- Column descriptions:
---- date: Date of the statistics
---- campaign_id: Unique identifier for the marketing campaign
---- campaign_name: Name of the marketing campaign
---- keyword_id: Unique identifier for the keyword
---- keyword_search_term: Search term associated with the keyword
---- device: Device type (e.g., mobile, desktop)
---- article_id: Unique identifier for the article
---- article_title: Title of the article
---- article_author: Author of the article
---- total_cost: Total cost incurred for clicks
---- total_revenue: Total revenue generated from clicks
---- total_clicks: Total number of clicks 
---- total_impressions: Total number of impressions

-- Step 1: Join keyword_stats with campaigns and keywords to get campaign and keyword dimensions
WITH campaign_data AS (
  SELECT
    DATE(ks.click_timestamp) AS date,
    c.campaign_id,
    c.campaign_name,
    k.keyword_id,
    k.keyword_search_term,
    k.device,
    ks.click_id,
    ks.click_cost AS cost
  FROM
    keyword_stats_prod ks
  LEFT JOIN campaigns_prod c 
    ON ks.campaign_id = c.campaign_id
  LEFT JOIN keywords_prod k 
    ON ks.keyword_id = k.keyword_id
), 

-- Step 2: Join the above result with revenue_stats on click_id to add revenue data
revenue_data AS (
  SELECT
    cd.*,
    rs.article_id,
    rs.revenue
  FROM
    campaign_data cd
  LEFT JOIN revenue_stats_prod rs 
    ON cd.click_id = rs.click_id
),

-- Step 3: Prepare traffic_stats for joining
traffic_data AS (
  SELECT
    ts.date,
    ts.article_id,
    ts.article_title,
    ts.article_author,
    ts.impressions
  FROM
    traffic_stats_prod ts
)

-- Final Step: Join campaign + revenue data with traffic data and aggregate the results
SELECT
  r.date,
  r.campaign_id,
  r.campaign_name,
  r.keyword_id,
  r.keyword_search_term,
  r.device,
  r.article_id,
  t.article_title,
  t.article_author,
  SUM(r.cost) AS total_cost,
  SUM(r.revenue) AS total_revenue,
  COUNT(DISTINCT r.click_id) AS total_clicks,
  SUM(t.impressions) AS total_impressions
FROM
  revenue_data r
LEFT JOIN traffic_data t 
  ON r.article_id = t.article_id 
  AND r.date = t.date
GROUP BY
  r.date, 
  r.campaign_id, 
  r.campaign_name, 
  r.keyword_id, 
  r.keyword_search_term, 
  r.device, 
  r.article_id, 
  t.article_title, 
  t.article_author
