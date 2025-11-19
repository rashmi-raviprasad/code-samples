# Digital Marketing Analytics Data Pipeline

A portfolio project demonstrating **enterprise-level data modeling, transformation logic, and pipeline design** in SQL.

---

## Overview

This project simulates a simplified digital marketing data pipeline, from raw data ingestion to aggregated reporting output.  

It is designed to highlight how SQL and structured modeling principles can integrate campaign, keyword, and traffic data into a unified reporting layer.

While the dataset is small-scale and self-contained, the design mirrors real-world **ETL best practices**, including **staging for validation, incremental production loads, and modular pipeline orchestration**.

---

## ETL Pipeline Architecture

              ┌───────────────┐
              │   Raw CSV     │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │ Staging Layer │
              │ (Validation & │
              │   Cleansing)  │
              └───────┬───────┘
                      │
                      ▼
             ┌─────────────────┐
             │Production Layer │
             │  (Incremental   │
             │     Loads)      │
             └────────┬────────┘
                      │
                      ▼
              ┌───────────────┐
              │ Downstream    │
              │ Analytics &   │
              │ Reporting     │
              └───────────────┘

**Notes on this portfolio structure:**

- The **staging script** (`[table]_staging.sql`) includes **raw ingestion and validation**.  
- The **production script** (`[table]_prod.sql`) handles **incremental inserts**.  
- Pipeline orchestration is demonstrated with `pipeline_config.yml`, showing **task dependencies and sequencing**.

---

## Repository Contents

| Folder/File                    | Description |
|--------------------------------|------------|
| `raw_data/`                    | Example CSVs representing raw input data |
| `[table]_staging.sql`          | Staging and validation step |
| `[table]_prod.sql`             | Production layer and incremental insert step |
| `paid_marketing_stats.sql`     | Final reporting layer combining all relevant metrics and dimensions |
| `pipeline_config.yml`          | Orchestration of all tables, showing dependencies |

---

## Data Model

### 1. Input Tables

| Table             | Description |
|-------------------|------------|
| `campaigns`       | Campaign identifiers and names |
| `keywords`        | Keyword identifiers and attributes |
| `keyword_stats`   | Click-level metrics including cost |
| `revenue_stats`   | Click-level metrics including revenue |
| `traffic_stats`   | Article-level daily impressions data |

### 2. Output Table: `paid_marketing_stats`

Aggregated table combining key dimensions and metrics:

| Column                | Description |
|----------------------|-------------|
| `date`               | Date of metric aggregation |
| `campaign_id`        | Unique campaign identifier |
| `campaign_name`      | Campaign name |
| `keyword_id`         | Unique keyword identifier |
| `keyword_search_term`| Keyword search term |
| `device`             | Device type (mobile/desktop) |
| `article_id`         | Unique article identifier |
| `article_title`      | Article title |
| `article_author`     | Author of the article |
| `total_clicks`       | Total clicks |
| `total_cost`         | Total advertising cost |
| `total_revenue`      | Total generated revenue |
| `total_impressions`  | Total content impressions |

---

## Author's Note & Highlights

This project is not meant to be runnable, but rather a case study for enterprise-level data engineering:
- **Layered ETL architecture**: staging → production  
- **Validation in staging**: nulls, duplicates, basic data quality  
- **Incremental inserts in production** for idempotency  
- **Modular SQL files**: easy to extend and maintain  
- **Orchestration via `pipeline_config.yml`** demonstrates task dependencies and ETL flow  

