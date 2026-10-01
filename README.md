# 🎬 Movies Data Engineering Pipeline

An end-to-end Data Engineering project that automates the ingestion, processing,
transformation, and analytics of movie data using modern cloud and data
engineering technologies.

## 🏗️ Architecture

```text
Movie CSV
   │
   ▼
Apache Airflow
   │
   ▼
AWS S3 - Raw Layer
   │
   ▼
Databricks + PySpark
   │
   ├── Bronze
   ├── Silver
   └── Gold
   │
   ▼
AWS S3 - Gold Layer
   │
   ▼
Snowflake
   │
   ▼
SQL Analytics