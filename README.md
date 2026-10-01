\# 🎬 Movies Data Engineering Pipeline



An end-to-end Data Engineering project that automates the ingestion, processing,

transformation, and analytics of movie data using modern cloud and data

engineering technologies.



\## 🏗️ Architecture



```text

Movie CSV

&#x20;  │

&#x20;  ▼

Apache Airflow

&#x20;  │

&#x20;  ▼

AWS S3 - Raw Layer

&#x20;  │

&#x20;  ▼

Databricks + PySpark

&#x20;  │

&#x20;  ├── Bronze

&#x20;  │

&#x20;  ├── Silver

&#x20;  │

&#x20;  └── Gold

&#x20;  │

&#x20;  ▼

AWS S3 - Gold Layer

&#x20;  │

&#x20;  ▼

Snowflake

&#x20;  │

&#x20;  ▼

SQL Analytics

