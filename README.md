## 🚀 Automated ETL Pipelines with Apache Airflow, Snowflake, and Python

A semester-long data engineering project by David Hansen

This project showcases a fully containerized Apache Airflow 3.0 environment orchestrating end-to-end ETL pipelines for real-world data sources. It demonstrates production-style workflows including daily incremental loads, historical backfills, SSH-tunneled data extraction, and Snowflake warehousing.

## 📌 Key Features
### 🌀 Orchestration with Apache Airflow 3.0

DAGs for daily incremental ingestion and historical backfill.

### 🐳 Full Docker Deployment

Snowflake Python connector libraries baked in

All services are configured with persistent volumes and environment variables.

### ❄️ Snowflake Data Warehouse Integration

The pipelines support:

Private-key Snowflake authentication

RAW → MODELED table design

Daily UPSERT logic using Snowflake MERGE

Backfills for custom date ranges

Handling UTC ↔ local timezone conversion

Efficient batching using write_pandas() and staging tables

### 📥 Multi-source Data Ingestion

This project ingests several real data systems:

1. Open Meteo Weather API 

Historical daily API pulls and pushes to Snowflake 

Pulls data for 20 major cities across the United States

2. SFTP / SSH News Feed

Uses Paramiko for secure SSH tunneling

Pulls CSVs from a restricted backend system hosted on a non-standard port

Automatically detects new files, downloads, and merges them

3. MongoDB Tunnel Ingestion (S&P 500 Stock Dataset)

Connects through SSH port forwarding

Pulls daily stock price documents into Snowflake
