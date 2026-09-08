# 🚀 Real-Time Crypto Intelligence Pipeline

A real-time Data Engineering platform combining streaming and batch processing to ingest, enrich, and distribute crypto financial data.

##  Context
**INVISTIS** wants to integrate real-time crypto data into its portfolio management application. **DATA NEXT** is mandated to design and validate the pipeline.

##  Architecture

(architecture_diagram.png)

##  Data Sources

| Source | Type | Kafka Topic | Description |
|---|---|---|---|
| Binance WebSocket | Streaming | `trades_topic` | BTC, ETH, BNB, SOL, ADA trades |
| NewsAPI | Streaming | `news_topic` | Enriched crypto articles |
| FRED API | Batch (Airflow) | `fred_topic` | Macroeconomic indicators |

##  Tech Stack

| Component | Technology |
|---|---|
| Message Broker | Confluent Cloud (Kafka) |
| Stream Processing | Spark 4.1 (Databricks) |
| Batch Orchestration | Apache Airflow (Astronomer) |
| Warehouse | Supabase (PostgreSQL) |
| Cache | Redis (optional) |
| Data Lake | Databricks (Parquet) |
| Language | Python 3.11 |

##  Project Structure
