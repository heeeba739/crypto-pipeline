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

##  Environment Variables

Copy `.env.exemple` to `.env` and fill in your values:

```bash
cp .env.exemple .env
```

```env
CONFLUENT_BOOTSTRAP_SERVERS=your_bootstrap_server
CONFLUENT_API_KEY=your_api_key
CONFLUENT_API_SECRET=your_api_secret
NEWSAPI_KEY=your_newsapi_key
FRED_API_KEY=your_fred_api_key
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

##  Installation

```bash
pip install confluent-kafka websocket-client requests pandas pyarrow supabase redis
```

##  Run the Producers

```bash
# Terminal 1 — Binance WebSocket
python binance_producer.py

# Terminal 2 — NewsAPI
python news_producer.py

# Terminal 3 — FRED API
python fred_producer.py
```

##  Kafka Topics

| Topic | Partitions | Description |
|---|---|---|
| `trades_topic` | 3 | Real-time Binance trades |
| `news_topic` | 3 | NewsAPI crypto articles |
| `enriched_market_topic` | 3 | Spark enriched data |
| `alerts_topic` | 1 | Volatility alerts |

##  Spark Structured Streaming

- **Watermark** : 5 minutes
- **Windowing** : 1 min / 5 min
- **Stream-to-Stream Join** : News + Price spike ±5 min
- **Multi-Sink Fan-Out** : Data Lake + Supabase + alerts_topic

##  Delivered

- ✅ Kafka pipeline operational (Confluent Cloud)
- ✅ Spark Structured Streaming validated (Databricks)
- ✅ Airflow FRED DAG deployed (Astronomer)
- ✅ Supabase warehouse connected
- ✅ Python scripts tested and documented
- ✅ Architecture diagram

##👤 Author

**Hiba Az** — Data Engineer @ DATA NEXT
