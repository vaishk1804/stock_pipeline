# Stock Market ELT Pipeline with Real-Time Streaming

A dual-path data engineering platform that ingests stock market data through both a scheduled batch pipeline and a real-time streaming layer, transforms it through a dbt-modeled warehouse, and serves it through an interactive dashboard — fully containerized and orchestrated end to end.

**Built by Vaishnavi Kashyap**

---

## Overview

Tracks 10 stocks through two independent ingestion paths feeding one warehouse:

- **Batch** — Airflow orchestrates a daily pipeline that fetches end-of-day OHLCV data, validates it, and transforms it through dbt.
- **Streaming** — A Kafka producer polls live quotes every 60 seconds during market hours; a consumer validates and writes them to PostgreSQL in real time.

Both paths land in the same `raw → staging → marts` warehouse, so the dashboard and analytical models query one unified source regardless of which path the data arrived through.

---

## Architecture

| Path | Trigger | Source | Lands in | Cadence |
|---|---|---|---|---|
| Batch | Airflow DAG (cron) | Alpha Vantage `TIME_SERIES_DAILY` | `raw.daily_prices` | Daily, 6 AM UTC, Mon–Fri |
| Streaming | Kafka producer (continuous) | Alpha Vantage `GLOBAL_QUOTE` | `raw.intraday_prices` | Every 60s, market hours |

**Flow:** ingest (batch + stream) → validate → dbt staging → dbt marts → dbt test → Streamlit dashboard.

The Kafka topic `stock.intraday.quotes` has 10 partitions, one per ticker (keyed by ticker symbol), with a dead-letter queue (`.dlq`) for malformed messages.

---

## Tech Stack

Apache Airflow 2.8.1 · dbt-postgres 1.7.0 · Apache Kafka (Confluent 7.5.0) · PostgreSQL 15 · Docker Compose · Streamlit 1.32.0 · Plotly · Kafka UI

---

## Quickstart

```bash
git clone https://github.com/vaishk1804/stock_pipeline.git
cd stock_pipeline

cp .env.example .env
# Add your ALPHA_VANTAGE_API_KEY to .env

docker compose up -d
```

This starts the full stack: PostgreSQL, Airflow (webserver, scheduler, init), dbt, Zookeeper, Kafka, Kafka UI, producer, consumer, and the dashboard.

**Trigger the batch pipeline:** open [localhost:8080](http://localhost:8080) (`admin`/`admin`), unpause and trigger `stock_daily_ingestion`. Runs in 3–5 minutes (free API tier rate-limited to 5 calls/min).

**Check the streaming layer:**

```bash
docker compose logs -f kafka-producer
docker compose logs -f kafka-consumer
```

Active only during US market hours (9:30 AM–4 PM ET, Mon–Fri). Browse messages at [localhost:8090](http://localhost:8090).

**View the dashboard:** [localhost:8501](http://localhost:8501)

---

## dbt Models

```
raw.daily_prices ──┐
                   ├── stg_daily_prices ──┬── fct_daily_returns ──┬── mart_moving_averages
raw.tickers ───────┘                      │                        └── mart_top_movers
                   └── stg_tickers ───────┘
raw.intraday_prices ─────────────── mart_intraday_pulse
```

| Model | Materialization | Key columns |
|---|---|---|
| `stg_daily_prices` / `stg_tickers` | View | cleaned, typed source data |
| `fct_daily_returns` | Table | `daily_return_pct`, `cumulative_return_pct` (LAG, FIRST_VALUE) |
| `mart_moving_averages` | Table | `ma_7d/30d/90d`, `ma_cross_signal` (rolling AVG, LAG) |
| `mart_top_movers` | Table | `gain_rank`, `loss_rank` (DENSE_RANK) |
| `mart_intraday_pulse` | View | `tick_direction`, `consumer_lag_seconds` |

25+ tests: schema tests, composite uniqueness on `(ticker, trade_date)`, a relationship test against `stg_tickers`, and 2 custom SQL tests (`assert_no_negative_prices`, `assert_all_tickers_present`).

---

## Airflow DAG

16 tasks: `check_api_health` → 10 sequential ticker fetches (rate-limit safe) → `validate_raw_data` → `dbt_run_staging` → `dbt_run_marts` → `dbt_test_all` → `pipeline_complete`.

Scheduled `0 6 * * 1-5`, `catchup=False`. Each ticker is an isolated task — one failure doesn't block the others.

---

## Notable Design Decisions

- **Idempotent upserts** (`ON CONFLICT DO UPDATE`) — safe to re-run any date without duplicates
- **`mart_intraday_pulse` is a view, not a table** — underlying data refreshes every 60s, so materializing it would serve stale numbers
- **`ROWS BETWEEN` not `RANGE`** in all window functions — avoids non-deterministic ties
- **Top 3 movers, not top 5** — with only 10 tickers, top 5 covers the whole universe and isn't a real filter
- **Partial moving-average windows are intentional** — early rows average whatever history exists (matches Bloomberg/Yahoo Finance behavior)

---

## Tickers Tracked

AAPL, MSFT, GOOGL, AMZN, META, NVDA, TSLA, JPM, JNJ, V — spanning Technology, Consumer Cyclical, Financial Services, and Healthcare.

---

## Project Structure

```
stock_pipeline/
├── airflow/dags/stock_ingestion_dag.py
├── kafka/producer/  kafka/consumer/
├── dbt/stock_mart/
│   ├── models/staging/   models/marts/
│   └── tests/
├── dashboard/app.py
├── scripts/init_db.sql, ingest_all_tickers.py, validate_raw_data.py
└── docker-compose.yml
