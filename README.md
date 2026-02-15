# SQL Data Warehouse Project

A weather data warehouse built with a **medallion architecture** (Bronze → Silver → Gold), orchestrated by Apache Airflow and backed by PostgreSQL. Extracts hourly weather forecasts from the [Open-Meteo API](https://open-meteo.com/) for multiple cities and transforms them through progressively refined layers.

## Architecture

```
 Open-Meteo API
       │
       ▼
┌─────────────┐    ┌──────────────┐    ┌──────────────┐
│   Bronze    │    │    Silver    │    │     Gold     │
│  (raw JSONB)│──▶│ (structured) │───▶│ (aggregated) │
│  Airflow DAG│    │  Airflow DAG │    │  Airflow DAG │
└─────────────┘    └──────────────┘    └──────────────┘
      Dataset trigger     Dataset trigger
```

### Data Layers

| Layer | Table | Description |
|-------|-------|-------------|
| Bronze | `bronze.weather_raw` | Raw JSONB API responses, one row per API call per city |
| Bronze | `bronze.api_error_log` | API extraction error tracking |
| Silver | `silver.weather_observations` | Flattened hourly observations (one row per hour per city) |
| Gold | `gold.daily_weather_summary` | Daily aggregates per city (avg/min/max temp, total precipitation, etc.) |

### Cities Tracked (You can always add more later)

- Bangkok (Asia/Bangkok)
- Tokyo (Asia/Tokyo)
- Paris (Europe/Paris)

### Hourly Metrics Collected

temperature, apparent temperature, relative humidity, precipitation, weather code, wind speed, cloud cover, UV index, rain, showers, snowfall, is_day

## Infrastructure

Everything runs in Docker via `docker-compose.yaml`:

| Service | Purpose |
|---------|---------|
| **postgres-warehouse** | Main data warehouse (PostgreSQL 16) |
| **postgres-airflow** | Airflow metadata store |
| **Redis** | Celery message broker |
| **Airflow** (apiserver, scheduler, dag-processor, worker, triggerer) | Pipeline orchestration (CeleryExecutor) |
| **pgAdmin** | Database management UI |

## Pipeline

### 1. Bronze DAG (`weather_extraction_daily`)

- **Schedule:** Daily at 6:00 AM UTC
- **What it does:** Calls the Open-Meteo forecast API for each city using dynamic task mapping (`.expand()`), stores the full JSONB response in `bronze.weather_raw`
- **Outlet:** Emits `Dataset("bronze_weather_raw")` to trigger downstream DAGs

### 2. Silver DAG (`silver_layer_pipeline`)

- **Schedule:** Triggered automatically when the bronze DAG completes (via Airflow Dataset)
- **What it does:** Runs `silver_layer.sql` which `UNNEST`s the JSONB hourly arrays into structured rows in `silver.weather_observations`
- **Outlet:** Emits `Dataset("silver_weather_observations")` to trigger the gold DAG
- **Idempotency:** `ON CONFLICT (location_name, observation_timestamp) DO NOTHING`
- **Validation:** Checks row counts and null values on critical columns

### 3. Gold DAG (`gold_layer_pipeline`)

- **Schedule:** Triggered automatically when the silver DAG completes (via Airflow Dataset)
- **What it does:** Aggregates silver hourly rows into daily summaries per city (avg/min/max temperature, total precipitation, dominant weather code, daylight hours, etc.)
- **Idempotency:** `ON CONFLICT (observation_date, location_name) DO UPDATE`
- **Validation:** Checks record counts, per-location breakdowns, and null values on critical columns

### 4. Backfill DAGs

- **`silver_backfill_historical`** — Manual trigger. Reprocesses historical bronze data into silver with optional `start_date`/`end_date` filtering.
- **`gold_backfill_historical`** — Manual trigger. Reprocesses historical silver data into gold daily summaries with optional date filtering.

Both support the `DATE_FILTER_PLACEHOLDER` pattern for flexible date range backfills.

## Project Structure

```
on_premise/
├── airflow/
│   ├── dags/
│   │   ├── bronze_dag.py              # Extract & load raw data
│   │   ├── silver_layer_dag.py        # Transform bronze → silver
│   │   ├── gold_layer_dag.py          # Aggregate silver → gold
│   │   ├── silver_backfill_dag.py     # Manual silver backfill
│   │   └── gold_backfill_dag.py       # Manual gold backfill
│   ├── config/airflow.cfg
│   └── logs/
├── src/weather_etl/
│   ├── extractors/
│   │   ├── open_meteo_api_extractor.py  # API client with caching & retries
│   │   └── database_error_logger.py     # Logs API failures to bronze.api_error_log
│   ├── loaders/
│   │   └── bronze_loader.py             # SQLExecutor: runs SQL files, queries, inserts, backfills
│   └── utils/
│       ├── db_connection.py             # Centralized warehouse connection helper
│       └── callbacks.py                 # on_failure_callback for all DAGs
├── sql/
│   ├── 00_init/
│   │   ├── init_db.sql                  # Schema, roles, grants
│   │   └── health_check.sql
│   ├── 01_bronze/bronze_layer.sql       # Bronze table DDL + indexes
│   ├── 02_silver/silver_layer.sql       # Silver DDL + INSERT transformation
│   └── 03_gold/gold_layer.sql           # Gold DDL + daily aggregation
├── docker-compose.yaml
├── Dockerfile
├── pyproject.toml
├── .env.example
└── .env
```

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.12+

### Setup

1. **Clone the repository**
   ```bash
   git clone <repo-url> && cd sql-data-warehouse-project/on_premise
   ```

2. **Create your `.env` file**
   ```bash
   cp .env.example .env
   ```
   Fill in the `<change_me>` values. Set `AIRFLOW_UID` with:
   ```bash
   echo $(id -u)
   ```

3. **Start the services**
   ```bash
   docker compose up --build -d
   ```

4. **Initialize the warehouse schema**

   Connect to `postgres-warehouse` (e.g. via pgAdmin at `localhost:81`) and run the SQL scripts in order:
   ```
   sql/00_init/init_db.sql
   sql/01_bronze/bronze_layer.sql
   sql/02_silver/silver_layer.sql
   sql/03_gold/gold_layer.sql
   ```

5. **Access the Airflow UI** at `http://localhost:8081` (default credentials: `airflow` / `airflow`)

### Running the Pipeline

- The **bronze DAG** runs automatically at 6 AM UTC daily (or trigger manually from the Airflow UI)
- The **silver DAG** triggers automatically after each bronze run (via Dataset)
- The **gold DAG** triggers automatically after each silver run (via Dataset)
- To **backfill silver**, trigger `silver_backfill_historical` manually with optional date parameters
- To **backfill gold**, trigger `gold_backfill_historical` manually with optional date parameters

## Tech Stack

- **Orchestration:** Apache Airflow 3.1.0 (CeleryExecutor)
- **Database:** PostgreSQL 16
- **Language:** Python 3.12
- **API:** Open-Meteo (free, no API key required)
- **Key Libraries:** openmeteo-requests, psycopg2, pandas, requests-cache

## Future Production Features

- [x] Implement multi-city extraction
- [x] Wire gold layer to an Airflow DAG
- [x] Implement alerting (on_failure_callback)
- [ ] Add comprehensive observability with structured logging
- [ ] Deploy to AWS or GCP for managed Airflow
- [ ] Implement data quality checks
- [ ] Add unit and integration tests
