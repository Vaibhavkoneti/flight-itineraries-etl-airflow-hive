# Flight Itineraries Data Pipeline — Medallion Architecture on Hadoop, Hive & Airflow

An end-to-end batch ETL pipeline that ingests ~29GB of raw US flight itinerary data, lands it in HDFS, and processes it through a **Bronze → Silver → Gold (medallion) architecture** using **Hive** for transformation and **Apache Airflow** for orchestration — all containerized with **Docker Compose**.

## Key results

**Business:**
- Pricing comparison across airlines — e.g. Alaska Airlines avg fare **$285.91** (7,587 flights) vs Delta avg fare **$508.98** (1,496 flights)
- Busiest route identified: **LGA → LAX, 3,076 flights**, followed by LAX→EWR (2,931) and LAX→LGA (2,907)
- Seasonal fare swing detected: April avg fare **$360.55** (203,125 flights) vs May avg fare **$323.98** (148,792 flights), a ~10% drop

**System:**
- **28.96GB** raw CSV ingested into HDFS Bronze layer, cleaned down to **351,917 rows** in the Silver layer
- HDFS storage footprint: only **2.91% disk used** (29.32GB / 1006.85GB capacity) — comfortable headroom for scale
- Storage format optimization across layers: raw CSV (text) → **ORC** (Silver, columnar + compressed) → **Parquet** (Gold, columnar + compressed)

## The 5 V's of Big Data this project addresses

![Big Data 5 Vs](docs/images/big_data_5vs.png)

| V | How it shows up here |
|---|---|
| **Volume** | ~29GB / ~5.8M rows of raw itinerary data stored in HDFS |
| **Velocity** | Daily scheduled batch ETL via Airflow |
| **Variety** | Raw CSV → columnar ORC → analytics-ready Parquet |
| **Veracity** | Schema enforcement, type casting, and date parsing in the Silver layer |
| **Value** | Pre-aggregated Gold tables answering concrete business questions (pricing, routes, seasonality) |

## Architecture

![Architecture diagram](docs/images/architecture_diagram.png)

```
CSV (local) → HDFS (WebHDFS) → Hive External Table (Bronze)
                                       ↓
                          Hive ORC Table, typed & cleaned (Silver)
                                       ↓
                     Hive Parquet Tables, business aggregates (Gold)
```

All services run in Docker: Postgres (Airflow + Hive metastore backing store), HDFS NameNode/DataNode, Hive Server2, Hive Metastore, and the Airflow webserver/scheduler.

## Medallion layers

**Bronze** — raw `itineraries.csv` uploaded to HDFS via WebHDFS, exposed as an external Hive table (`bronze_itineraries`), no transformation.

**Silver** — `bronze_itineraries` is cleaned and typed (dates parsed, epoch timestamps converted) and materialized as a Hive table stored in **ORC** format (`silver_itineraries`).

**Gold** — three business-ready aggregate tables stored in **Parquet**, built from the Silver layer:
- `gold_avg_price_by_airline` — average fare and flight count per airline
- `gold_frequent_routes` — top 100 most frequent origin → destination routes
- `gold_price_distribution_by_month` — min/max/avg/median fare per month

## Orchestration (Airflow DAG)

`itineraries_data_analysis_medallion` runs daily and chains:

```
create_connections >> bronze_layer >> silver_layer >> gold_layer
```

- `create_connections` — registers the WebHDFS and HiveServer2 Airflow connections
- `bronze_layer` — uploads the CSV to HDFS and creates the Bronze external table
- `silver_layer` — runs the Hive CTAS query producing the cleaned ORC table
- `gold_layer` — runs the three Hive aggregation queries producing the Parquet Gold tables

Code layout:

```
airflow/dags/itineraries_analysis/
├── main.py              # DAG definition and task wiring
├── config.py            # HDFS/Hive endpoints and table paths
├── tasks.py             # Airflow connection bootstrap
├── utils.py             # Small file/logging helpers
└── modules/
    ├── bronze.py        # Bronze layer: HDFS ingest + external table
    ├── silver.py        # Silver layer: cleaning + ORC CTAS
    └── gold.py          # Gold layer: aggregation + Parquet tables
```

![Module design](docs/images/Module_design.png)

## Tech stack

- **Apache Airflow 2.6.2** — orchestration
- **Hadoop 3.2.1 (HDFS)** — distributed storage
- **Apache Hive 2.3.2** — SQL-on-Hadoop transformation layer
- **PostgreSQL** — Airflow metadata DB and Hive metastore backing store
- **Docker Compose** — service orchestration for the whole stack
- **PyHive / WebHDFS hooks** — Python connectivity from Airflow tasks to Hive/HDFS

## Running it locally

> Requires Docker and Docker Compose. The stack spins up 8 containers (Postgres, NameNode, DataNode, Hive Server2, Hive Metastore, Hive Metastore Postgres, Airflow webserver, Airflow scheduler).

```bash
docker-compose up -d
```

Airflow UI: http://localhost:8080 (user: `admin` / password: `admin`)
HDFS NameNode UI: http://localhost:9870

### Dataset

The full dataset (~29GB, [Expedia flight itineraries](https://www.kaggle.com/datasets/dilwong/flightprices)) is **not included** in this repo. A 1,000-row sample is provided at `airflow/data/itineraries_sample.csv` so the pipeline can be test-run end-to-end. To run against the full dataset, download it from Kaggle and place it at `airflow/data/itineraries.csv`.

## Screenshots

| | |
|---|---|
| **HDFS NameNode overview** ![Hadoop connection](docs/images/hadoop_connection.png) | **HDFS browser — bronze/silver/gold dirs** ![HDFS namenode browser](docs/images/hdfs_namenode_browser.png) |
| **Datanode usage** ![Datanode usage](docs/images/datanode_usage.png) | **Datanode data integration** ![Datanode integration](docs/images/datanode_data_integration.png) |
| **Docker Compose volumes/networking** ![Docker compose](docs/images/docker_compose.png) | **Container network** ![Network](docs/images/network.png) |
| **DAG run history & details** ![DAG details](docs/images/DAG_details.png) | **ORC (Silver) storage** ![ORC](docs/images/orc.png) |
| **Parquet (Gold) storage** ![Parquet](docs/images/parquet.png) | |

### Gold layer outputs

| | |
|---|---|
| **Average price by airline** ![Avg price](docs/images/avg_price.png) | **Most frequent routes** ![Frequent routes](docs/images/frequent_route.png) |
| **Price distribution by month** ![Price distribution](docs/images/price_distribution.png) | |
