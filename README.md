# NYC Transportation — Taxi vs. Citi Bike

A batch data pipeline that answers a simple question — *which is faster between two NYC zones, a yellow cab or a Citi Bike?* — while solving four hard data engineering problems that make the question non-trivial to answer at all.

Built with **Apache Airflow 3.1**, **MinIO** (S3-compatible object storage), and **Postgres**, orchestrated via Docker Compose.

> **Attribution:** This project is derived from **Chapter 13 of _Data Pipelines with Apache Airflow, Second Edition_** (Harenslak, de Ruiter, Cabral, Geusebroek & van der Ende, Manning, 2026). The use case, the `PandasOperator` pattern, the `MinioPandasToPostgres` idea, and the raw/processed zoning convention all come from that chapter. Everything below — the framing, the contracts, the deployment, and the docs — is an addition on top of that base.

---

## The Data Engineering Problem

Two sources, both ticking every 15 minutes, otherwise as different as they can be:

| | **Citi Bike** | **Yellow Cab** |
|---|---|---|
| Interface | Authenticated REST API (`GET /recent/minute/15`) | Unauthenticated static file share (`GET /` → JSON listing) |
| Format | JSON list of rides | CSV, one file per 15-minute window |
| Location unit | **Latitude / longitude** of stations | **Taxi zone IDs** of pickup / dropoff |
| Retention | Full history in source DB — **backfillable** | **Files older than 59 minutes are deleted** |
| Backfill | Possible (query further back with a larger window) | **Impossible beyond ~1 hour** |
| Cadence | Request-driven | Cron every 15 minutes |

Ingesting both into a single comparison table forces four problems at once:

1. **Asymmetric retention.** Taxi files vanish after 59 minutes. A scheduler gap longer than one hour is *permanent* data loss for taxi. Citi Bike can be re-fetched; taxi cannot. The pipeline cannot treat the two sources symmetrically.
2. **Unit mismatch.** Citi Bike reports station coordinates; taxi reports zone IDs. Without a reference-data join, there is no shared key to compare on — the two datasets cannot be joined at all.
3. **Heterogeneous ingestion.** One source needs an authenticated HTTP client with a per-file fetch loop; the other is a directory listing plus per-file CSV downloads. A single generic HTTP operator cannot serve both.
4. **Idempotency across reruns.** Both sources represent "the state of the world at a point in time." Re-running the 14:15 interval — because of a failure, a retry, or a manual backfill — must not duplicate rows, must not double-count, and must produce the same result whether it runs once or five times.

The payload (a "fastest mode" table) is trivial; the engineering is in making the dataset **trustworthy** in the presence of all four constraints.

---

## The Solution

### Layered data lake (raw → processed → serving)

Everything the pipeline produces lands in MinIO under a partitioned, interval-keyed layout:

```
datalake/
├── raw/
│   ├── citibike/{YYYYMMDDTHHMMSS}.json      # byte-for-byte API response
│   └── taxi/{MM-DD-YYYY-HH-MM-SS}.csv       # byte-for-byte file-share output
└── processed/
    ├── citibike/{YYYYMMDDTHHMMSS}.parquet   # conformed, zone-mapped
    └── taxi/{YYYYMMDDTHHMMSS}.parquet       # conformed
```

**The raw layer is the source of record.** Because taxi files are deleted after 59 minutes, the raw object written by the pipeline is the *only* copy of that window that will ever exist. Writing raw first is what makes the pipeline re-processable: change the transformation, re-run the processed step against the same raw bytes, and never touch the API again.

### Interval-matched scheduling

```python
schedule=CronDataIntervalTimetable("*/15 * * * *", "UTC")
```

Every DAG run carries a `data_interval_start` and `data_interval_end`. These are the single source of truth for:

- **Object keys** — `raw/citibike/{{ data_interval_start | ts_nodash }}.json`, so a re-run writes to the same key and overwrites (`replace=True`) instead of accumulating.
- **Taxi filtering** — `_download_taxi_data` only fetches files whose server-reported `mtime` falls inside `[data_interval_start, data_interval_end)`, so a run picks up exactly one 15-minute window of taxi data rather than the whole server directory.
- **The idempotency key** in the serving layer — the `airflow_execution_date` column.

`catchup=False` is deliberate: the raw layer is append-only and the taxi source cannot be backfilled, so historical runs have nothing meaningful to catch up on.

### Two custom operators, one shape

**`PandasOperator`** composes three callables:

```
input_callable → [transform_callable] → output_callable
```

The only contract is *"DataFrame in, DataFrame out."* Both sources use the same operator; the difference between reading JSON from an authenticated API and reading CSV from a file share lives entirely in the callables passed in. Swapping `pd.read_json` for `pd.read_csv` is a keyword change, not a rewrite.

**`MinioPandasToPostgres`** loads a processed Parquet object into Postgres inside a single transaction:

```python
with engine.begin() as conn:
    conn.execute(f"DELETE FROM {table} WHERE airflow_execution_date='{...}';")
    df.to_sql(table, con=conn, index=False, if_exists="append")
```

This is the idempotency primitive for the serving layer. Re-running the 14:15 interval deletes the 14:15 slice and re-inserts it — atomically. Partial failures cannot leave the table in a half-replaced state.

### Reference-data enrichment (the conformance join)

Citi Bike stations are points; taxi zones are polygons. To put both sources in the same unit, `transform_citi_bike_data` downloads the NYC taxi zone shapefile and performs a **spatial join** (`geopandas.sjoin`) of each ride's start and end station points against the zone polygons. The output columns `start_location_id` and `end_location_id` are now in the *same unit* as the taxi data.

This step is what turns two piles of API responses into a dataset that can be compared.

### Serving layer

Two tables in `result_db`:

```sql
taxi_rides       (tripduration, starttime, start_location_id,
                  stoptime, end_location_id, airflow_execution_date)

citi_bike_rides  (tripduration, starttime, start_location_id,
                  stoptime, end_location_id, airflow_execution_date)
```

The DE work ends here. A downstream Flask app joins the two tables on `(start_location_id, end_location_id, weekday, time_group)` and reports the faster mode. Any other consumer — a BI tool, a notebook, a different API — could do the same.

---

## Pipeline at a glance

```
              ┌──────────────────────────────┐
              │  download_citi_bike_data     │  (REST + HTTP Basic)
              │  → raw/citibike/{ts}.json    │
              └──────────────┬───────────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │  process_citi_bike_data      │  (PandasOperator + geopandas.sjoin)
              │  → processed/citibike/*.pq   │
              └──────────────┬───────────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │  citi_bike_to_db             │  (DELETE+INSERT by interval)
              │  → citi_bike_rides           │
              └──────────────────────────────┘

              ┌──────────────────────────────┐
              │  download_taxi_data          │  (file share, mtime-filtered)
              │  → raw/taxi/{name}.csv       │
              └──────────────┬───────────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │  process_taxi_data           │  (column rename + duration)
              │  → processed/taxi/{ts}.pq    │
              └──────────────┬───────────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │  taxi_to_db                  │  (DELETE+INSERT by interval)
              │  → taxi_rides                │
              └──────────────────────────────┘
```

The two chains are intentionally symmetric. Both sources flow through the same `PandasOperator` and the same `MinioPandasToPostgres`; the only differences are the callables and the raw key layout.

---

## Repository layout

```
.
├── dags/nyc_dag.py                    # the pipeline
├── src/nyctransport/
│   ├── operators/
│   │   ├── pandas_operator.py         # input → [transform] → output
│   │   └── s3_to_postgres.py          # idempotent MinIO → Postgres
│   └── hooks/minio_hook.py            # placeholder
├── services/
│   ├── taxi_db/                       # mock source: taxi Postgres (2023 data)
│   ├── citibike_db/                   # mock source: citibike Postgres (2023 data)
│   ├── taxi_fileserver/               # mock file share (nginx + cron)
│   ├── citibike_api/                  # mock REST API (Flask + HTTP Basic)
│   ├── nyc_transportation_api/        # consumer (Flask + Bootstrap)
│   └── result_db/create_tables.sql    # serving schema
├── airflow/Dockerfile                 # custom Airflow image
├── compose.yaml, compose.override.yaml
├── docs/
│   ├── DATA-CONTRACT.md               # source + lake + serving contracts
│   └── RUN-BOOK.md                    # deployment / debugging log
└── tests/dags/test_dag_integrity.py
```

The `services/` directory contains **mocks of upstream systems** — they stand in for the real Citi Bike API and NYC taxi file share, with 2023 data baked in and a `year_offset` that shifts timestamps to look current. They are scaffolding, not part of the pipeline's data engineering surface.

---

## Running it

**Prerequisites:** Docker with ~16 GB RAM and ~20 GB disk.

```bash
docker compose up -d
```

On GitHub Codespaces, use the wrapper that applies the required `iptables-legacy` fix first (see `docs/RUN-BOOK.md` §5):

```bash
./scripts/run-compose.sh
```

Verify the stack:

```bash
./scripts/verify-stack.sh
```

Once healthy:

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| MinIO console | http://localhost:9001 | `AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| Taxi file share | http://localhost:8081 | — |
| Citi Bike API | http://localhost:8082 | `citibike` / `cycling` |
| Comparison app | http://localhost:8083 | — |

Unpause `nyc_dag` in the Airflow UI. It runs every 15 minutes; within one cycle the comparison app begins populating.

---

## Reading order

1. **This README** — the problem and the shape of the solution.
2. **`docs/DATA-CONTRACT.md`** — what each source guarantees, and the lake/serving layouts.
3. **`dags/nyc_dag.py`** — the pipeline itself. The two chains are symmetric.
4. **`src/nyctransport/operators/`** — the two custom operators that carry the DE load.
5. **`docs/RUN-BOOK.md`** — the deployment and debugging log, including the Codespaces networking trap.

---

## Credit

Derived from **Chapter 13** of *Data Pipelines with Apache Airflow, Second Edition* — Harenslak, de Ruiter, Cabral, Geusebroek & van der Ende (Manning, 2026). The use case, the `PandasOperator` pattern, the `MinioPandasToPostgres` idea, and the raw/processed zoning convention come from the book.