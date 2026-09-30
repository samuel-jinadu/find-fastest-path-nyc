# NYC Transportation — Fastest Mode Between Two Zones

> **Solves:** How do you build a *trustworthy, incrementally-updated comparison dataset* from two sources that (a) speak different protocols, (b) measure distance in different units, (c) have asymmetric retention guarantees, and (d) cannot be backfilled the same way? The NYC "taxi vs. Citi Bike" question is the payload; the data engineering problem is heterogeneous ingestion, conformance, and idempotent serving.

---

## Non–data-engineering parts (credit where it's due)

Before describing what this project does *as data engineering*, it's honest to name what it is **not**. A large amount of the code in this repo is demo scaffolding or adjacent disciplines, not DE:

| Part of the repo | What it actually is | Why it isn't DE |
|---|---|---|
| `services/taxi_db/` and `services/citibike_db/` | Docker images with **baked-in 2023 data** and a `year_offset` that pretends 2023 is "now" | These simulate *source systems*. In a real deployment these would be the actual upstream DBs, not images you build. The `awk`/`random.sample` downsampling is a demo trick to keep image size under control. |
| `services/taxi_fileserver/` | An nginx container with `autoindex on` and a cron job that dumps 15-min CSVs from Postgres | This is a **mock of an external file share**. Real file shares are managed by someone else. |
| `services/citibike_api/` | A Flask wrapper around a Postgres table, shifting timestamps by `year_offset` | Same: **mock of an external REST API**. |
| `services/nyc_transportation_api/` + `templates/index.html` | A Flask + Bootstrap **frontend** that renders the comparison table | This is reporting/BI/consumer surface, not data engineering. The hard-coded `taxi_zones` dict is domain lookup data, not a pipeline concern. |
| `.devcontainer/devcontainer.json` + `scripts/run-compose.sh` | The `iptables-legacy` FORWARD fix for Codespaces | **Dev-environment plumbing**, not DE. See `docs/RUN-BOOK.md` §5 — this is the "two-firewall trap". |
| `.github/workflows/build-images.yaml` | Matrix build of 6 images to GHCR | **DevOps / CI**, not DE. |
| MinIO container + `minio_init` bootstrap | Local S3 substitute | **Infra**, though the *use* of it (raw/processed zoning) is DE. |

Everything below — the ingestion logic, the DAG, the custom operators, the data contracts — is where the data engineering actually lives.

---

## Source acknowledgment

This project is a **heavily modified, production-shaped version of Chapter 13** of:

> **Bastiaan Harenslak, Julian de Ruiter, Ismael Cabral, Kris Geusebroek, and Daniel van der Ende — *Data Pipelines with Apache Airflow, Second Edition* (Manning, 2026).**

Chapter 13 introduces the NYC transportation use case, the `PandasOperator` pattern, the `MinioPandasToPostgres` operator, the raw/processed zoning in object storage, and the idempotent DELETE+INSERT idea. It adds Docker Compose orchestration, real Airflow 3.1 deployment, CI, a data contract, a baseline doc, a run-book, and a working frontend. 

---

## The data engineering problem

Two sources, both ticking every 15 minutes, but otherwise as different as they can be:

| | **Citi Bike** | **Yellow Cab** |
|---|---|---|
| Interface | REST API (`GET /recent/minute/15`) | Static file share (`GET /` → JSON listing) |
| Auth | HTTP Basic (`citibike:cycling`) | None |
| Format | JSON list of rides | CSV, one file per 15-min window |
| Location unit | **lat/lon** of start/end stations | **Taxi zone IDs** of pickup/dropoff |
| Retention | Full 2023 dataset in DB; **backfillable** | **Files older than 59 minutes are deleted** |
| Backfill | Possible (wider `amount`) | **Impossible beyond ~1 hour** |
| Cadence | Request-driven | Cron every 15 min |

This produces four hard DE problems at once:

1. **Asymmetric retention.** If the scheduler is down for two hours, taxi data is *permanently lost*; Citi Bike can be recovered. The pipeline cannot treat the two sources symmetrically.
2. **Unit mismatch.** You cannot compare taxi and bike trips unless you map bike station coordinates into taxi zones. That's a **reference-data join**, and it's the difference between a dataset and a pile of rows.
3. **Heterogeneous ingestion.** One source needs an authenticated HTTP client; the other needs a directory listing plus per-file fetches. A single `SimpleHttpOperator` will not do.
4. **Idempotency across reruns.** Both sources are "state at a point in time." Re-running the 14:15 interval must not duplicate rows, must not double-count, and must produce the same result whether it runs once or five times.

The pipeline's job is to turn this into a **layered dataset** (raw → processed → serving) that the Flask app — or any other consumer — can trust.

---

## How it solves it

### 1. Layered data lake (MinIO, S3-compatible)

```
datalake/
├── raw/
│   ├── citibike/{YYYYMMDDTHHMMSS}.json      # exactly what the API returned
│   └── taxi/{MM-DD-YYYY-HH-MM-SS}.csv       # exactly what the file share served
└── processed/
    ├── citibike/{YYYYMMDDTHHMMSS}.parquet   # conformed, zone-mapped
    └── taxi/{YYYYMMDDTHHMMSS}.parquet       # conformed
```

The **raw layer is the source of record**. Because taxi files are deleted after 59 minutes, this is the only copy of that window that will ever exist. Storing raw first is what makes the pipeline re-processable: change the transformation, re-run `process_*` against the same raw bytes — do not re-hit the API.

See `docs/DATA-CONTRACT.md` for the full schema and layout contract.

### 2. Interval-based scheduling, matched to source cadence

```python
schedule=CronDataIntervalTimetable("*/15 * * * *", "UTC")
```

Every run knows its `data_interval_start` and `data_interval_end`. These are used as:

- **Filenames** (`raw/citibike/{{ data_interval_start | ts_nodash }}.json`) so re-runs land on the same key and overwrite (`replace=True`) rather than accumulate.
- **Taxi mtime filter** — `_download_taxi_data` only fetches files whose `mtime` falls inside `[data_interval_start, data_interval_end)`. This fixes the "XCom returns all server files" issue (see `docs/BASELINE.md` §Known issues → resolved A).
- **The idempotency key** in the serving layer (`airflow_execution_date` column).

`catchup=False` is deliberate: because the raw layer is append-only and the taxi source can't be backfilled anyway, historical runs have nothing to catch up on.

### 3. Two custom operators, one shape

**`PandasOperator`** (`src/nyctransport/operators/pandas_operator.py`) composes three callables:

```
input_callable → [transform_callable] → output_callable
```

The only contract is "DataFrame in, DataFrame out." Both sources share this operator; the differences live in the callables passed to `partial`. Swapping `pd.read_json` for `pd.read_parquet` is a keyword change, not a rewrite.

**`MinioPandasToPostgres`** (`src/nyctransport/operators/s3_to_postgres.py`) loads a processed Parquet object into Postgres with a **single transaction** wrapping DELETE-then-INSERT:

```python
with engine.begin() as conn:
    conn.execute(f"DELETE FROM {table} WHERE airflow_execution_date='{...}';")
    df.to_sql(table, con=conn, index=False, if_exists="append")
```

This is the idempotency primitive for the serving layer. Re-running the 14:15 interval deletes the 14:15 slice and re-inserts it — atomically. The `with engine.begin()` block was added to fix the previously non-atomic DELETE+INSERT (see `docs/BASELINE.md` → resolved B).

### 4. Reference-data enrichment (the conformance step)

`transform_citi_bike_data` downloads the NYC taxi zone shapefile once and performs a **spatial join** of Citi Bike start/end station points against taxi zone polygons. The output columns (`start_location_id`, `end_location_id`) are now in the *same units* as the taxi data — which is what makes the comparison possible at all.

This is the step that turns a pile of API responses into a dataset.

### 5. Serving layer and comparison query

Two tables in `result_db`:

```sql
taxi_rides       (tripduration, starttime, start_location_id, stoptime, end_location_id, airflow_execution_date)
citi_bike_rides  (tripduration, starttime, start_location_id, stoptime, end_location_id, airflow_execution_date)
```

The `nyc_transportation_api` (Flask) joins them on `(start_zone, end_zone, weekday, time_group)` and reports which mode wins. The DE work ends at the two tables; everything downstream is consumption.

### 6. Orchestration and deployment

- **Airflow 3.1.2** with `CeleryExecutor` (Redis broker), a standalone DAG processor, a triggerer, and a worker.
- **Connections are declared via env vars** in `compose.override.yaml`, not hard-coded in DAGs.
- **CI:** `.github/workflows/ci.yaml` runs the DAG integrity test inside the built Airflow image on every PR. `.github/workflows/build-images.yaml` builds and pushes all 6 custom images to GHCR.
- **DAG integrity test** (`tests/dags/test_dag_integrity.py`) parametrizes over `dags/**/*.py` and fails on any error the `DagBag` swallows.

---

## Repository layout

```
.
├── dags/nyc_dag.py                    # the pipeline
├── src/nyctransport/
│   ├── operators/
│   │   ├── pandas_operator.py         # input → transform → output
│   │   └── s3_to_postgres.py          # idempotent MinIO → Postgres
│   └── hooks/minio_hook.py            # (placeholder)
├── services/
│   ├── taxi_db/                       # mock source (baked-in data)
│   ├── citibike_db/                   # mock source
│   ├── taxi_fileserver/               # mock file share (nginx)
│   ├── citibike_api/                  # mock REST API (Flask)
│   ├── nyc_transportation_api/        # consumer (Flask + Bootstrap)
│   └── result_db/create_tables.sql    # serving schema
├── docs/
│   ├── DATA-CONTRACT.md               # source + lake + serving contracts
│   ├── BASELINE.md                    # current stack + known issues
│   └── RUN-BOOK.md                    # deployment / debugging log
├── tests/dags/test_dag_integrity.py
├── .github/workflows/                 # CI + image builds
├── compose.yaml, compose.override.yaml
├── airflow/Dockerfile                 # custom Airflow image
└── setup.py, requirements*.txt, Makefile, pytest.ini
```

---

## Running it

**Prerequisites:** Docker, ~16 GB RAM, ~20 GB disk, and — on Codespaces — the `iptables-legacy` fix (already wired into `.devcontainer/devcontainer.json` via `postStartCommand`; see `docs/RUN-BOOK.md` §5).

```bash
# Start the full stack
docker compose up -d

# Or, on Codespaces, use the wrapper that applies the firewall fix first:
./scripts/run-compose.sh

# Verify every service is reachable and the bucket exists
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

Unpause `nyc_dag` in the Airflow UI. It runs every 15 minutes (`*/15 * * * *`). Within one cycle, the comparison app begins populating.

---

## Testing

```bash
make test        # runs pytest inside the airflow-cli container
```

Currently this covers **DAG integrity only**. Unit tests for the transformation functions, integration tests against a containerized Postgres/MinIO, and `dag.test()` runs are on the roadmap (`docs/BASELINE.md` → Next, Milestones 1–3).

---

## Known limitations and roadmap

See **`docs/BASELINE.md`** for the authoritative list. Highlights:

**Open issues (from BASELINE.md):**

- **No retries, timeouts, or SLAs** on any task; no failure alerting.
- **No data-quality checks** (row counts, null keys, non-negative durations, valid zone IDs) before write.
- **Taxi 1-hour retention** ⇒ any scheduler gap > 1 hour is permanent loss. Only Citi Bike is backfillable.
- **Shapefile is re-downloaded per run** to `/tmp` — no shared artifact cache, no checksum guard.
- **`MinioPandasToPostgres` DELETE uses f-string interpolation** — should be a bound parameter.
- **`transform_citi_bike_data` runs two `geopandas.sjoin` passes** and can duplicate rows when a station falls in overlapping polygons.
- **`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"`** — DAGs go live as soon as they're parsed.
- **`nyc_dag` has no `tags`, `owner`, or `doc_md`.**
- **Dependency drift:** `requirements.txt` pins `~=3.0`, `setup.py` pins `~=3.1`, images use `3.1.2`.

**Roadmap (from BASELINE.md → Next):**

1. Add `retries` + `execution_timeout` on every task; add one data-quality check per source before DB write; **split `nyc_dag` into two DAGs (taxi vs. Citi Bike) for clearer ownership.**
2. Parameterize the DELETE in `MinioPandasToPostgres`; persist the taxi-zone shapefile as a shared artifact; add a size cap and streaming for taxi downloads.
3. Add `tags` / `owner` / `doc_md`; failure alerting; a **"minimal stack" compose profile** (Airflow + MinIO + `result_db`) for DAG-only development on resource-constrained environments.

---

## Reading order

If you're new to this repo:

1. **This README** — the problem and the shape of the solution.
2. **`docs/DATA-CONTRACT.md`** — what each source guarantees and what the lake/serving layouts look like.
3. **`dags/nyc_dag.py`** — the pipeline itself. Read top to bottom; the two chains are symmetric.
4. **`src/nyctransport/operators/`** — the two custom operators that carry the DE load.
5. **`docs/BASELINE.md`** — what works, what doesn't, and what's next.
6. **`docs/RUN-BOOK.md`** — the deployment and debugging log, including the Codespaces networking trap. Read this if you're running the stack yourself.

---

## License and attribution

This project is derived from **Chapter 13 of *Data Pipelines with Apache Airflow, Second Edition*** (Manning, 2026). The original chapter is the source of the use case, the `PandasOperator` pattern, the `MinioPandasToPostgres` idea, and the raw/processed zoning convention. All modifications, deployment scaffolding, CI, documentation, and the run-book are additions on top of that base.