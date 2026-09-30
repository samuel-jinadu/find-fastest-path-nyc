# Baseline — 2026-09-30

## Stack
- **16 containers**, CeleryExecutor:
  - **6 Airflow services**: `airflow-apiserver`, `airflow-scheduler`, `airflow-worker`, `airflow-triggerer`, `airflow-dag-processor`, `airflow-init`
  - **4 Postgres**: `postgres` (Airflow metadata), `taxi_db`, `citibike_db`, `result_db`
  - **Redis** (Celery broker/backend)
  - **MinIO** + `minio_init` (bucket bootstrap, `pgsty/mc:latest` — the official `minio/mc` image was retired)
  - **3 app services**: `taxi_fileserver` (nginx), `citibike_api` (Flask), `nyc_transportation_api` (Flask)
- Airflow image: `apache/airflow:3.1.2` → custom build at `airflow/Dockerfile` (adds pandas, pyarrow, geopandas, minio, requests, `apache-airflow-providers-{amazon,postgres}`).
- Custom images pulled from GHCR: `ghcr.io/samuel-jinadu/nyc-{airflow,taxi_db,taxi_fileserver,citibike_db,citibike_api,nyc_transportation_api}:latest`. Built via `.github/workflows/build-images.yaml` (documented in `scripts/RUN-BOOK.md`; **not present in this snapshot** — verify it still exists in the repo).
- Networking: user-defined bridge `10.99.0.0/24` (defined in `compose.override.yaml`).
- Codespaces `iptables-legacy` FORWARD fix lives in `.devcontainer/devcontainer.json` `postStartCommand`.

## Connections (from `compose.override.yaml`)
- `s3` → `http://minio:9000`, bucket `datalake`
- `citibike` → `http://citibike_api:5000` (HTTP basic `citibike:cycling`)
- `taxi` → `http://taxi_fileserver` (no auth)
- `result_db` → `postgresql://nyc:tr4N5p0RT4TI0N@result_db:5432/nyctransportation`

## DAG: `nyc_dag`
- File: `dags/nyc_dag.py`
- Schedule: `CronDataIntervalTimetable("*/15 * * * *", "UTC")`
- `start_date = today − 1 day`, `catchup = False`
- Two parallel chains:
  - `download_citi_bike_data` → `process_citi_bike_data` → `citi_bike_to_db`
  - `download_taxi_data` → `process_taxi_data` → `taxi_to_db`
- Custom operators in `src/nyctransport/operators/`:
  - `PandasOperator` — input → optional transform → output callables
  - `MinioPandasToPostgres` — MinIO object → Postgres, with per-execution-date delete
- Output tables in `result_db`: `taxi_rides`, `citi_bike_rides` (schema in `services/result_db/create_tables.sql`)

## Tests & tooling
- `tests/dags/test_dag_integrity.py` — parametrized over `dags/**/*.py`; parses with `DagBag` and fails on swallowed errors.
- `pytest.ini`: `testpaths = tests`, `python_files = test_*.py`, `python_functions = test_*`.
- `make test` runs pytest inside `airflow-cli` (installs pytest into `/tmp/pylibs` on the fly).
- `scripts/run-compose.sh` — pre-applies Codespaces iptables fix, then `docker compose up`.
- `scripts/verify-stack.sh` — smoke-checks Airflow, MinIO, Citi Bike API, taxi fileserver, result DB.
- `scripts/RUN-BOOK.md` — full deployment/debugging log (see also "Known issues" below).

## Data flow / contracts (see `docs/DATA-CONTRACT.md`)
- **Citi Bike API** — `GET /recent/{minute|hour|day}/{amount}`, basic auth, JSON list of rides. 2023 rows shifted to current year via `year_offset = current_year − DATA_YEAR`. **Backfillable.**
- **Taxi fileserver** — `GET /` returns JSON directory listing; each CSV has `pickup_datetime, dropoff_datetime, pickup_locationid, dropoff_locationid, trip_distance`. New file every 15 min via cron; **files older than 59 min are deleted**. **Not backfillable > 1 h.**
- **MinIO `datalake`** layout:
  - `raw/citibike/{ts}.json`
  - `raw/taxi/{filename}.csv`
  - `processed/citibike/{ts}.parquet`
  - `processed/taxi/{ts}.parquet`
- **Result DB** — `taxi_rides`, `citi_bike_rides`; both carry `airflow_execution_date` to make per-run replacement idempotent.

## Known issues

Resolved since the previous baseline (2026-09-29):
- ~~**A** — taxi XCom returns all server files~~ → `_download_taxi_data` now filters by `mtime ∈ [data_interval_start, data_interval_end)` and returns only the matching keys.
- ~~**B** — DELETE+INSERT not atomic in `MinioPandasToPostgres`~~ → both statements now run inside a single `with engine.begin() as conn:` block.

Still open:
- **C** — taxi-zone shapefile (`taxi_zones.zip`) is fetched per-run to `/tmp/taxi_zones*`; no shared/artifact cache and no checksum/partial-download guard.
- **D** — no `retries`, `retry_delay`, `execution_timeout`, or SLA on any task; no alerting on failure.
- **E** — no data-quality checks (row counts, null keys, non-negative durations, valid zone ids) before write.
- **F** — taxi 1-hour retention ⇒ any scheduler gap > 1 h is permanent loss for taxi; only Citi Bike is backfillable.
- **G** — `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"` ⇒ DAGs go live as soon as they're parsed.
- **H** — `nyc_dag` has no `tags`, `owner`, or `doc_md`.
- **I** — `_download_taxi_data` fetches each file sequentially and buffers full body via `resp.text`; no size cap or streaming.
- **J** — `MinioPandasToPostgres` builds the DELETE via f-string interpolation; brittle to timestamp formatting — use a bound parameter.
- **K** — `transform_citi_bike_data` runs `geopandas.sjoin` twice over the full frame; `how="left"` can duplicate rows when station points fall in overlapping polygons.
- **L** — `make test` installs pytest into `/tmp/pylibs` at run time instead of using the pre-installed dev deps from `airflow/Dockerfile` / `requirements-dev.txt`.
- **M** — dependency-spec drift: `requirements.txt` pins `apache-airflow~=3.0`, `setup.py` pins `apache-airflow~=3.1`, Docker/compose use `3.1.2`.
- **N** — `Makefile` build targets still reference the old names (`airflowbook/chapter12_*`) — stale after the GHCR move.
- **O** — `.github/workflows/build-images.yaml` is referenced by the RUN-BOOK but absent from the snapshot; confirm it's committed.

## Constraints
- Taxi files older than 59 min are deleted by the fileserver ⇒ DAG cadence and scheduler uptime are coupled to source retention.
- Citi Bike API shifts 2023 data into the current year (`year_offset = current_year − 2023`); joins with external time series must account for this.
- Both sources tick every 15 min; DAG schedule `*/15 * * * *` matches cadence.
- Codespaces: every fresh Codespace needs the `iptables-legacy` FORWARD fix (already wired into `devcontainer.json`); the two-firewall trap is documented in `scripts/RUN-BOOK.md`.
- Free-tier Codespaces: a 4-core machine ≈ 30 real hours/month; long-running full-stack is not sustainable (see RUN-BOOK §7).

## Next
- **Milestone 1** (carried over): add `retries` + `execution_timeout` on every task; add one data-quality check per source before DB write; split `nyc_dag` into two DAGs (taxi vs. Citi Bike) for clearer ownership.
- **Milestone 2**: parameterize the DELETE in `MinioPandasToPostgres`; persist/cache the taxi-zone shapefile as a shared artifact; add size cap + streaming for taxi downloads.
- **Milestone 3**: add `tags` / `owner` / `doc_md`; failure alerting; a "minimal stack" compose profile (Airflow + MinIO + `result_db`) for DAG-only development on Codespaces.